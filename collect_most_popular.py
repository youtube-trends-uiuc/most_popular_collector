import requests
import json
import logging
import time
import googleapiclient.discovery
from googleapiclient.errors import HttpError, UnknownApiNameOrVersion
import datetime
from socket import error as socket_error
import errno
import boto3


COUNTRIES = ["AE", "AR", "AT", "AU", "AZ", "BA", "BE", "BG", "BH", "BO", "BR", "BY", "CA", "CH", "CL", "CO", "CR", "CY",
             "CZ", "DE", "DK", "DO", "DZ", "EC", "EE", "EG", "ES", "FI", "FR", "GB", "GE", "GH", "GR", "GT", "HK", "HN",
             "HR", "HU", "ID", "IE", "IL", "IN", "IQ", "IS", "IT", "JM", "JO", "JP", "KE", "KR", "KW", "KZ", "LB", "LI",
             "LK", "LT", "LU", "LV", "LY", "MA", "ME", "MK", "MT", "MX", "MY", "NG", "NI", "NL", "NO", "NP", "NZ", "OM",
             "PA", "PE", "PH", "PK", "PL", "PR", "PT", "PY", "QA", "RO", "RS", "RU", "SA", "SE", "SG", "SI", "SK", "SN",
             "SV", "TH", "TN", "TR", "TW", "TZ", "UA", "UG", "US", "UY", "VN", "YE", "ZA", "ZW"]

WAIT_WHEN_SERVICE_UNAVAILABLE = 30
WAIT_WHEN_CONNECTION_RESET_BY_PEER = 60


def get_youtube_client(developer_key):
    try:
        youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                  version="v3",
                                                  developerKey=developer_key,
                                                  cache_discovery=False)
    except UnknownApiNameOrVersion as e:
        page = requests.get("https://www.googleapis.com/discovery/v1/apis/youtube/v3/rest")
        service = json.loads(page.text)
        youtube = googleapiclient.discovery.build_from_document(service=service,
                                                                developerKey=developer_key)
    return youtube


def collect_most_popular():
    # read credentials
    s3 = boto3.resource('s3')
    content_object = s3.Object('youtube-trends-uiuc-admin', 'credentials.json')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    credentials = json.loads(file_content)
    # TODO: Maybe shuffle credentials.

    with open('./most_popular.json', 'w') as json_writer:
        current_key = 0
        youtube = get_youtube_client(credentials[current_key]['developer_key'])

        for region_code in COUNTRIES:
            rank = 1
            retrieved_at = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S.%f") + "000"
            next_page_token = None

            # Loop through all pages for this region
            while True:
                service_unavailable = 0
                connection_reset_by_peer = 0
                no_response = True
                response = dict()

                while no_response:
                    try:
                        # Build the request with or without pageToken
                        request_params = {
                            'part': 'snippet,statistics',
                            'chart': 'mostPopular',
                            'regionCode': region_code,
                            'maxResults': 50
                        }
                        if next_page_token:
                            request_params['pageToken'] = next_page_token

                        response = youtube.videos().list(**request_params).execute()
                        no_response = False
                    except socket_error as e:
                        if e.errno != errno.ECONNRESET:
                            logging.info("Other socket error!")
                            raise
                        else:
                            connection_reset_by_peer = connection_reset_by_peer + 1
                            logging.info("Connection reset by peer! {}".format(connection_reset_by_peer))
                            if connection_reset_by_peer <= 10:
                                time.sleep(WAIT_WHEN_CONNECTION_RESET_BY_PEER)
                                youtube = get_youtube_client(credentials[current_key]['developer_key'])
                            else:
                                raise
                    except HttpError as e:
                        if "403" in str(e):
                            logging.info("403 - Quota Exceeded. Invalid {} developer key: {}".format(current_key,
                                                                                                     credentials[
                                                                                                         current_key][
                                                                                                         'developer_key']))
                            current_key = current_key + 1
                            if current_key < len(credentials):
                                youtube = get_youtube_client(credentials[current_key]['developer_key'])
                            else:
                                raise
                        elif "503" in str(e):
                            logging.info("503 - Service unavailable")
                            service_unavailable = service_unavailable + 1
                            if service_unavailable <= 10:
                                time.sleep(WAIT_WHEN_SERVICE_UNAVAILABLE)
                            else:
                                raise
                        elif "429" in str(e):
                            logging.info("429 - Too Many Requests")
                            service_unavailable = service_unavailable + 1
                            if service_unavailable <= 10:
                                time.sleep(WAIT_WHEN_SERVICE_UNAVAILABLE)
                            else:
                                raise
                        else:
                            raise

                # Process items in the current page
                for item in response.get('items', []):
                    item['snippet']['publishedAt'] = item['snippet']['publishedAt'].replace('Z', '.000000000').replace(
                        'T', ' ')
                    item['metadata'] = dict()
                    item['metadata']['region_code'] = region_code
                    item['metadata']['retrieved_at'] = retrieved_at
                    item['metadata']['rank'] = rank
                    item['metadata']['strict_periodicity'] = True
                    item['metadata']['batch_type'] = 0
                    item['metadata']['time_elapsed_since_last_retrieval'] = None
                    item['metadata']['likes_recorded'] = True
                    rank = rank + 1
                    json_writer.write("{}\n".format(json.dumps(item)))

                # Check if there's a next page
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    # No more pages for this region, break the while loop
                    break

                # Optional: Add a small delay between page requests to be respectful to the API
                # time.sleep(0.1)


# TODO: A system to tell us when there is an error.
if __name__ == '__main__':
    collect_most_popular()
