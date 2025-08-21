import boto3
import datetime

def upload_most_popular():
    s3 = boto3.resource('s3')
    creation_date = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    # TODO: Maybe make sure that period is in [0, 6, 12, 18]
    period = datetime.datetime.now(datetime.UTC).strftime("%H")
    s3.Bucket('youtube-trends-uiuc-json').upload_file("./most_popular.json.bz2",
                                                      f"creation_date={creation_date}/period={period}/most_popular.json.bz2")
    s3.Bucket('youtube-trends-uiuc').upload_file("./most_popular.orc",
                                                 f"creation_date={creation_date}/period={period}/most_popular.orc")


if __name__ == '__main__':
    upload_most_popular()
