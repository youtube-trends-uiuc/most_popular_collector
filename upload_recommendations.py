import boto3
import datetime

def upload_recommendations():
    s3 = boto3.resource('s3')
    creation_date = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    # TODO: Maybe make sure that period is in [0, 6, 12, 18]
    period = datetime.datetime.now(datetime.UTC).strftime("%H")
    s3.Bucket('youtube-trends-uiuc-json').upload_file("./recommendations.json.bz2",
                                                      f"creation_date={creation_date}/period={period}/recommendations.json.bz2")
    s3.Bucket('youtube-trends-uiuc').upload_file("./json.orc",
                                                 f"creation_date={creation_date}/period={period}/json.orc")


if __name__ == '__main__':
    upload_recommendations()
