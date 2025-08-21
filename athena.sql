CREATE EXTERNAL TABLE IF NOT EXISTS youtube_trends (
  kind string,
  etag string,
  id string,
  snippet struct<
    publishedAt:timestamp,
    channelId:string,
    title:string,
    description:string,
    thumbnails:struct<
      default:struct<
        url:string,
        width:int,
        height:int
      >,
      medium:struct<
        url:string,
        width:int,
        height:int
      >,
      high:struct<
        url:string,
        width:int,
        height:int
      >,
      standard:struct<
        url:string,
        width:int,
        height:int
      >,
      maxres:struct<
        url:string,
        width:int,
        height:int
      >
    >,
    channelTitle:string,
    tags:array<string>,
    categoryId:string,
    liveBroadcastContent:string,
    defaultLanguage:string,
    localized:struct<
      title:string,
      description:string
    >,
    defaultAudioLanguage:string
  >,
  statistics struct<
    viewCount:bigint,
    likeCount:bigint,
    dislikeCount:bigint,
    favoriteCount:bigint,
    commentCount:bigint
  >,
  metadata struct<
    region_code:string,
    retrieved_at:timestamp,
    rank:int
  >
)
PARTITIONED BY (creation_date String, period String)
STORED AS ORC
LOCATION 's3://youtube-trends-uiuc/'
tblproperties ("orc.compress"="ZLIB");

msck repair table youtube_trends;