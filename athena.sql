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
    rank:int,
    strict_periodicity:boolean,
    batch_type:tinyint,
    time_elapsed_since_last_retrieval:bigint,
    likes_recorded:boolean
  >
)
PARTITIONED BY (creation_date String, period String)
STORED AS ORC
LOCATION 's3://youtube-trends-uiuc/'
tblproperties (
  'orc.compress'='ZLIB',
  'projection.enabled' = 'true',
  'projection.creation_date.type' = 'date',
  'projection.creation_date.range' = '2020-01-01,NOW',
  'projection.creation_date.format' = 'yyyy-MM-dd',
  'projection.creation_date.interval' = '1',
  'projection.creation_date.interval.unit' = 'DAYS',
  'projection.period.type' = 'enum',
  'projection.period.values' = '00,06,12,18',
  'storage.location.template' = 's3://youtube-trends-uiuc/creation_date=${creation_date}/period=${period}/'
);
