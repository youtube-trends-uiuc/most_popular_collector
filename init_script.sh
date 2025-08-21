#!/bin/bash
sudo timedatectl set-timezone UTC
sudo apt-get install -y python3-pip python3-venv default-jre
cd /home/ubuntu
wget https://repo1.maven.org/maven2/org/apache/orc/orc-tools/2.2.0/orc-tools-2.2.0-uber.jar
wget https://raw.githubusercontent.com/youtube-trends-uiuc/recommendations_collector/refs/heads/main/collect_recommendations.py
wget https://raw.githubusercontent.com/youtube-trends-uiuc/recommendations_collector/refs/heads/main/requirements.txt
wget https://raw.githubusercontent.com/youtube-trends-uiuc/recommendations_collector/refs/heads/main/upload_recommendations.py
python3 -m venv ./venv
source ./venv/bin/activate
pip3 install --trusted-host pypi.python.org -r ./requirements.txt
python3 ./collect_recommendations.py && \
java -jar ./orc-tools-2.2.0-uber.jar convert recommendations.json -s 'struct<kind:string,etag:string,id:string,snippet:struct<publishedAt:timestamp,title:string,description:string,channelId:string,channelTitle:string,categoryId:string,tags:array<string>,liveBroadcastContent:string,defaultLanguage:string,defaultAudioLanguage:string,localized:struct<title:string,description:string>,thumbnails:struct<default:struct<url:string,width:int,height:int>,medium:struct<url:string,width:int,height:int>,high:struct<url:string,width:int,height:int>,standard:struct<url:string,width:int,height:int>,maxres:struct<url:string,width:int,height:int>>>,statistics:struct<viewCount:bigint,likeCount:bigint,dislikeCount:bigint,favoriteCount:bigint,commentCount:bigint>,metadata:struct<region_code:string,retrieved_at:timestamp,rank:int>>' -o json.orc -t "yyyy-MM-dd HH:mm:ss.n" 2>&1 | tee ./orc_output.log && \
bzip2 -z --best ./recommendations.json 2>&1 | tee ./bzip2_output.log && \
python3 ./upload_recommendations.py && \
sudo shutdown -h now