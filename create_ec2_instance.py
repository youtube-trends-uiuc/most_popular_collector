import boto3
from urllib.parse import urlparse
import json
import uuid


INIT_SCRIPT = """#!/bin/bash
cd /home/ubuntu
su ubuntu -c 'mkdir .aws'
su ubuntu -c 'printf "[default]\\nregion={region}" > /home/ubuntu/.aws/config'
su ubuntu -c 'wget {init_script} -O {new_name}'
su ubuntu -c 'chmod +x {new_name}'
su ubuntu -c "echo '/home/ubuntu/{new_name} {s3_config}' > call.txt"
su ubuntu -c 'sudo apt-get update'
su ubuntu -c 'sudo apt-get install -y screen'
su ubuntu -c "screen -dmS internet_scholar sh -c '/home/ubuntu/{new_name} {s3_config} 2>&1 | tee output.txt; exec bash'"
"""

aws_config = {
    "s3-admin": "internet-scholar-admin",
    "s3-data": "internet-scholar",
    "athena-admin": "internet_scholar_admin",
    "athena-data": "internet_scholar",
    "default_region": "us-west-2",
    "key_name": "webscholar",
    "security_group": "launch-wizard-1",
    "iam": "ec2_internetscholar",
    "instance_type": "t3a.nano",
    "volume_size": 15,
    "name": "youtube-video-snippet",
    "init_script": "https://raw.githubusercontent.com/internet-scholar/youtube_video_snippet/master/init_script.sh",
    "ami": "ami-020d545e05dd191a9"
}


def lambda_handler(event, context):
    url_object = urlparse(event['s3_config'], allow_fragments=False)
    s3 = boto3.resource('s3')
    content_object = s3.Object(url_object.netloc, url_object.path.lstrip('/'))
    file_content = content_object.get()['Body'].read().decode('utf-8')
    config = json.loads(file_content)

    new_name = f"{uuid.uuid4()}.sh"
    init_script = INIT_SCRIPT.format(init_script=config['aws']['init_script'],
                                     region=config['aws']['default_region'],
                                     s3_config=event['s3_config'],
                                     new_name=new_name)

    ec2 = boto3.resource('ec2')
    ec2.create_instances(
        ImageId=config['aws']['ami'],
        InstanceType=config['aws']['instance_type'],
        MinCount=1,
        MaxCount=1,
        KeyName=config['aws']['key_name'],
        InstanceInitiatedShutdownBehavior='terminate',
        UserData=init_script,
        SecurityGroupIds=[config['aws']['security_group']],
        BlockDeviceMappings=[
            {
                'DeviceName': '/dev/sda1',
                'Ebs': {
                    'DeleteOnTermination': True,
                    'VolumeSize': config['aws']['volume_size']
                }
            },
        ],
        TagSpecifications=[{'ResourceType': 'instance',
                            'Tags': [{"Key": "Name", "Value": config['aws']['name']}]}],
        IamInstanceProfile={'Name': config['aws']['iam']}
    )
