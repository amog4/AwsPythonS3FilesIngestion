import os
import sys
sys.path.insert(1, '{}/../'.format(os.path.dirname(__file__)))
from config.constantproperties import S3_BASE_KEY
from  s3bucket import s3bucket
from boto3 import client,resource
import yaml
import datetime
bucket_name = os.environ.get("BUCKET_NAME")
location = os.environ.get("LOC")


client = client('s3')
resource = resource('s3')

data_path = '{}/../data/'.format(os.path.dirname(__file__))
print(data_path)

# config from yaml file
with open('{}/../config/dev.yml'.format(os.path.dirname(__file__)),'rb') as f:
    doc = yaml.load(f,Loader=yaml.FullLoader)

for key,v in doc.items():
    fileupload_ = key
    val = v

s3path = S3_BASE_KEY.format(name= val['name'],schema = val['schema'])

s3_ = s3bucket(bucket_name=bucket_name,client = client,resource=resource)
date_ = str(datetime.datetime.now()).split(' ')[0]

# upload files
for path,subdirs, files in os.walk(data_path):
    print(path)
    for file in files:
        with open(os.path.join(path,file),'rb') as d:
            s3_.uploadfile(path = d.name,key=s3path+file)





