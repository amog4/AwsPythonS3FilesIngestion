import os
from config.constantproperties import S3_BASE_KEY
import yaml

with open('{}/../config/dev.yml'.format(os.path.dirname(__file__)),'rb') as f:
    print(f)
    doc = yaml.load(f,Loader=yaml.FullLoader)

for key,v in doc.items():
    fileupload_ = key
    val = v

s3path = S3_BASE_KEY.format(name= val['name'],schema = val['schema'])

