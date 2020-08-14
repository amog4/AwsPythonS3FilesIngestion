import logging
from boto3 import client,resource
#import botocore
import os
import re

bucket_name = os.environ.get("BUCKET_NAME")
location = os.environ.get("LOC")
client = client('s3')
s3 = resource('s3')

class s3bucket(object):

    def __init__(self,bucket_name):
        self.bucket_name = bucket_name


    def create_bucket(self,location):
        """
        :param location: Aws region
        :return: returns bucket creation if not raises error
        """
        try:

            response = client.create_bucket(
                ACL='private',
                Bucket= self.bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': location
                }
            )

            logging.info("Bucket Creation Response {0}".format(response))

        except Exception as er:
            logging.error("Log error {0}".format(er))
            print('error')



    def bucket_exists(self):

        """
        :return: If Bucket Name exits then Bucket exists else
        """
        bucket = s3.Bucket(self.bucket_name)
        exits = True

        try:
            s3.meta.client.head_bucket(Bucket=bucket.name)
            logging.info('found bucket {}'.format(bucket.name))
        except s3.meta.client.exceptions.BucketAlreadyExists as err:
            logging.error("Bucket exists error {}".format(err))



    def get_files(self,prefix,exp):
        """

        :param prefix: path
        :param exp: search pattern in file
        :return: returns keys of the objects
        """
        f = []
        files = s3.list_objects_v2(Bucket=self.bucket_name,Prefix =prefix)
        for fx in files['Contents']:
            if re.search(exp,fx) :
                f.append(fx['key'])
        return f


    def uploadfile(self,data,key):

        try:
            client.put_object(
                Body=data,
                Bucket=self.bucket_name,
                Key=key)
            logging.info('insert file with key  {0}'.format(key))
        except Exception as e:
            logging.error('upload file error  {}'.format(e))


    def deletefile(self,key):

        try:
            response = client.delete_object(
                Bucket=self.bucket_name,
                Key=key)
            logging.info('deleted file {0}'.format(response))
        except Exception as e:
            logging.info('deleted file error {0}'.format(e))




if __name__ == '__main__':
    s = s3bucket(bucket_name=bucket_name)
    #s.create_bucket(location=location )
    s.bucket_exists()



