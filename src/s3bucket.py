import logging
#import botocore
#import os
import re



class s3bucket(object):

    def __init__(self,bucket_name,client,resource):
        self.bucket_name = bucket_name
        self.client = client
        self.s3 = resource


    def create_bucket(self,location):
        """
        :param location: Aws region
        :return: returns bucket creation if not raises error
        """
        try:

            response = self.client.create_bucket(
                ACL='private',
                Bucket= self.bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': location
                }
            )

            logging.info("Bucket Creation Response {0}".format(response))

        except Exception as er:
            logging.error("Log error {0}".format(er))




    def bucket_exists(self):

        """
        :return: If Bucket Name exits then Bucket exists else
        """
        #bucket = s3.Bucket(self.bucket_name)
        exits = True

        try:
            self.s3.meta.client.head_bucket(Bucket=self.bucket_name)
            logging.info('found bucket {}'.format(self.bucket_name))
            return exits
        except self.s3.meta.client.exceptions.BucketAlreadyExists as err:
            logging.error("Bucket exists error {}".format(err))
            return False



    def get_files(self,prefix,exp):
        """

        :param prefix: path
        :param exp: search pattern in file
        :return: returns keys of the objects
        """
        f = []
        files = self.s3.list_objects_v2(Bucket=self.bucket_name,Prefix =prefix)
        for fx in files['Contents']:
            if re.search(exp,fx) :
                f.append(fx['key'])
        return f


    def uploadfile(self,path ,key):

        try:
            self.client.upload_file(
                Filename=path ,
                Bucket=self.bucket_name,
                Key=key)
            logging.info('insert file with key  {0}'.format(key))
        except Exception as e:
            logging.error('upload file error  {}'.format(e))


    def uploadobject(self,data,key):

        try:
            self.client.put_object(
                Body=data,
                Bucket=self.bucket_name,
                Key=key)
            logging.info('insert file with key  {0}'.format(key))
        except Exception as e:
            logging.error('upload file error  {}'.format(e))



    def deletefile(self,key):

        try:
            response = self.client.delete_object(
                Bucket=self.bucket_name,
                Key=key)
            logging.info('deleted file {0}'.format(response))
        except Exception as e:
            logging.info('deleted file error {0}'.format(e))







