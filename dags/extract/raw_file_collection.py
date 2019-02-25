'''
Script requests the current gdelt data and uploads to s3
'''

# imports
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile
import os
import re

# for S3 access
import boto3


class S3_uploader(object):
    def __init__(self):
        # Link is updated every 15 minutes
        self.target_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
        self.target_file = None
        self.target_file_url = None

    def set_target_file(self, target):
        '''
        define the target file as either export, mentions, or gkg

        Params
        -------
        target:str
            either export, metions, or gkg
        '''
        target_file_link = None

        data = urlopen(self.target_url)
        for line in data:
            if target in line.decode('utf-8'):
                target_file_link = line.decode('utf-8')
                break

        # Order of links: file size, hash, link to zip
        target_link = target_file_link.split(" ")[2]
        self.target_file_url = target_link

        target_file = target_link.split("/")[-1]
        target_filename = target_file.replace(".zip\n", "")
        print('Target file - ' + target_file)
        print('Target file URL - ' + target_link)

        self.target_file = target_filename

    def download_target(self):
        print('Downloading GDELT update file ' + self.target_file)
        # change location to /root/airflow/gdelt/raw_temp_files
        urlretrieve(self.target_file_url, TEMP_DIR_PATH + self.target_file + ".zip")

    def unzip_target(self):
        filename = TEMP_DIR_PATH + self.target_file + ".zip"
        print('Unzipping -' + filename)

        with ZipFile(filename, 'r') as zip:
            # extracting all the files
            print('Extracting all the files now...')
            zip.extractall(TEMP_DIR_PATH)
            print('Done!')

    def delete_targets(self):
        print('Removing temporary local files')
        os.remove(TEMP_DIR_PATH + self.target_file)
        os.remove(TEMP_DIR_PATH + self.target_file + '.zip')
        print('Temporary files deleted')

    def upload_to_s3(self):
        print('Uploading - {} to S3'.format(self.target_file))
        self.bucket = None
        filepath = TEMP_DIR_PATH + self.target_file

        if re.search(r'gkg', filepath):
            self.bucket = 'raw-gkg-gdelt'
        elif re.search(r'export', filepath):
            self.bucket = 'raw-events-gdelt'
        elif re.search(r'mentions', filepath):
            self.bucket = 'raw-mentions-gdelt'
        else:
            print('Not a valid file')

        s3.upload_file(TEMP_DIR_PATH + self.target_file, self.bucket, self.target_file)
        print('Upload Complete!')


if __name__ == '__main__':

    TEMP_DIR_PATH = "/root/airflow/gdelt/raw_temp_files/"
    s3 = boto3.client('s3')

    # initialize uploader objects
    gkg_uploader = S3_uploader()
    events_uploader = S3_uploader()
    mentions_uploader = S3_uploader()
    # download and send gkg files to s3
    gkg_uploader.set_target_file('gkg')
    gkg_uploader.download_target()
    gkg_uploader.unzip_target()
    gkg_uploader.upload_to_s3()
    gkg_uploader.delete_targets()
    # download and send events files to s3
    events_uploader.set_target_file('export')
    events_uploader.download_target()
    events_uploader.unzip_target()
    events_uploader.upload_to_s3()
    events_uploader.delete_targets()
    # download and send events files to s3
    mentions_uploader.set_target_file('mentions')
    mentions_uploader.download_target()
    mentions_uploader.unzip_target()
    mentions_uploader.upload_to_s3()
    mentions_uploader.delete_targets()
