"""
Get a list of sequence files given a cloud path
Supporting AWS S3, GCS, DBFS and regular Unix paths.
Allows fa, fq, seq, or gz extensions


By zhong wang @lbl.gov

"""
from axolotl.utils.utils import get_dbutils
import os

def allowed_extensions(filename):
    """
    allowed file extensions
    """
    allowed = ['fa', 'fq', 'seq']
    if filename.split('.')[-1] == 'gz':
        filename = '.'.join(filename.split('.')[0:-1])
    if filename.split('.')[-1] in allowed:
        return True
    else:
        return False

def get_cloud_filelist(filepath, max_files=0):
    counter = 0
    files = []
    if filepath[0:5] == 's3://': #AWS S3
        import boto3
        bucket = filepath.replace('s3://', '').split('/')[0]
        key = '/'.join(filepath.replace('s3://', '').split('/')[1:])
        my_bucket = boto3.resource('s3').Bucket(bucket)
        counter = 0
        files = []
        for f in my_bucket.objects.filter(Prefix=key):
            if allowed_extensions(f.key):
                files.append('s3a://' + bucket + '/' + f.key)
                counter +=1
                if (max_files>0) and (counter >= max_files):
                    break
    elif filepath[0:5] == 'gs://': # GCS
        from google.cloud import storage
        bucket = filepath.replace('gs://', '').split('/')[0]
        key = '/'.join(filepath.replace('gs://', '').split('/')[1:])
        client = storage.Client()
        for f in client.list_blobs(bucket, prefix=key):
            if allowed_extensions(f.name):
                files.append('gs://' + bucket + '/' + f.name)
                counter +=1
                if (max_files>0) and (counter >= max_files):
                    break                  
    elif filepath[0:6] == 'dbfs:/': #DBFS
        dbutils = get_dbutils()
        for f in dbutils.fs.ls(filepath):
            if allowed_extensions(f.name):
                files.append(f.path)
                counter +=1
                if (max_files>0) and (counter >= max_files):
                    break            
    else: # assuming Unix
        for f in os.listdir(filepath):
            if allowed_extensions(f):
                files.append('/'.join([filepath, f]))
                counter +=1
                if (max_files>0) and (counter >= max_files):
                    break  

    return files