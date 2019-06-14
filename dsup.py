"""
Author(s): Malav Shah

Upload Dataset to AWS
"""

import boto3
import os

def validate_bucket_name(bucket_name, s3):
    s3t = boto3.resource('s3')
    if s3t.Bucket(bucket_name).creation_date is None:
        print("....Bucket does NOT exist")
        #if raw_input("Would you like to create a bucket named %s [y/n]" % bucket_name) == 'y':
        #    create_bucket(bucket_name, s3)
        #else:
        #    print("Enter valid bucket name: ")
        #    return False
        print("Bucket does not exits, enter valid bucket name")
        return False
    else:
        print("...bucket exists....")
        return True

def main():
    localDataPath = raw_input("Data Path: ") #r"/Users/Malav_Mac/Documents/Malav_Folder/TSL_Research/data_dump"
    bucket_name = raw_input("Bucket name: ") #'trialupload1'

    # Create an S3 client
    s3 = boto3.client('s3')

    while not validate_bucket_name(bucket_name, s3):
        bucket_name = raw_input("Bucket name: ")


    #get all files within directory
    files =next(os.walk(localDataPath))[2]
    print("...begining upload to %s" % bucket_name) 

    num_files = 0
    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.
    for file in files:
        if not file.startswith('.'):
            local_file = os.path.join(localDataPath, file)
            s3.upload_file(local_file, bucket_name, file)
            num_files = num_files + 1

    print("...finished uploading...%d files uploaded" % num_files)

if __name__ == "__main__":
    main()


