import csv
import boto3
import botocore
import pandas as pd

def dump_df(df):
    print(df)
    return

    '''
    ## following https://medium.com/@haldis444/use-lambda-to-append-daily-data-to-csv-file-in-s3-2c2813bc33d0

    # download s3 csv file to lambda tmp folder
    bucket_name = "njtransit-crowding-data"
    file_name = f"njt_crowding_by_route_{route}.csv"
    local_file_name = f'/tmp/{file_name}'
    object_key=f"njt-crowding-route-{route}.csv"

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    #https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
    try:
        s3.Object(bucket_name, object_key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            with open(local_file_name, 'w', newline='') as outfile:
                reader = []
                for row in filtered_rows:
                    reader.insert(0,row)
                writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
                for line in reader:
                    writer.writerow(line)
        else:
            # Something else has gone wrong.
            raise
    else:
        # The object does exist.
        s3.Bucket(bucket_name).download_file(object_key,local_file_name)

        # list you want to append
        with open(local_file_name,'r') as infile:
            reader = list(csv.reader(infile))
            reader = reader[::-1] # the date is ascending order in file
            for row in filtered_rows:
                reader.insert(0,row)

        with open(local_file_name, 'w', newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
            for line in reader:
                writer.writerow(line)

    # upload file from tmp to s3
    bucket.upload_file(local_file_name, object_key)

    # delete the local tmp beacause AWS reuses lambda containers
    import os
    os.remove(local_file_name)

    '''