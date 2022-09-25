import os
import boto3
import pandas as pd
import botocore

def dump_df(route, df_2_add):
    
    # https://janakiev.com/blog/pandas-pyarrow-parquet-s3/

    # settings
    bucket_name = "njtransit-crowding-data"
    object_key=f"njt-crowding-route-{route}.parquet"
    local_file_name = f'/tmp/{object_key}'

    # s3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    #https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
    try:
        s3.Object(bucket_name, object_key).load()
    except botocore.exceptions.ClientError as e:
        # create local file
        if e.response['Error']['Code'] == "404":
            df_2_add.to_parquet(local_file_name)
        else:
            # Something else has gone wrong.
            raise
    else:
        # The object does exist.
        s3.Bucket(bucket_name).download_file(object_key,local_file_name)
        
        # read it into a df
        old_df = pd.read_parquet(local_file_name)
        
        # combine them
        new_df = old_df.append(df_2_add)
        
        # write out the combined df locally
        new_df.to_parquet(local_file_name)

    # upload file from tmp to s3
    bucket.upload_file(local_file_name, object_key)

    # delete the local tmp beacause AWS reuses lambda containers
    os.remove(local_file_name)
    
    return f"s3://{bucket_name}/{object_key}"
