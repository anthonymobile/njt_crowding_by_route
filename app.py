#!/usr/bin/env python
# coding: utf-8

from lxml import etree as ET
from lxml import html
import requests
import csv
import boto3
import botocore

import datetime
import dateutil

from NJTransitAPI import *

##TODO: enacsulate below in a function with return
# def scrape_stop():
    # ...
    # return { 'message':  f"Wrote {len(filtered_rows)} new buses to s3://{bucket}/{file_name}"}


##TODO: pass route in from zappa kwargs or URL
# my_kwargs = event.get("kwargs")
# route = my_kwargs['route']
route = "119"

data, fetch_timestamp = get_xml_data('nj', 'route_points', route=route)

route_services = parse_xml_getRoutePoints(data)

# iterate over the services and routes

for route in route_services:
    for service in route.paths:
        stoplist=[p.identity for p in service.points if isinstance(p, Route.Stop)]
        print (stoplist)
        

'''
        url=f"https://www.njtransit.com/my-bus-to?stopID={stop_id}&form=stopID"

        # Request the page and scrape the data
        try:
            page = requests.get(url)
        except:
            print("error getting web page")
            
        tree = html.fromstring(page.content)

        # parse element using XPath and process
        raw_rows = tree.xpath("//div[@class='media-body']")
        parsed_rows=[str(row.xpath("string()")) for row in raw_rows]
        split_rows = [row.split('\n') for row in parsed_rows]
        stripped_rows = []
        for row in split_rows:
            stripped_row = []
            for word in row:
                stripped_row.append(word.strip())
            stripped_rows.append([i for i in stripped_row if i])
        filtered_rows = [b for b in stripped_rows if len(b)==5]
        
        # clean up fields
        for row in filtered_rows:
            row[1] = row[1].split("#")[1]
            row[2] = row[2].split("Arriving in ")[1].split(" minutes")[0]
            row.insert(0, stop_id)
            row.insert(
                0, str(
                    datetime.datetime.now(
                        tz=dateutil.tz.gettz('America/New_York')
                        ).isoformat()
                    )
                )

        # # create the df and prepare to write
        # df = pd.DataFrame(filtered_rows, columns =['stop_id', 'timestamp', 'Destination', 'BusID', 'ETA_mins', 'ETA_time', 'Occupancy' ])        

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