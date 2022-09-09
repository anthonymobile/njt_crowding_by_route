#!/usr/bin/env python
# coding: utf-8

from lxml import etree as ET
from lxml import html
import requests
import csv
import boto3
import botocore

import datetime as dt
import dateutil

import aiohttp
import asyncio
import time

import pandas as pd
from NJTransitAPI import *

##TODO: encapsulate below in a function with return
# def scrape_stop():
    # ...
    # return { 'message':  f"Wrote {len(filtered_rows)} new buses to s3://{bucket}/{file_name}"}


def parse_results(route, direction, results):
    for page in results:
        tree = html.fromstring(page)

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
        
        print([f for f in filtered_rows])
        
        # # clean up fields
        # for row in filtered_rows:
        #     row[1] = row[1].split("#")[1]
        #     row[2] = row[2].split("Arriving in ")[1].split(" minutes")[0]
        #     row.insert(0, "direction")
        #     row.insert(0, "route")
        #     row.insert(
        #         0, str(
        #             dt.datetime.now(
        #                 tz=dateutil.tz.gettz('America/New_York')
        #                 ).isoformat()
        #             )
        #         )
            
        return
  



##TODO: pass route in from zappa kwargs or URL
# my_kwargs = event.get("kwargs")
# route = my_kwargs['route']
route = "119"

data, fetch_timestamp = get_xml_data('nj', 'route_points', route=route)

route_points = parse_xml_getRoutePoints(data)


# focus only on the first defined set of routes in points
# iterate over the 2 directions

output = []

for path in route_points[0].paths:
    
    direction = path.d
    
    url = "https://www.njtransit.com/my-bus-to?stopID={}&form=stopID"

    # async based on
    # https://betterprogramming.pub/asynchronous-programming-in-python-for-making-more-api-calls-faster-419a1d2ee058
    # https://github.com/PatrickAlphaC/async-python/blob/main/av_async_run.py        
    
    results = []
    
    def get_tasks(stoplist, session):
        tasks = []
        for stop_id in stoplist:
            tasks.append(session.get(url.format(stop_id), ssl=False))
        return tasks

    async def run_tasks():
        session = aiohttp.ClientSession()
        stoplist=[p.identity for p in path.points if isinstance(p, Route.Stop)]
        tasks = get_tasks(stoplist, session)
        responses = await asyncio.gather(*tasks)
        # collect the results
        for response in responses:

            #FIXME: the conversion to text appears to be screwing with the parsing of the html?
            #results.append(await response.text())
            results.append(await response.read())
        await session.close()
        
        # do something with the results
        parse_results(route, direction, results)
            


    start = time.time()
    asyncio.run(run_tasks())
    end = time.time()
    total_time = end - start
    print(f"Made {len(results)} API calls in {total_time:.1f} seconds.")



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