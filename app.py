import aiohttp
import asyncio
import time

from NJTransitAPI import *
from Parser import parse_results
from Dumper import dump_df

import pandas as pd

def scrape_route(route):

    # get route geometry
    data, fetch_timestamp = get_xml_data('nj', 'route_points', route=route)
    route_points = parse_xml_getRoutePoints(data)
    
    # make container to hold results
    dfs = []

    # loop over each direction in first defined service
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
                results.append(await response.read())

            await session.close()
            
            #parse and dump results
            df_direction = parse_results(route, direction, results)
        
            # dfs.append(df_direction)
            return df_direction
                
        # run and report
        start = time.time()
        dfs.append(
            asyncio.run(run_tasks())
        )
        end = time.time()    
        print(f"Scraped {route} to {direction} in {(end - start):.1f} seconds.")

    data = pd.concat([df for df in dfs])
    dump_df(data)
    print(data)        


#######################################################
################## INVOKE THE SCRIPT ##################
#######################################################

##TODO: pass route in from zappa kwargs or URL
# # my_kwargs = event.get("kwargs")
# route = my_kwargs['route']
route = "119"
scrape_route(route)

