import datetime as dt
import dateutil
import pandas as pd
from lxml import etree as ET
from lxml import html

def parse_results(route, direction, results):
    
    cols = ['timestamp',
            'route',
            'stop_id',
            'destination',
            'headsign',
            'bus_id',
            'eta_min',
            'eta_time',
            'crowding'
            ]
    
    df = pd.DataFrame(columns=cols)
    
    for page in results:
        
        tree = html.fromstring(page)
        
        # parse stop_id
        try:
            stop_id = tree.xpath("//p[@class='ff-secondary--bold text--shuttle-gray']")[0].text.strip().split(' ')[-1]
        #skip this result if we can't extract the stop_id
        except:
            continue

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

        # clean up fields and add metadata
        for row in filtered_rows:
            row[1] = row[1].split("#")[1]
            if row[2] != "DELAYED":
                row[2] = row[2].split("Arriving in ")[1].split(" minutes")[0]
            row.insert(0, direction)
            row.insert(0, stop_id)
            row.insert(0, route)
            row.insert(
                0, str(
                    dt.datetime.now(
                        tz=dateutil.tz.gettz('America/New_York')
                        ).isoformat()
                    )
                )

        page_df = pd.DataFrame(filtered_rows, columns=cols)
        df = pd.concat([df, page_df], axis=0)
        
    #### CLEAN DATA BEFORE RETURN
    
    # clean up eta_min
    # n.b. the endpoint https://www.njtransit.com/my-bus-to?stopID=30189&form=stopID
    # never returns a 0 or < 1 min ETA, 1 is the lowest and we will keep only those
    df = df[df['eta_min'].isin(['1'])]
    
    #drop no crowding data
    df = df[~df['crowding'].isin(['NO DATA'])]
    
    # #clean up timestamps
    # df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # # drop all nans
    # df = df[df['timestamp'].notna()]
    
    # parse date and set timezone
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert('America/New_York')
    
    return df