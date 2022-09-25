from app import scrape_route

event = {
    "kwargs":
        {
            "route": "119"
            }
        }

context = {}

scrape_route(event, context)
