# NJTransit Crowding By Route

An lxml-based grabber/parser that extracts a CSV of an entire route crowding data.

# tech

## local testing

Use `flask run` or VSCode Flask debug profile.

Go to `http://127.0.0.1:5000/scrape?route=119`

## deployment

Deployed as a zappa function with

```
zappa dev deploy
```

Get url
```
zappa status
```


Update with

```
zappa dev update
```

Invoke 

```
TK
```
