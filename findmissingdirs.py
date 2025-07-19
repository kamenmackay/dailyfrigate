from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

# List of directories to check
directories_to_check = [
    "/export/dsk1/app/loaders/log/alerter", "/export/dsk1/app/loaders/log/boc", 
    "/export/dsk1/app/loaders/log/cboe", "/export/dsk1/app/loaders/log/cdntbill", 
    "/export/dsk1/app/loaders/log/cfra", "/export/dsk1/app/loaders/log/cins", 
    "/export/dsk1/app/loaders/log/cnsx", "/export/dsk1/app/loaders/log/coppclark",
    "/export/dsk1/app/loaders/log/dow", "/export/dsk1/app/loaders/log/dpa", 
    "/export/dsk1/app/loaders/log/edi", "/export/dsk1/app/loaders/log/ediV2", 
    "/export/dsk1/app/loaders/log/esg", "/export/dsk1/app/loaders/log/etf", 
    "/export/dsk1/app/loaders/log/figi", "/export/dsk1/app/loaders/log/ftse", 
    "/export/dsk1/app/loaders/log/fundata", "/export/dsk1/app/loaders/log/iiroc",
    "/export/dsk1/app/loaders/log/libor", "/export/dsk1/app/loaders/log/lipper", 
    "/export/dsk1/app/loaders/log/listener", "/export/dsk1/app/loaders/log/loaderlogger", 
    "/export/dsk1/app/loaders/log/logos", "/export/dsk1/app/loaders/log/mergent", 
    "/export/dsk1/app/loaders/log/mmid", "/export/dsk1/app/loaders/log/morningstar", 
    "/export/dsk1/app/loaders/log/msci", "/export/dsk1/app/loaders/log/nasdaq", 
    "/export/dsk1/app/loaders/log/ngx", "/export/dsk1/app/loaders/log/notification", 
    "/export/dsk1/app/loaders/log/nyse", "/export/dsk1/app/loaders/log/quartz",
    "/export/dsk1/app/loaders/log/rates", "/export/dsk1/app/loaders/log/screports", 
    "/export/dsk1/app/loaders/log/sec", "/export/dsk1/app/loaders/log/sedi", 
    "/export/dsk1/app/loaders/log/shareos", "/export/dsk1/app/loaders/log/snp", 
    "/export/dsk1/app/loaders/log/sofr", "/export/dsk1/app/loaders/log/sp", 
    "/export/dsk1/app/loaders/log/structuredLogs", "/export/dsk1/app/loaders/log/tenforeeod", 
    "/export/dsk1/app/loaders/log/tsx", "/export/dsk1/app/loaders/log/wallstreethorizon", 
    "/export/dsk1/app/loaders/log/zacks"
]


# Elasticsearch connection
es = Elasticsearch(
    "https://applogs:applogs@ailsa.van.quotemedia.com:9200",
    verify_certs=False,
    ssl_show_warn=False,
    timeout=900,
)

# Time range: last 7 days
now = datetime.now()
start_date = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")
end_date = now.strftime("%Y-%m-%dT%H:%M:%S")

# Elasticsearch query
query = {
    "query": {
        "bool": {
            "must": [
                {"range": {"@timestamp": {"gte": start_date, "lte": end_date}}},
                {"terms": {"log.file.path": directories_to_check}}
            ]
        }
    }
}

# Search Elasticsearch
response = es.search(index="loaders-prod", body=query, size=10000)

# Extract found directories
found_directories = {hit["_source"]["log"]["file"]["path"] for hit in response["hits"]["hits"]}

# List of directories not found in Elasticsearch
missing_directories = set(directories_to_check) - found_directories

# Output the missing directories
print("Directories not found in Elasticsearch in the last 7 days:")
for directory in sorted(missing_directories):
    print(directory)
