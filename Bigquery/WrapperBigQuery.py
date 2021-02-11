from google.cloud import bigquery
from google.oauth2 import service_account

import pandas as pd
import pandas_gbq

credentials = service_account.Credentials.from_service_account_file(
    './Bigquery/heartbeat-001-f88870825bf4.json',
)
def implicit():
    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    # , SUM(number) as total_people
    # WHERE state = 'TX'
    # GROUP BY name, state
    # ORDER BY total_people DESC
    # LIMIT 20
    client = bigquery.Client()
    query = """
        SELECT * 
        FROM `heartbeat-001.crytpoQuotes.deribit`   
    """
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    for row in query_job:
        # Row values can be accessed by field name or index.
        print("name={}".format(row))


def writeQuotes(jsonToWrite, plateforme):
    df = pd.DataFrame(jsonToWrite)
    job = pandas_gbq.to_gbq(df, "crytpoQuotes."+plateforme, project_id="heartbeat-001", if_exists="append", credentials=credentials)


def latestQuotes():
    sql = """
        (SELECT 
        kraken.Quote AS Quote,
        kraken.Asset as asset,  
        kraken.Plateforme as plateforme, 
        TIMESTAMP_SECONDS(CAST(kraken.Datetime as INT64)) AS datetime,  
        from `crytpoQuotes.kraken`  as kraken
        WHERE kraken.Datetime > 1601535890 
        and kraken.Asset is not null 
        and kraken.Datetime is not null 
        and kraken.Datetime  > 1601535890
        ORDER BY kraken.Datetime DESC
        LIMIT 1)
        UNION DISTINCT
        (
        SELECT 
        deribit.Quote AS Quote,
        deribit.Asset as asset,  
        deribit.Plateforme as plateforme, 
        TIMESTAMP_SECONDS(CAST(deribit.Datetime as INT64)) AS datetime,  
        FROM `crytpoQuotes.deribit`  as deribit
        WHERE deribit.Datetime > 1601535890 
        and deribit.Asset is not null 
        and deribit.Datetime is not null 
        and deribit.Datetime  > 1601535890
        ORDER BY deribit.Datetime DESC
        LIMIT 1)
        UNION DISTINCT
        (
        SELECT 
        coinbase.Quote AS Quote,
        coinbase.Asset as asset,  
        coinbase.Plateforme as plateforme, 
        TIMESTAMP_SECONDS(CAST(coinbase.Datetime as INT64)) AS datetime,  
        FROM `crytpoQuotes.coinbase`  as coinbase
        WHERE coinbase.Datetime > 1601535890 
        and coinbase.Asset is not null 
        and coinbase.Datetime is not null 
        and coinbase.Datetime  > 1601535890
        ORDER BY coinbase.Datetime DESC
        LIMIT 1)
        """
    job = pandas_gbq.read_gbq(sql, project_id="heartbeat-001", credentials=credentials)
    return job.to_json(orient="records")
#implicit()
