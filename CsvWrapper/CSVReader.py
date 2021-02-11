import csv
import pandas as pd
import moment
import json
from os import path


def getLatestQuoteFromCsv(plateforme=None):
    pathname = "crypto" + plateforme + moment.now().format("DDMMYYYY") + ".csv"
    if path.exists(pathname):
        frame = pd.read_csv(pathname)
        return frame.tail(1).to_json(orient="records")
    else:
        response = {}
        response["Plateforme"] = "no file"
        return json.dumps([response])


def latestQuotes():
    df = pd.read_json(getLatestQuoteFromCsv("Coinbase"), orient="records")
    deribit = pd.read_json(getLatestQuoteFromCsv("Deribit"), orient="columns")
    df = df.append(deribit)
    kraken = pd.read_json(getLatestQuoteFromCsv("Kraken"), orient="columns")
    df = df.append(kraken)
    return df
