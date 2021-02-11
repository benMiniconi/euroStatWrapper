import csv
import os
from os import path

csv_columns = ["Plateforme", "Asset", "Quote", "Datetime", "Bid", "BidAmount", "Ask", "AskAmount", "OpenInterest"]

def writeQuote(csv_file, quotes):
    if path.exists(csv_file):
            csv_size = os.stat(csv_file).st_size
            try:
                with open(csv_file, 'a+') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                    if csv_size == 0:
                        writer.writeheader()
                    for data in quotes:
                        writer.writerow(data)
            except IOError:
                print("I/O error")
    else:
        try:
            with open(csv_file, 'w+') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for data in quotes:
                    writer.writerow(data)
        except IOError:
            print("I/O error")