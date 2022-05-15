import sys

import boto3
import requests
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

def JsonToDataframe(json):
    return pd.DataFrame.from_dict(pd.json_normalize(json), orient='columns')

def UploadToS3(df, name):
    csv = df.to_csv()

    with open(name, "wt") as f:
        f.write(csv)
    
    s3 = boto3.client("s3")
    with open(name, "rb") as f:
        s3.upload_fileobj(f, "saliy-s3-labs", name)

def UploadFileToS3(file_name):
    with open(file_name, "rb") as f:
        s3.upload_fileobj(f, "saliy-s3-labs", file_name)

# load json of currency
date = 20210101

for i in range(0, 12):
    url = f"https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?date={date}&json"
    #load json from url
    r = requests.get(url).json()
    #Convert json to dataframe
    df = JsonToDataframe(r)
    #Upload to bucket
    UploadToS3(df, f"c_{date}")
    date += 100

date = 20210101

dates = []
usd_rate = []
eur_rate = []
for i in range(0, 12):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket = "saliy-s3-labs", Key=f"c_{date}.csv")
    df = pd.read_csv(obj["Body"])
    print(date)
    print(df.head(5))
    
    usd_rate.append((df[df['cc'] == 'USD']['rate']).values[0])
    eur_rate.append((df[df['cc'] == 'EUR']['rate']).values[0])
    dates.append(i + 1)
    
    date += 100

d = np.array(dates)
usd = np.array(usd_rate)
eur = np.array(eur_rate)

plt.plot(d, usd)
plt.title("UAH per USD")
plt.savefig('uah2usd.png')
plt.show()

plt.plot(d, eur)
plt.title("UAH per EUR")
plt.savefig('uah2eur.png')
plt.show()

UploadFileToS3('uah2usd.png')
UploadFileToS3('uah2eur.png')
