#!/usr/bin/env python3
import boto3
import awswrangler as wr
import pandas as pd
from faker import Faker
from uuid import uuid4
import random
import hashlib
import datetime

####################################
# Using this script
# 1) Setup AWS CLI profiles for both accounts
# 2) update the profiles in this script
# 3) install awswrangler and faker (via pip)
# 4) run this script
####################################

## Variables
account_1_profile = "acc54"  # enter profile for first account
account_2_profile = "acc68"  # enter profile for second account

airline_crm_num_rows = 5000
airline_conversion_num_rows = 1000

socialco_impressions_num_rows = 100000

# Pool of users in socialco data set = socialco_impressions_users_stop - socialco_impressions_user_start
# % airline users in socialco user pool = (1 - (socialco_impressions_user_start/airline_crm_num_rows))*100
socialco_impressions_user_start = 0
socialco_impressions_users_stop = 10000

fake = Faker()

# US population by state https://worldpopulationreview.com/states
# cat  ~/Downloads/data.json  | jq 'map({"key": .State, "value": .Pop}) | from_entries' | pbcopy
state_weights = {
    "California": 39664128,
    "Texas": 30097526,
    "Florida": 22177997,
    "New York": 19223191,
    "Pennsylvania": 12805190,
    "Illinois": 12518071,
    "Ohio": 11727377,
    "Georgia": 10936299,
    "North Carolina": 10807491,
    "Michigan": 9995212,
    "New Jersey": 8870685,
    "Virginia": 8638218,
    "Washington": 7887965,
    "Arizona": 7640796,
    "Tennessee": 7001803,
    "Massachusetts": 6922107,
    "Indiana": 6842385,
    "Missouri": 6184843,
    "Maryland": 6075314,
    "Colorado": 5961083,
    "Wisconsin": 5867518,
    "Minnesota": 5739781,
    "South Carolina": 5342388,
    "Alabama": 4949697,
    "Louisiana": 4616106,
    "Kentucky": 4487233,
    "Oregon": 4325290,
    "Oklahoma": 4007179,
    "Connecticut": 3546588,
    "Utah": 3363182,
    "Nevada": 3238601,
    "Puerto Rico": 3194714,
    "Iowa": 3174426,
    "Arkansas": 3042017,
    "Mississippi": 2961536,
    "Kansas": 2919179,
    "New Mexico": 2109093,
    "Nebraska": 1960790,
    "Idaho": 1896652,
    "West Virginia": 1755715,
    "Hawaii": 1401709,
    "New Hampshire": 1378449,
    "Maine": 1359677,
    "Montana": 1093117,
    "Rhode Island": 1062583,
    "Delaware": 998619,
    "South Dakota": 902542,
    "North Dakota": 774008,
    "Alaska": 720763,
    "District of Columbia": 718355,
    "Vermont": 622882,
    "Wyoming": 582233,
}


def airline_crm(num_rows=1):
    return [
        {
            "identifier": hashlib.sha256(bytes(x)).hexdigest(),
            "name": fake.name(),
            "ids_r_us_id": fake.sha256(raw_output=False),
            "expected_ltv": random.randint(0, 1000000),
            "status": random.choices(
                [None, "apprentice", "journeyman", "master"], weights=[60, 17, 2, 1]
            )[0],
            "business_traveler": fake.boolean(chance_of_getting_true=25),
            "last_trip": datetime.date.today()
            - datetime.timedelta(days=random.randint(0, 1000)),
            "city": fake.city(),
            "state": random.choices(
                list(state_weights.keys()), list(state_weights.values())
            )[0],
            "usercode": random.randint(1000, 2000),
        }
        for x in range(num_rows)
    ]


def airline_conversions(num_rows, num_users):
    # num_users - number of users the airline has, this enables an overlap with the airline_crm table
    return [
        {
            "identifier": hashlib.sha256(
                bytes(random.randint(0, num_users))
            ).hexdigest(),
            "pixel_id": "67d34640-b7a4-42a8-b821-6434d70f08a4",
            "sale_date": fake.date_this_month(before_today=True, after_today=True),
            "event_type": "PURCHASE",
            "version": "2.0",
            "price": random.randint(50, 2000),
            "currency": "usd",
            "transaction_id": fake.uuid4(),
        }
        for x in range(num_rows)
    ]

def create_tag2word():
    f = open('./query2people.txt')

    tag2word_arr = [
        {
            "tag": line.split()[1],
            "word": line.split()[3]
        }
        for line in f.readlines() if len(line.split()) == 4
    ]

    return tag2word_arr, [ pr['word'] for pr in tag2word_arr]


tag2word_arr, word_list = create_tag2word()

## Create Airline Data Sets
bucket_name = "cleanrooms-demo-" + str(uuid4())
session = boto3.Session(profile_name=account_1_profile)

s3_client = session.client("s3")

database = f"cleanrooms-demo"
print(s3_client.create_bucket(Bucket=bucket_name))

wr.catalog.create_database(name=database, exist_ok=True, boto3_session=session)

# wr.s3.to_parquet(
#     df=pd.DataFrame(airline_crm(num_rows=airline_crm_num_rows)),
#     path=f"s3://{bucket_name}/{database}/airline_crm",
#     dataset=True,
#     database=database,
#     table="airline_crm",
#     boto3_session=session,
# )

wr.s3.to_parquet(
    df=pd.DataFrame(
        airline_conversions(
            num_rows=airline_conversion_num_rows, num_users=airline_crm_num_rows
        )
    ),
    path=f"s3://{bucket_name}/{database}/airline_conversions",
    dataset=True,
    partition_cols=["sale_date"],
    database=database,
    table="airline_conversions",
    boto3_session=session,
)

wr.s3.to_parquet(
    df=pd.DataFrame(tag2word_arr),
    path=f"s3://{bucket_name}/{database}/tag_word_mapping",
    dataset=True,
    database=database,
    table="tag_word_mapping",
    boto3_session=session,
)


## Create SocialCo data set

def socialco_impressions(num_rows, users_start, users_stop):
    # users_start, users_stop - sets range for emails, this can be used to determine % overlap with the airline data set
    base_data = [
        {
            "identifier": hashlib.sha256(
                bytes(random.randint(users_start, users_stop))
            ).hexdigest(),
            "impression_date": fake.date_this_month(
                before_today=True, after_today=True
            ),
            "campaign_id": random.randint(123456, 123466),
            "creative_id": random.randint(123456, 123466),
        }
        for x in range(num_rows)
    ]

    outlier_data = [
        {
            "identifier": hashlib.sha256(
                bytes(random.randint(users_start, users_stop))
            ).hexdigest(),
            "impression_date": fake.date_this_month(
                before_today=True, after_today=True
            ),
            "campaign_id": 10000,
            "creative_id": random.randint(123456, 123466),
        }
        for x in range(10)
    ]

    base_data.extend(outlier_item)

    return base_data

def create_user2word(num_rows, word_candidates):
    cand_len = len(word_candidates)
    return [
        {
            "identifier": hashlib.sha256(
                bytes(random.randint(0, cand_len*10))
            ).hexdigest(),
            "query_word" : word_candidates[random.randint(0, cand_len-1)],
            "query_date": fake.date_this_month(
                before_today=True, after_today=True
            ),
        }
        for x in range(num_rows)
    ]

bucket_name = "cleanrooms-demo-" + str(uuid4())
session = boto3.Session(profile_name=account_2_profile)

s3_client = session.client("s3")
print(s3_client.create_bucket(Bucket=bucket_name))


database = f"cleanrooms-demo"

wr.catalog.create_database(name=database, exist_ok=True, boto3_session=session)

wr.s3.to_parquet(
    df=pd.DataFrame(
        socialco_impressions(
            num_rows=socialco_impressions_num_rows,
            users_start=socialco_impressions_user_start,
            users_stop=socialco_impressions_users_stop,
        )
    ),
    path=f"s3://{bucket_name}/{database}/socialco_impressions",
    dataset=True,
    partition_cols=["impression_date"],
    database=database,
    table="socialco_impressions",
    boto3_session=session,
)

wr.s3.to_parquet(
    df=pd.DataFrame(
		create_user2word(10000, word_list)
    ),
    path=f"s3://{bucket_name}/{database}/user_search_log",
    dataset=True,
    partition_cols=["query_date"],
    database=database,
    table="user_search_log",
    boto3_session=session
)

