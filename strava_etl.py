import os # os.path.exists()
import sys # sys.exit()

import requests 
import json
import datetime
import time

import pandas as pd
from pandas._libs.tslibs import Timestamp

import sqlalchemy
import sqlite3

from settings import *


def _create_tokens(code: str) -> dict:
    ''' Helper function to write & obtain tokens after the browser steps mentioned in README.md'''
    stravaTokens = requests.post(
        url='https://www.strava.com/oauth/token',
        data = {
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'code': code,
            'grant_type': 'authorization_code'
        }
    ).json()

    with open('strava_tokens.json', 'w') as of:
        json.dump(stravaTokens, of)

    return stravaTokens


def _refresh_access_token(refresh_token: str) -> dict:
    '''Helper function to overwrite & get new set of tokens after previous ones expired '''
    newStravaTokens = requests.post(
        url='https://www.strava.com/oauth/token',
        data = {
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'grant_type': 'refresh_token',
            "refresh_token": refresh_token,
        }
    ).json()

    with open('strava_tokens.json', 'w') as of:
        json.dump(newStravaTokens, of)
        
    return newStravaTokens


def auth_tokens() -> dict:
    '''
    Main function to obtain required tokens, either defined previously (.json file) or create new.
    Required to get access_token
    '''
    try:
        with open('strava_tokens.json', 'r') as check:
            stravaTokens = json.load(check)
    except:
        # Just required for initial run - shouldn't be a problem afterwards
        code = input('Please paste in the code seen in the url following Strava authenication process:\n')
        stravaTokens = _create_tokens(code)
        
    if stravaTokens['expires_at'] < time.time():
        stravaTokens = _refresh_access_token(stravaTokens['refresh_token'])

    return stravaTokens


def activities_to_pd(stravaTokens: dict, sinceTime: Timestamp) -> pd.DataFrame:
    '''
    Obtain dataframe of activities. Ignores manual entries (lots of missing data) and None values in external_id field.
    - sinceTime parameter expresses the age of data to obtain from Strava's API. 
    E.g. (now() - timedelta(weeks=1)).timestamp() -> obtains last weeks entries

    Strava limits to 100 requests every 15 minutes, 1000 daily.
    - Only one request realistly is needed per week.
    '''
    df = pd.DataFrame(columns=[
        "id","name","start_date_local","type",
        "distance","moving_time","elapsed_time","total_elevation_gain",
        "elev_high", "elev_low", "max_speed", "achievement_count",
        "end_lat", "end_lng","external_id"
        ])

    pageNum = 1
    while True:
        r = requests.get("https://www.strava.com/api/v3/activities" + "?access_token=" + stravaTokens['access_token'] +
            f"&after={sinceTime}" +
            f"&per_page=200&page={pageNum}"
        ).json()

        if not r: # empty
            break

        for i in range(len(r)):   
            # removing manual inputs and activities without any external-id -- three in total
            if r[i]['manual'] == True or r[i]['external_id'] is None:
                continue

            data_row = {name: r[i][name] for name in [c for c in df.columns if c not in ['end_lat', 'end_lng']]}
            data_row['end_lng'], data_row['end_lat'] = r[i]['end_latlng']

            df = df.append(data_row, ignore_index=True)
        pageNum += 1
        
    return df


def if_valid_data(df: pd.DataFrame, sinceTime: Timestamp) -> bool:
    ''' Checks if dataframe is valid '''
    if df.empty:
        print("No activities recorded.")
        return False
    
    # Primary key check
    if not df['id'].is_unique:
        raise Exception("Primary key violated (not unique)")
    # Check for any null values
    elif df.isnull().values.any():
        print(df.head())
        raise Exception("Null values found")

    # Check for any data older than desired extraction
    times = df['start_date_local'].to_list()
    for time in times:
        timestamp = datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%SZ').timestamp()
        if timestamp < sinceTime:
            raise Exception("Data extracted older than desired time")
    
    return True


if __name__ == "__main__":
    ####  Extract & Transform
    stravaTokens = auth_tokens()
        
    # # Grab new data -> if database exist, know to grab last weeks data
    if os.path.exists("my_strava_activities.sqlite"):
        sinceTime = (datetime.datetime.now() - datetime.timedelta(weeks=1)).timestamp()
    else:
        sinceTime = datetime.datetime(1971, 1, 1).timestamp()

    # Create df and check if valid
    df = activities_to_pd(stravaTokens, sinceTime)

    if not if_valid_data(df, sinceTime):
        sys.exit("No data found to be added")

    print("Valid data -> load stage")
    ####  Load

    engine = sqlalchemy.create_engine(DATABASE_LOC)
    connect = sqlite3.connect("my_strava_activities.sqlite")
    cursor = connect.cursor()

    # https://github.com/pandas-dev/pandas/issues/15988 -> duplicates silently ignored
    sql_query = """
    CREATE TABLE IF NOT EXISTS my_strava_activities (
        id INT(255) PRIMARY KEY ON CONFLICT IGNORE,
        name VARCHAR(200),
        start_date_local VARCHAR(200),
        type VARCHAR(200),
        distance DECIMAL(6, 1),
        moving_time INT(255),
        elapsed_time INT(255),
        total_elevation_gain DECIMAL(6, 1),
        elev_high DECIMAL(6, 1),
        elev_low DECIMAL(6, 1),
        max_speed DECIMAL(3,1),
        achievement_count INT(32),
        end_lat DECIMAL(8,6),
        end_lng DECIMAL(9,6),
        external_id VARCHAR(200)
    )
    """
    
    cursor.execute(sql_query)
    print("Opened database successfully")

    df.to_sql(name="my_strava_activities", con=engine, index=False, if_exists='append')

    connect.close()
    print("Closed database successfully")
