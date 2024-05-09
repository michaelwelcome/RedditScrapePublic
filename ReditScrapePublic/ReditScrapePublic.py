
import requests
import pandas as pd
import datetime
import time

#globals
CLIENT_ID = 'CLIENT_ID'
CLIENT_SECRET = 'CLIENT_SECRET'
USER_AGENT = 'USER_AGENT'
USER_ID = 'USER_ID'
PASSWORD = 'PASSWORD'
ACCESS_TOKEN = ''
RUN_MINUTES = 15
PRINT_SECONDS = 30
SUB_REDDIT = 'memes'



#function definitions
def get_access_token():
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)

    data = {
            'grant_type' : 'password',
            'username' : USER_ID,
            'password' : PASSWORD
        }

    access_token_headers = {
        'User-Agent': USER_AGENT}

    response = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, headers=access_token_headers, data=data)

    return response.json()['access_token']

def get_oauth_headers():
    bearer = "bearer {x}".format(x=ACCESS_TOKEN)

    return {
        'User-Agent': USER_AGENT,
        'Authorization': bearer}

def get_reddit_new_response_list(sub_reddit):
    resp_list = []

    response = requests.get('https://oauth.reddit.com/r/{x}/new'.format(x=sub_reddit), headers=oauth_headers, params={'limit': 100})

    xrate_limit_remaining = response.headers['x-ratelimit-remaining']
    xrate_limit_remaining_float = float(xrate_limit_remaining)
    print('xrate_limit_remaining = ', xrate_limit_remaining)
    if(xrate_limit_remaining_float < 25.0):
        time.sleep(10.0)
    elif(xrate_limit_remaining_float < 50.0):
        time.sleep(2.0)
    elif(xrate_limit_remaining_float < 100.0):
        time.sleep(1.0)


    for post in response.json()['data']['children']:
        resp_list.append( {
            'Id': post['data']['id'],
            'Ups': post['data']['ups'],
            'Author': post['data']['author'],
            'Author_Count': 1,
            })

    return resp_list

def insert_new_responses(df, resp_list):
    resp_list_new = []
    df_new = pd.DataFrame()

    if df.empty:
        #on the first pass the dataframe is empty and all records need to be added
        df_new = pd.DataFrame(resp_list)
    else:
        #create a set of ids, this is used to determine if the item has already been added
        id_set = set(df.Id)

        #check each item in the incoming list, if it does not exist add it to the new list
        for rec in resp_list:
            exists = rec['Id'] in id_set
            if(not exists):
                resp_list_new.append(rec)

        #if there are items in the new list combine them with the existing items and rebuild the dataframe
        if(len(resp_list_new) > 0):
            df_temp = pd.DataFrame(resp_list_new)
            df_new = pd.concat([df,df_temp])
        else:
            #just return the old dataframe
            df_new = df

    return df_new

def print_stats(df, last_print_time):
    print('call count = ', call_count)
    print('data frame count = ', len(df))

    print_elapsed_seconds = (current_time - last_print_time).total_seconds()

    if(print_elapsed_seconds >= PRINT_SECONDS):
        last_print_time = datetime.datetime.now()
        print('')
        print('Most up votes')
        print(df.sort_values(by=['Ups'], ascending=False).head(10))
        agr_df = df.groupby('Author').agg({'Author_Count': 'sum'})
        print('')
        print('Most Posts By a User')
        print(agr_df.sort_values(by=['Author_Count'], ascending=False).head(10))

    return last_print_time


#get access token
ACCESS_TOKEN = get_access_token()

#get oauth headers
oauth_headers = get_oauth_headers()

#initializations

start_time = datetime.datetime.now()
current_time = datetime.datetime.now()
last_print_time = datetime.datetime.now()
call_count = 0.0
df = pd.DataFrame()


while(True):
    print('')
    current_time = datetime.datetime.now()
    prog_elapsed_seconds = (current_time - start_time).total_seconds()
    resp_list = get_reddit_new_response_list(SUB_REDDIT)
    call_count = call_count + 1.0
    df = insert_new_responses(df, resp_list)
    last_print_time = print_stats(df, last_print_time)

    if (prog_elapsed_seconds >= RUN_MINUTES*60):
        break;

