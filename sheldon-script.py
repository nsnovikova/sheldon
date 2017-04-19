# -*- encoding: utf-8 -*-

#vk api imports
from __future__ import unicode_literals, print_function
import pprint
from urllib.parse import parse_qs
import webbrowser
import pickle
from datetime import datetime, timedelta
import vk
import time
import csv
import itertools

#fb api imports
from facebookads.api import FacebookAdsApi
from facebookads import objects
from facebookads import adobjects

#google sheets api imports
import httplib2
import os

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

##### DATES RANGES FOR API #######
FB_DATE_FROM = '<START-DATE-FOR-FB-STATISTICS>'
FB_DATE_TO = '<END-DATE-FOR-FB-STATISTICS>'

PERIOD = 'month' # day, month, overall
DATE_FROM = '<START-DATE-FOR-VK-STATISTICS>'
DATE_TO = DATE_FROM

GA_START_DATE = '<START-DATE-FOR-GA-STATISTICS>'
GA_END_DATE = '<END-DATE-FOR-GA-STATISTICS>'

GA_GOAL_NUMBER = '<GOAL NUMBER FROM GA>' # ID of the Goal to calculate CPA on

############ SPREADSHEET PARAMETERS ##################

SPREADSHEET_ID = '<SPREADSHEET ID TO PLACE DATA TO>'
SHEET_TITLE = '<SPREADSHEET TITLE>'

EXCEL_FILENAME = 'ad_report.csv'
USDTORUB = 56.07

############ FACEBOOK PARAMETERS, get token here: https://developers.facebook.com/apps/407029356343576/marketing-api/tools/ ###################
FB_APP_ID = '<APP_ID>'
FB_APP_SECRET = '<APP_SECRET>'
FB_ACCESS_TOKEN = '<ACCESS_TOKEN>'
FB_AD_ACCOUNT_ID = '<AD_ACCOUNT_ID_TO_GET_STATISTICS_FROM>'


SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = '<PATH_TO_SECRET_JSON_FILE>'
APPLICATION_NAME = 'Sheldon'
CREDENTIALS_FILENAME = '<FILE_TO_STORE_DATA_TO>'

######## GA PARAMETERS #####################
GA_ACCOUNT_EMAIL = ''

GA_SCOPES = 'https://www.googleapis.com/auth/analytics.readonly'
GA_CLIENT_SECRET_FILE = ''
GA_APPLICATION_NAME = 'Sheldon'
GA_CREDENTIALS_FILENAME = ''

GA_SERVICE_ACCOUNT_EMAIL = ''
GA_VIEW_ID = '


########### VK PARAMETERS ########################
APP_ID = ""
ACCOUNT_ID = ""
AUTH_FILE = '.auth_data'
FORBIDDEN_CHARS = '/\\\?%*:|"<>!'




#################  VK API CODE #######################

def get_saved_auth_params():
    access_token = None
    user_id = None
    try:
        with open(AUTH_FILE, 'rb') as pkl_file:
            token = pickle.load(pkl_file)
            expires = pickle.load(pkl_file)
            uid = pickle.load(pkl_file)
        if datetime.now() < expires:
            access_token = token
            user_id = uid
    except IOError:
        pass
    return access_token, user_id


def save_auth_params(access_token, expires_in, user_id):
    expires = datetime.now() + timedelta(seconds=int(expires_in))
    with open(AUTH_FILE, 'wb') as output:
        pickle.dump(access_token, output)
        pickle.dump(expires, output)
        pickle.dump(user_id, output)


def get_auth_params():
    auth_url = ("https://oauth.vk.com/authorize?client_id={app_id}"
                "&scope=ads&redirect_uri=http://oauth.vk.com/blank.html"
                "&display=page&response_type=token".format(app_id=APP_ID))
    print("Copy and paste this url to your browser:")
    print(auth_url)
    webbrowser.open_new_tab(auth_url)
    redirected_url = input("Paste here url you were redirected:\n")
    aup = parse_qs(redirected_url)
    aup['access_token'] = aup.pop(
        'https://oauth.vk.com/blank.html#access_token')
    save_auth_params(aup['access_token'][0], aup['expires_in'][0],
                     aup['user_id'][0])
    return aup['access_token'][0], aup['user_id'][0]


def get_api(access_token):
    session = vk.Session(access_token=access_token)
    return vk.API(session)

def get_campaigns(api, account_id, include_deleted=0):
    data_dict = {
        'account_id': account_id,
        'include_deleted': include_deleted
    }
    return api.ads.getCampaigns(**data_dict)

def get_ads(api, account_id, include_deleted=0):
    data_dict = {
        'account_id': account_id,
        'include_deleted': include_deleted
    }
    return api.ads.getAds(**data_dict)

def get_statistics(api, account_id, ids, period, date_from, date_to, ids_type):
    data_dict = {
        'account_id': account_id,
        'ids_type': ids_type,
        'ids': ids,
        'period': period,
        'date_from':date_from,
        'date_to':date_to
    }
    return api.ads.getStatistics(**data_dict)
    
def convert_vk_statistics_to_report(ads, statistics):

    res = []
    for stat in statistics:
        ad = None
        for a in ads:
            if str(a['id']) == str(stat['id']):
                ad = a
                break   
        if ad and len(stat['stats']):
            ad_stat = stat['stats'][0]
            name = ad['name'] if 'name' in ad.keys() else 'None'
            status = ad['status'] if 'status' in ad.keys() else 'None'
            impressions = ad_stat['impressions'] if 'impressions' in ad_stat.keys() else 'None'
            reach = ad_stat['reach'] if 'reach' in ad_stat.keys() else 'None'
            spent = ad_stat['spent'] if 'spent' in ad_stat.keys() else 'None'
            res.append({
                'vk_date_from':DATE_FROM,
                'vk_period':PERIOD,
                'vk_name':name,
                'vk_status':status,
                'vk_impressions':impressions,
                'vk_reach':reach,
                'vk_spent':spent
                })
    return res

def vk_api_init():
    access_token, _ = get_saved_auth_params()
    if not access_token or not _:
        access_token, _ = get_auth_params()
    api = get_api(access_token)
    return api

def get_vk_statistics_report(api):
    ads = get_ads(api, account_id=ACCOUNT_ID)
    vk_statistics = []
    if len(ads) > 0:
        ids = [v['id'] for v in ads]
        statistics = get_statistics(api, account_id=ACCOUNT_ID, ids=ids, period=PERIOD, date_from=DATE_FROM, date_to=DATE_TO, ids_type='ad')
        vk_statistics = convert_vk_statistics_to_report(ads, statistics)
    else:
        print("There is no active ads")

    return vk_statistics
    

#################### Google sheets API code #####################

def get_credentials(credentials_filename, application_name, client_secret_file, scopes):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   credentials_filename)

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(client_secret_file, scopes)
        flow.user_agent = application_name
        if flags:
            credentials = tools.run_flow(flow, store, flags)
#        else: # Needed only for compatibility with Python 2.6
#            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def push_statistics_to_file(statistics, header_values, filename=EXCEL_FILENAME):
    fl = open(filename, 'w')
    writer = csv.writer(fl)
    writer.writerow(header_values)

    for stat in statistics:
        values = []
        for h_value in header_values:
            if h_value in stat:
                values.append(stat[h_value])
            else:
                values.append('No_data')
        writer.writerow(values)
        
    fl.close()


def push_statistics_to_spreadsheet(service, spreadsheet, statistics):
    '''
        Final report consist of next fields:
        ('ga_source',
            'ga_campaign',
            'ga_sessions',
            'ga_conversions')
    ('fb_ad_name',
    'fb_date_start',
    'fb_date_stop',
    'fb_impressions',
     'fb_objective',
     'fb_spend',
     'fb_clicks',
     'fb_score',
     'fb_negative_feedback',
     'fb_positive_feedback',
     'fb_status')

    ('vk_name',
     'vk_date_from',
     'vk_period',
     'vk_status',
     'vk_impressions',
     'vk_reach',
     'vk_spent')
    '''

    spreadsheet_id = spreadsheet['spreadsheetId']
    
    ### CLEAR SPREADSHEET #####
    range_= SHEET_TITLE
    clear_values_request_body = {}
    results = service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id,
                                                    range=range_,
                                                    body=clear_values_request_body).execute()

    if len(results):
        print("Spreadsheet successfully cleared... Start adding values...")
    else:
        print("Can't clear spreadsheet...")
        return
    
    ### ADD HEADER ###
    header_values = ['ga_source',
             'ga_campaign',
             'ga_sessions',
             'ga_conversions',

             'fb_ad_name',
             'fb_date_start',
             'fb_date_stop',
             'fb_impressions',
             'fb_objective',
             'fb_spend',
             'fb_clicks',
             'fb_score',
             'fb_negative_feedback',
             'fb_positive_feedback',
             'fb_status',

             'vk_name',
             'vk_date_from',
             'vk_period',
             'vk_status',
             'vk_impressions',
             'vk_reach',
             'vk_spent'
    ]
    range_= SHEET_TITLE
    value_input_option = 'USER_ENTERED'
    value_range_body = {
        "range": range_,
        "majorDimension": "ROWS",     
        "values": [
           header_values
        ]
    }
    results = service.spreadsheets().values().update(spreadsheetId = spreadsheet_id,
                                                     range=range_,
                                                     valueInputOption=value_input_option,
                                                     body=value_range_body).execute()
    if len(results):
        print("Header successfully added...")
    else:
        print("Can't add header...")
        return

    ### STATISTIC ###
    range_ = SHEET_TITLE
    value_input_option = 'USER_ENTERED'
    insert_data_option = 'OVERWRITE'
    print("Length of statistics (if more than 100, then write to file)")
    print(len(statistics))

    if len(statistics) < 100:
        for stat in statistics:
            values = []
            for h_value in header_values:
                if h_value in stat:
                    values.append(stat[h_value])
                else:
                    values.append('No_data')
            
            value_range_body = {
                "range": range_,
                "majorDimension": "ROWS",     
                "values": [ values ]
            }
            service.spreadsheets().values().append(spreadsheetId=spreadsheet_id,
                                                   range=range_,
                                                   valueInputOption=value_input_option,
                                                   insertDataOption=insert_data_option,
                                                   body=value_range_body).execute()
    else:
        push_statistics_to_file(statistics, header_values)


    print("Values successfully added...")


    



############ GA CODE ###########################################
def get_ga_report(analytics):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.

    goalNumber = "ga:goal" + GA_GOAL_NUMBER + "Completions"    
    return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
            'viewId': GA_VIEW_ID,
            'dateRanges': [{'startDate': GA_START_DATE, 'endDate': GA_END_DATE}],
            'dimensions': [{"name":"ga:source"},{"name":"ga:campaign"}],
            'metrics': [{"expression": "ga:sessions"},{"expression": goalNumber}]
        }]
      }
  ).execute()


def get_ga_data_report():
    credentials = get_credentials(credentials_filename=GA_CREDENTIALS_FILENAME,
                                  application_name=GA_APPLICATION_NAME,
                                  client_secret_file=GA_CLIENT_SECRET_FILE,
                                  scopes=GA_SCOPES)
    http = credentials.authorize(httplib2.Http())

    discoveryUrl = ('https://analyticsreporting.googleapis.com/$discovery/rest?' + 'version=v3')
    service = discovery.build('analytics', 'v4', http=http, discoveryServiceUrl=discoveryUrl)

    ga_report = get_ga_report(service)

    ###### convert ga response to [{},{}] #####
    rows = ga_report['reports'][0]['data']['rows'] 
    res = []
    print("Ga report data successfully returned...")
    for row in rows:
        dimensions_list = row['dimensions']
        metrics_list = row['metrics'][0]['values']
                   
        source = dimensions_list[0] if len(dimensions_list) > 0 else 'None'
        campaign = dimensions_list[1] if len(dimensions_list) > 0 else 'None'
        sessions = metrics_list[0] if len(metrics_list) > 0 else 'None'
        conversions = metrics_list[1] if len(metrics_list) > 0 else 'None'
        
        res.append({'ga_source':source,
                    'ga_campaign':campaign,
                    'ga_sessions':sessions,
                    'ga_conversions':conversions})
    return res


#################### Facebook API code #####################
def fb_api_init():
    FacebookAdsApi.init(FB_APP_ID, FB_APP_SECRET, FB_ACCESS_TOKEN)
    me = objects.AdUser(fbid='me')

    return me

def get_fb_campaign_statistics(me):
    ad_account = objects.AdAccount('act_' + FB_AD_ACCOUNT_ID)
    ads = ad_account.get_ads()
    statistics = []
    
    insights = ad_account.get_insights(fields=[
            adobjects.adsinsights.AdsInsights.Field.ad_name,
            adobjects.adsinsights.AdsInsights.Field.clicks,
            adobjects.adsinsights.AdsInsights.Field.impressions,
            adobjects.adsinsights.AdsInsights.Field.relevance_score,
            adobjects.adsinsights.AdsInsights.Field.spend
        ],
        params={
            'time_range': {'since':FB_DATE_FROM,'until':FB_DATE_TO},
            'level': 'ad',
            'limit': 1000
        }
    )

    for insight in insights:
        statistics.append(insight)

    return statistics

def convert_fb_stat_to_report(fb_statistics):
    '''
     <AdsInsights> {
        "ad_name": "",
        "date_start": "2017-04-01",
        "date_stop": "2017-04-17",
        "impressions": "62",
        "objective": "LINK_CLICKS",
        "relevance_score": {
            "negative_feedback": "LOW",
            "positive_feedback": "MEDIUM",
            "score": "4",
            "status": "OK"
        },
        "spend": "0",
        "clicks": "1"
    }

    TO

    [{},{}]
    '''

    statistics = []

    for adInsight in fb_statistics:
        o = {}
        o['fb_ad_name'] = adInsight.get('ad_name')
        o['fb_date_start'] = adInsight.get('date_start')
        o['fb_date_stop'] = adInsight.get('date_stop')
        o['fb_impressions'] = adInsight.get('impressions')
        o['fb_objective'] = adInsight.get('objective')
        o['fb_spend'] = adInsight.get('spend')
        o['fb_clicks'] = adInsight.get('clicks')

        rel_score = adInsight.get('relevance_score')
        o['fb_score'] = 'None'
        o['fb_negative_feedback'] = 'None'
        o['fb_positive_feedback'] = 'None'
        o['fb_status'] = rel_score['status']

        if rel_score and (rel_score['status'] == "OK"):
            o['fb_score'] = rel_score['score']
            o['fb_negative_feedback'] = rel_score['negative_feedback']
            o['fb_positive_feedback'] = rel_score['positive_feedback']
            o['fb_status'] = rel_score['status']
        
        statistics.append(o)

    return statistics


def add_vk_data_to_report(ga_statistics_report, vk_statistics_report, source='vk'):
    '''
        ga_statistics_report keys expected: ga_source, ga_campaign, ga_sessions, ga_conversions
        vk_statistics_report keys expected: vk_name, vk_date_from, vk_period, vk_status, vk_impressions, vk_reach, vk_spent

        return: [{'source', 'campaign_name', 'spent', 'impressions', 'clicks', 'conversions', 'cpm', 'cpa'}, {}, ...]
    '''

    res = []
    
    for vk_stat in vk_statistics_report:
        stat = {}
        stat['source'] = source
        stat['campaign_name'] = vk_stat['vk_name']
        stat['spent'] = vk_stat['vk_spent']
        stat['impressions'] = vk_stat['vk_impressions']

        stat['clicks'] = 0
        stat['conversions'] = 0
        stat['cpm'] = 0
        stat['cpa'] = 0
        
        for ga_stat in ga_statistics_report:
            if (ga_stat['ga_source'] == source) and (ga_stat['ga_campaign'] == vk_stat['vk_name']):
                stat['clicks'] = ga_stat['ga_sessions']
                stat['conversions'] = ga_stat['ga_conversions']
                break
        try:
            if int(stat['impressions']) > 0 and stat['spent'] is not None:
                stat['cpm'] = float(stat['spent']) / (float(stat['impressions']) / 1000)
        except:
            pass

        try:
            if int(stat['conversions']) > 0:
                stat['cpa'] = float(stat['spent']) / float(stat['conversions'])
        except:
            pass
        res.append(stat)
    return res


def add_fb_data_to_report(ga_statistics_report, fb_statistics_report, source='fb'):
    '''
        ga_statistics_report keys expected: ga_source, ga_campaign, ga_sessions, ga_conversions
        fb_statistics_report keys expected: fb_ad_name, fb_impressions, fb_spend, fb_clicks

        return: [{'source', 'campaign_name', 'spent', 'impressions', 'clicks', 'conversions', 'cpm', 'cpa'}, {}, ...]
    '''

    res = []
    
    for fb_stat in fb_statistics_report:
        stat = {}
        stat['source'] = 'fb'
        stat['campaign_name'] = fb_stat['fb_ad_name']
        stat['spent'] = float(fb_stat['fb_spend']) * USDTORUB
        stat['impressions'] = fb_stat['fb_impressions']

        stat['clicks'] = fb_stat['fb_clicks']
        stat['conversions'] = 0
        stat['cpm'] = 0
        stat['cpa'] = 0
        
        for ga_stat in ga_statistics_report:
            if (ga_stat['ga_source'] in ['fb', 'insta']) and (ga_stat['ga_campaign'] == fb_stat['fb_ad_name']):
                stat['clicks'] = ga_stat['ga_sessions']
                stat['conversions'] = ga_stat['ga_conversions']
                stat['source'] = ga_stat['ga_source']
                break

        try:
            if int(stat['impressions']) > 0:
                stat['cpm'] = float(stat['spent']) / (float(stat['impressions']) / 1000)
        except:
            pass

        try:
            if int(stat['conversions']) > 0:
                stat['cpa'] = float(stat['spent']) / float(stat['conversions'])
        except:
            pass
        
        res.append(stat)
    return res


def main():
########### FINAL STATISTICS ###########################################
# ['source', 'campaign_name', 'spent', 'impressions', 'clicks', 'cpm', 'cpa']
    statistics = []
    
########### GET GA DATA ###########################################
    ga_statistics_report = get_ga_data_report()
    push_statistics_to_file(ga_statistics_report, ["ga_source",
                                         "ga_campaign",
                                         "ga_sessions",
                                         "ga_conversions"], filename='ga_raw.csv')

############ GET FB DATA ###########################################
    api = fb_api_init()
    fb_statistics = get_fb_campaign_statistics(api)
    fb_statistics_report = convert_fb_stat_to_report(fb_statistics)

    push_statistics_to_file(fb_statistics_report, ["fb_ad_name",
                                                   "fb_date_start",
                                                   "fb_date_stop",
                                                   "fb_impressions",
                                                   "fb_objective",
                                                   "fb_spend",
                                                   "fb_clicks",
                                                   "fb_score",
                                                   "negative_feedback",
                                                   "positive_feedback",
                                                   "status"
                                                   ], filename='fb_raw.csv')

    stat = add_fb_data_to_report(ga_statistics_report, fb_statistics_report)
    statistics = itertools.chain(statistics, stat)
    
############ GET VKONTAKTE DATA ###########################################
    vk_api = vk_api_init()
    vk_statistics_report = get_vk_statistics_report(vk_api)
    push_statistics_to_file(vk_statistics_report, ["vk_name",
                                                   "vk_date_from",
                                                   "vk_period",
                                                   "vk_status",
                                                   "vk_impressions",
                                                   "vk_reach",
                                                   "vk_spent"
                                                   ], filename='vk_raw.csv')

    
    stat = add_vk_data_to_report(ga_statistics_report, vk_statistics_report)
    statistics = itertools.chain(statistics, stat)

    push_statistics_to_file(statistics, ["source",
                                                   "campaign_name",
                                                   "spent",
                                                   "impressions",
                                                   "clicks",
                                                   "conversions",
                                                   "cpm",
                                                    "cpa"
                                                   ], filename='report.csv')


############ PUSH statistics TO SPREADSHEET ###########################################
#    credentials = get_credentials(credentials_filename=CREDENTIALS_FILENAME,
#                                  application_name=APPLICATION_NAME,
#                                  client_secret_file=CLIENT_SECRET_FILE,
#                                  scopes=SCOPES)
#    http = credentials.authorize(httplib2.Http())
#    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?' + 'version=v4')
#    service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discoveryUrl)

#    spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
#    push_statistics_to_spreadsheet(service, spreadsheet, statistics)

main()
