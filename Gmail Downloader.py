#!/usr/bin/env python
# coding: utf-8
import os
from imbox import Imbox # pip install imbox
import traceback
import pprint as pp
import numpy as np
import pandas as pd
import configparser
from datetime import date
pd.options.mode.chained_assignment = None  # default='warn'
from oauth2client.service_account import ServiceAccountCredentials
import pygsheets

# enable less secure apps on your google account
# https://myaccount.google.com/lesssecureapps

#os.chdir('/Users/garrett/Desktop/codes/CRMEmailAttachmentDownloader')


host = "imap.gmail.com"

# Getting credentials from secure ini file
email_ini_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'GmailLogin.ini')
print("The email ini path is looking here: " + email_ini_path)
email_config = configparser.ConfigParser()
email_config.read(email_ini_path)
username = email_config['Gmail']['user']
password = email_config['Gmail']['password']

print('Username is: ',username)
print('Password is: ',password)

try:
    os.makedirs(os.path.join('retrieved_downloads','vinsolutions'))
except:
    pass

primary_working_directory = os.getcwd()
download_folder = os.path.join(os.getcwd(),'retrieved_downloads','vinsolutions')

if not os.path.isdir(download_folder):
    os.makedirs(download_folder, exist_ok=True)

#Builds list of day params as integers to match imbox's weird snytax
today = date.today().strftime('%Y,%m,%d').split(',')
today = [ int(x) for x in today ]
print(today)

mail = Imbox(host, username=username, password=password, ssl=True, ssl_context=None, starttls=False)

#This is Part 1 of 2 that downloads the BDC Data from emails in today's inbox
messages = mail.messages(subject='VSLeadPull', date__on=date(today[0], today[1], today[2])) # Subject Contains String

for (uid, message) in messages:
    mail.mark_seen(uid) # optional, mark message as read
    for idx, attachment in enumerate(message.attachments):
        try:
            att_fn = attachment.get('filename')
            download_path = f"{download_folder}/BDC_{att_fn}"
            print(uid)
            print(download_path)
            with open(download_path, "wb") as fp:
                fp.write(attachment.get('content').read())
        except:
            pass
            print(traceback.print_exc())

#This is Part 2 of 2 that downloads the Showroom Data from emails in today's inbox
messages = mail.messages(subject='VSShowroomPull', date__on=date(today[0], today[1], today[2])) # Subject Contains String

for (uid, message) in messages:
    mail.mark_seen(uid) # optional, mark message as read
    for idx, attachment in enumerate(message.attachments):
        try:
            att_fn = attachment.get('filename')
            download_path = f"{download_folder}/Showroom_{att_fn}"
            print(uid)
            print(download_path)
            with open(download_path, "wb") as fp:
                fp.write(attachment.get('content').read())
        except:
            pass
            print(traceback.print_exc())



mail.logout()


"""
Available Message filters:

# Gets all messages from the inbox
messages = mail.messages()

# Unread messages
messages = mail.messages(unread=True)

# Flagged messages
messages = mail.messages(flagged=True)

# Un-flagged messages
messages = mail.messages(unflagged=True)

# Flagged messages
messages = mail.messages(flagged=True)

# Un-flagged messages
messages = mail.messages(unflagged=True)

# Messages sent FROM
messages = mail.messages(sent_from='sender@example.org')

# Messages sent TO
messages = mail.messages(sent_to='receiver@example.org')

# Messages received before specific date
messages = mail.messages(date__lt=datetime.date(2018, 7, 31))

# Messages received after specific date
messages = mail.messages(date__gt=datetime.date(2018, 7, 30))

# Messages received on a specific date
messages = mail.messages(date__on=datetime.date(2018, 7, 30))

# Messages whose subjects contain a string
messages = mail.messages(subject='Christmas')

# Messages from a specific folder
messages = mail.messages(folder='Social')
"""


# Changes Python to Download Folder
os.chdir(download_folder)
# Creates list array of files downloaded
files = os.listdir(download_folder)
print(files)


for file in files:
    ftype = file.split("_")[0]
    print(ftype)


#Build Master DataFrame of all imported data
#Note that DF is currently the BDC master table
df_showroom = pd.DataFrame()
df = pd.DataFrame()
for file in files:
    ftype = file.split("_")[0]
    if ftype == 'BDC':
        info = pd.read_excel(file)
        df = df.append(info, ignore_index=True)
        df.drop_duplicates(keep='first', inplace=True)
    elif ftype == 'Showroom':
        info = pd.read_excel(file)
        df_showroom = df_showroom.append(info, ignore_index=True)


#df['Dealer'].unique()
#df_showroom['Dealer'].unique()

os.chdir('../..')





import shutil
shutil.rmtree(download_folder)



#df.info()
#df.sample(50)

#Raw Imported Types
#df.dtypes
#df_showroom.dtypes
print("Preparing to create aggregates")
print(df.info)
#Fixing types needed for aggregates
df['Created Date'] = pd.to_datetime(df['Created Date'], errors='coerce')
df['Completed Date'] = pd.to_datetime(df['Completed Date'], errors='coerce')
df['Sold Date'] = pd.to_datetime(df['Sold Date'], errors='coerce')
# Notice that the original imported blanks were in fact '/xa0' values and had to be replaced.
df['Adjusted Response Time (Min)'] = df['Adjusted Response Time (Min)'].replace(u'\xa0', np.NaN)
df.dtypes



# This builds a dataFrame of average repsponse time by dealer by day
df_response = df.groupby(['Dealer',df['Lead Origination Date'].dt.date]).mean()
df_response = df_response[['Adjusted Response Time (Min)']]
df_response.index.set_names(["Dealer", "Date"], inplace=True)
df_response


# This builds a dataFrame of leads by dealer by day
df_leads = df.groupby(['Dealer',df['Lead Origination Date'].dt.date,'Lead Type',]).count().unstack(level=2)
df_leads = df_leads['Lead ID'] #Used to reduce DF
df_leads = df_leads[['Internet','Phone','Walk-in']]
df_leads.columns = ['Internet Leads','Phone Leads','Walk-in Leads']
df_leads.index.set_names(["Dealer", "Date"], inplace=True)
df_leads


# This builds a dataFrame of appointments set by dealer by day
df_appointment_set = df.groupby(['Dealer',df['Created Date'].dt.date,'Lead Type',]).count().unstack(level=2)
df_appointment_set = df_appointment_set['Lead ID'] #Used to reduce DF
df_appointment_set = df_appointment_set[['Internet','Phone','Walk-in']]
df_appointment_set.columns = ['Internet Set','Phone Set','Walk-in Set']
df_appointment_set.index.set_names(["Dealer", "Date"], inplace=True)
df_appointment_set


# This builds a dataFrame of appointments show by dealer by day
df_appointment_show = df.groupby(['Dealer',df['Completed Date'].dt.date,'Lead Type',]).count().unstack(level=2)
df_appointment_show = df_appointment_show['Lead ID'] #Used to reduce DF
df_appointment_show = df_appointment_show[['Internet','Phone','Walk-in']]
df_appointment_show.columns = ['Internet Show','Phone Show','Walk-in Show']
df_appointment_show.index.set_names(["Dealer", "Date"], inplace=True)
df_appointment_show


# This builds a dataFrame of units sold by dealer by day
df_sold = df.groupby(['Dealer',df['Sold Date'].dt.date,'Lead Type',]).count().unstack(level=2)
df_sold = df_sold['Lead ID'] #Used to reduce DF
df_sold = df_sold[['Internet','Phone','Walk-in']]
df_sold.columns = ['Internet Sold','Phone Sold','Walk-in Sold']
df_sold.index.set_names(["Dealer", "Date"], inplace=True)
df_sold

# This builds a dataFrame of showroom visits by dealer by day
# IF statement makes it so that if this info is not available yet it will skip
# instead of crashing the script

df_showroom['Test Drive'] == True
df_showroom['Test Drive'].replace('N',np.NaN, inplace=True)
df_showroom_agg = df_showroom.groupby(['Dealer',df_showroom['Visit Start Date'].dt.date]).count()
df_showroom_agg.index.set_names(["Dealer", "Date"], inplace=True)
df_showroom_agg.columns = ['Showroom Visit','Test Drive']
df_showroom_agg.sort_values('Showroom Visit', ascending=False)



# Merges the aggregated dataFrames above into one filal dataFrame
final = pd.DataFrame().join([df_leads,df_appointment_set,df_appointment_show,df_sold, df_showroom_agg, df_response], how='outer')
final.drop_duplicates(inplace=True)
final.fillna(0, inplace=True)
final['Total Sold'] = final['Internet Sold']+final['Phone Sold']+final['Walk-in Sold']
final


# Copies final dataFrame for Google Data Studio format
gds = final

# Renaming Columns to work with pre-existing Data Studios data in GSheets
gds.index.set_names(["DealerName", "Entry Date"], inplace=True)
gds.columns = ['Internet Leads','Phone Leads','Fresh Walk Ins','Internet Set','Phone Set','Walk Ins Set',              'Internet Show','Phone Show','Walk Ins Show','Internet Sold','Phone Sold','Walk Ins Sold',              'Showroom Visits','Test Drive','Avg. Response Time','Units Sold']
gds.drop(columns=['Test Drive','Walk Ins Set','Walk Ins Show','Walk Ins Sold'], inplace=True)
gds['Closing Percent'] = gds['Units Sold']/gds['Showroom Visits']
gds[['Avg. Response Time','Closing Percent']] = gds[['Avg. Response Time','Closing Percent']].replace(np.inf, "")
gds


#os.getcwd()

# This creates a dictionary of dealers and place holder for their unique GSheet URLs
GsheetLookup = dict.fromkeys(df.Dealer.unique().tolist(), "Not updated")

"""
Key Area to do updates when new clients are onboarded. This needs to be
manually updated for each client. Each GSheet needs to add
pulling-crm-data-to-master-dat@dealer-world-data.iam.gserviceaccount.com
with edit permissions
"""
GsheetLookup['Steven Nissan'] = 'https://docs.google.com/spreadsheets/d/17MJaxHCVI-xq2Gtc-eh0WHWLNcqNfpe9XPxmDt2ywYg'
GsheetLookup['Steven Kia'] = 'https://docs.google.com/spreadsheets/d/1m_qDa76R2_AXGFRS76UcT98puxnWwhUdRiFF_rZGjcA'
GsheetLookup['Gallagher Buick GMC'] = 'https://docs.google.com/spreadsheets/d/1m40fVBOfzqqHJKK6sNhq8WGGxBdz3-5-apRUAfyWEdQ'

dealers = list(GsheetLookup.keys())
print("These are the dealers that are being updated: ",dealers)


##################### Section that Updates the GSheets #########################
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

"""
Note that you need to go into Google Cloud Services and create a service app to
aquire the json creds. Don't forget to 'enable' google sheets and/or google drive.
You will then need to add the email in the json key to the gsheet with edit permissions.
"""

#Location is explicitly set so that CRON job can be run from the root directory
google_api = os.path.join(os.path.dirname(os.path.realpath(__file__)),'PersonalGoogleDriveAPICreds.json')
credentials = ServiceAccountCredentials.from_json_keyfile_name(google_api, scope)

gc = pygsheets.authorize(service_file=google_api)

#dealer = 'Steven Nissan'
for dealer in dealers:
    print(dealer)
    #open the google spreadsheet, [0] selects first sheet
    # NOTE that the worksheet tab HAS to be a an interger,
    # it can not be a string such as "Dealer Leads" make sure that it is
    # the FIRST tab of the worksheek.

    try: #If there is no google sheet in the dictionary it will not break code
        gsheet = gc.open_by_url(GsheetLookup[dealer])[0]

        #Reads in the data currently on the Gsheet, concates that with the new imported data, then removes duplicates
        temp = gsheet.get_as_df()
        temp.drop(columns=['DealerName'], inplace=True)
        try: #Breaks if there is an empty GSheet
            temp['Entry Date'] = pd.to_datetime(temp['Entry Date'], errors='coerce')
        except:
            pass
        temp = pd.concat([gds.loc[dealer].reset_index(),temp], ignore_index=False)
        try:
            temp.insert(0, 'DealerName', dealer)
        except:
            pass
        check2 = temp
        temp['DealerName'] = dealer

        temp = temp.drop_duplicates(subset=['DealerName','Entry Date'], keep='first', ignore_index=True).reset_index(drop=True)
        temp.sort_values('Entry Date', ascending=True, inplace=True)

        gsheet.clear()
        gsheet.set_dataframe(temp, (1,1))
    except:
        pass
