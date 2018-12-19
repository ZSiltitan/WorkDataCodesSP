# -*- coding: utf-8 -*-
"""
Created on Fri Feb 10 17:55:20 2017

@author: evelyn.ng
"""

import sys
import os
import datetime
import calendar
import csv
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext, SparkSession
from pyspark.sql.functions import udf, lit, col, coalesce
import pyspark.sql.functions as F
from pyspark.sql.types import *
from subprocess import call
from datetime import date, timedelta
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

print sys.argv

'''
SETTING CONSTANTS
'''

END_DATE = datetime.datetime.combine(datetime.datetime.now(), datetime.time(0))
END_TIME = int(time.mktime(END_DATE.timetuple()))
print END_DATE, END_TIME
# START_DATE = cur_date.strftime('%Y-%m-%d')

'''
Setting SPARK CONSTANTS
'''

# Change this two
HOME_PATH = '/home/zexi.zeng/SPXReport/'
SPARK_JOB_NAME = 'Cargo Query' + ' | ' + END_DATE.strftime('%Y%m%d')

sc = SparkContext(appName=SPARK_JOB_NAME)
spark = SparkSession \
    .builder \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql('use shopee_id')

# -*- coding: utf-8 -*-

import smtplib
import mimetypes
import datetime
from email.encoders import encode_base64
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import os
import traceback
import email

# import config

def send_shopee_stats(from_addr, to_addrs, mime):
    try:
        mailServer = smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT)
        mailServer.ehlo()
        if config.SMTP_USE_TLS:
            mailServer.starttls()
            mailServer.ehlo()
        if config.SMTP_PASS:
            mailServer.login(config.SMTP_FROM, config.SMTP_PASS)
        mailServer.sendmail(from_addr, to_addrs, mime.as_string())
        mailServer.close()
    except:
        traceback.print_exc()
        return False
    return True

def get_attachment(attachment_file_path):
    content_type, encoding = mimetypes.guess_type(attachment_file_path)
    if content_type is None or encoding is not None:
        content_type = 'application/octet-stream'
    main_type, sub_type = content_type.split('/', 1)
    file = open(attachment_file_path, 'rb')
    if main_type == 'text':
        attachment = MIMEText(file.read())
    elif main_type == 'message':
        attachment = email.message_from_file(file)
    elif main_type == 'image':
        attachment = MIMEImage(file.read(),_sub_type=sub_type)
    elif main_type == 'audio':
        attachment = MIMEAudio(file.read(),_sub_type=sub_type)
    else:
        attachment = MIMEBase(main_type, sub_type)
        attachment.set_payload(file.read())
        encode_base64(attachment)
    file.close()
    attachment.add_header(
        'Content-Disposition', 
        'attachment',     
        filename=os.path.basename(attachment_file_path).encode('utf-8')
    )
    return attachment    

def send_mail(subject, msg, from_addr, to_addrs, cc_addrs=[], bcc_addrs=[], attachments=[], func=send_shopee_stats, *a, **b):

    mime = MIMEMultipart()

    mime['From'] = from_addr
    mime['To'] = ', '.join(to_addrs)
    if len(cc_addrs) > 0:
        mime['Cc'] = ', '.join(cc_addrs)

    mime['Subject'] = subject
    body = MIMEText(msg, _subtype='html', _charset='utf-8')
    mime.attach(body)

    for file_name in attachments:
        mime.attach(get_attachment(file_name))

    _to_addrs = to_addrs + cc_addrs + bcc_addrs
    result = func(from_addr, _to_addrs, mime)

def send_report():
    date_today = datetime.datetime.now()
    #date_today = date_today.strftime('%d-%b')
    #if need to change from_address, change the config.py file
    #if need to change to_address, just change the list in this function
    send_mail(
        "Daily Backlog Report for TW - " + str((datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%d-%b, %Y')) ,
        "Hi all, <br>Please find the attached file of TW Backlog Report (HM, HL, FM, 711, OKMART) for "
        + str((datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%d-%b, %Y'))
        + "<br>Please download the excel file to view the data and keep monitoring if there's any issues."
        + "<br><br>Best Regards,<br>Zexi",
        from_addr='zexi.zeng@shopee.com',
        to_addrs=['ginee.leow@shopee.com','zexi.zeng@shopee.com'],
        # cc_addrs=['alvin.teo@shopee.com','xin.du@shopee.com','ruofan.xu@shopee.com'],
        attachments=[HOME_PATH + FILE_NAME]
    )

# def Report_send(attachments_addr):
#     send_mail(
#         'Stuck_Order_completed_data', 
#         "Attached is the Stuck in Complete order data for " 
#         + str(datetime.datetime.now().replace(microsecond=0)) 
#         + "<br><br>Best Regards,<br>Zexi", 
#         from_addr='zexi.zeng@shopee.com',
#         to_addrs=['ginee.leow@shopee.com'],
#         # to_addrs=['ruofan.xu@shopee.com','zexi.zeng@shopee.com'],
#         cc_addrs=[],
#         attachments=attachments_addr
#     )


def gsheetupdate():
    pass
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credential = ServiceAccountCredentials.from_json_keyfile_name('SpreadSheet-6e7fc9bdf0c9.json',scope)
    gc = gspread.authorize(credential)
    wks = gc.open('SPX MY Analytics Test').worksheet('New Daily Summary')
    ### update former data##
    range_list = ['Y1:Y212','X1:X212','W1:W212','V1:V212','U1:U212','T1:T212','S1:S212','R1:R212','Q1:Q212','P1:P212','O1:O212',
                  'N1:N212','M1:M212','L1:L212'] #dayX to dayX-13

    num, col_index = 0, 13

    while num < col_index:
        values_list = wks.range(range_list[num+1]) #prev day
        update_cell_list = wks.range(range_list[num]) #new day
        
        count = 0
        for cell in update_cell_list:
            #print(cell.value)
            cell.value = values_list[count].value
            #print(cell.value)
            count +=1
            if count < 212:
                continue
            wks.update_cells(update_cell_list)
            
        num +=1

def data_upload()
    raw_data_file = 'SPXMYDailyKPI_'+(datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')+'.csv'
    df = pd.read_csv(raw_data_file)
    df = df.transpose()
    df = df.reset_index()
    dftemp = pd.read_excel('Templatev2.xlsx')
    dftemp.drop(columns = ['L1','L2','L3','L4'],inplace= True)
    dfn = pd.merge(dftemp,df,how='left',on = 'index')
    dfn.fillna(value = ' ',inplace=True)
    listdf = dfn[0].tolist()#column name is 0
    Updatelist = wks.range('L1:L212')
    count = len(listdf)
    for i in range(count):
        Updatelist[i].value = listdf[i]
    wks.update_cells(Updatelist)



###Parsing Ad-hoc Queries ###
def main():
    date_today = datetime.datetime.now()
    date_today = date_today.strftime('%d-%b')
    starting_time = datetime.datetime.now()
    
    query_path = 'SPXMY.txt'
    query = open(query_path, 'r')
    query = query.read()

    # df_path = 'query_result.csv'
    df = spark.sql(query)
    df = df.toPandas()
    df.to_csv('MY/SPXMYDailyKPI_'+str(datetime.datetime.now().strftime('%Y-%m-%d'))+'.csv', encoding='utf-8')
    #write_csv(df, df_path)

    # Report_send(['Stuck_at_Completed_'+str(datetime.datetime.now().strftime('%Y-%m-%d'))+'.csv'])
    print "Start to update the last 13 days records"
    gsheetupdate()
    print "Update done!"
    print "Start to upload the latest data"
    data_upload()
    print "Complete uploading!"
 
    ending_time = datetime.datetime.now()
    print "Process started:", starting_time
    print "Process ended:", ending_time

if __name__ == '__main__':
    main() 



