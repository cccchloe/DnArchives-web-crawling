# -*- coding: utf-8 -*-
"""
Created on Sun Aug 12 10:20 AM
@author: qdd

"""

from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import socket
import pickle
import os
import re
from multiprocessing import Pool
import socket
from datetime import datetime,timedelta
import dateutil.parser
import sys
import numpy as np
from time import time, ctime
reload(sys)  
sys.setdefaultencoding("utf-8")

# =============================================================================
##@ agora
##@ change the parameters here
data_dir = 
result_dir = 
Number_Processor=6
websitename = 

# define some basic information
if not os.path.exists(result_dir):
    os.makedirs(result_dir)
dates=os.listdir(data_dir)
dates_parsed = ['2014-06-01', '2014-06-22', '2014-10-07', '2014-10-10', '2014-10-23', '2014-10-31', '2014-12-28', '2015-02-19','2014-02-06-whyusheep-userlist.txt', '2014-06-06-boosie5150-vendorinterface.html','2015-03-27','2015-05-03']
dates = list(set(dates)-set(dates_parsed))
error_log_file = result_dir + websitename+'_log_error.csv'
error_log_table = pd.DataFrame(columns = ['url','e','e_sys','e_sys_position','error_type','error_description'])
index_error = 0


##@ define parsing functions
def readAttr(soup,tag,select=['all']):               # extract the information based on tag and attributes.
    d={}
    index=0
    d[index]=''
    for tag in soup.select(tag):
        for attr,value in tag.attrs.items():
            if select==['all']:
                d[index]=value
                index+=1
            else:
                if attr in select:
                    d[index]=value
                    index+=1
    return d

def readContent(soup,tag,attribute={}):                 # extract data baed on content instead of attributes
    d={}
    index=0
    d[index]=''
    for tag in soup.select(tag):
        d[index]=tag.text
        index+=1
    return d

def read_page(page):
    with open(page) as html:
        content=html.read()
    bs1=bs(content,"lxml")
    return bs1

def parse_price(price,unit):
    return unit+' '+re.search(r'\d+.\d+',price).group()

def parse_price_int(price,unit):
    return unit+' '+re.search(r'\d+',price).group()

def create_table(table_list, folder, encode, output):
    Final=pd.read_csv(folder+table_list[0], encoding=encode)
    for table in table_list[1:]:
        tableX=pd.read_csv(folder+table,encoding=encode)
        Final=Final.append(tableX)
    Final.to_csv(output,index=False)

def parse_category(page):
    category = ''
    category_hierarchy = ''
    count = 0
    tags = page.select('#top-navigation')
    for tag in tags:
        for a_tag in tag.find_all('a'):
            if count == 0:
                category_hierarchy = category_hierarchy + a_tag.text
                category = a_tag.text
            else:
                category_hierarchy = category_hierarchy + '/'+a_tag.text
            count = count + 1
    return category, category_hierarchy

def parse_sold_since(tag):
    return tag.name == 'p' and 'sold since' in tag.text

def parse_positive_last_12months(tag):
    return tag.name == 'b' and 'Positive feedback (last 12 months)' in tag.text

def parse_user_profile(tag):
    return tag.name == 'h1' and 'User Profile' in tag.text

def write_error_log (url, e, e_sys, e_sys_position, error_type, error_description):
    global error_log_table
    global index_error
    error_log_table.loc[index_error,'url']= url
    error_log_table.loc[index_error,'e']= e
    error_log_table.loc[index_error,'e_sys']= e_sys
    error_log_table.loc[index_error,'e_sys_position']= e_sys_position
    error_log_table.loc[index_error,'error_type']= error_type
    error_log_table.loc[index_error,'error_description']= error_description
    index_error = index_error + 1

def parsing_table(date):
    global error_log_table 
    global index_error
    print(date)

    vendor_table=pd.DataFrame(columns=['url', 'scraped_date', 'vendor_name','vendor_id', 'vendor_rating', 'vendor_rating_scale','vendor_transactions_lower', 'vendor_transactions_upper', 'vendor_last_login', 'vendor_last_login_days', 'vendor_description','vendor_pgp','vendor_verification','vendor_parsed_type'])
    indexv=0

    feedback_table=pd.DataFrame(columns=['url', 'scraped_date', 'vendor_name', 'vendor_id', 'item_name', 'item_id', 'feedback_rating', 'feedback_rating_scale', 'feedback_date', 'feedback_date_text','feedback_text', 'feedback_author', 'feedback_type'])
    indexf=0

    product_table=pd.DataFrame(columns=['url', 'scraped_date', 'vendor_name', 'vendor_id', 'item_name', 'item_id', 'price_bitcoin', 'category', 'category_hierarchy','ships_from', 'ships_to', 'item_description','item_parsed_type'])    
    indexp=0
    
    ##@ parse product pages
    print "parse folder p"
    try:
        products=[ap for ap in os.listdir(data_dir+date+'/p')]
        if products!=[]:
            for product in products:
                try:
                    # print(date,product)
                    # print(data_dir+date+'/'+product)
                    # print (os.path.exists(data_dir+date+'/'+product))
                    bsp=read_page(data_dir+date+'/p/'+product)
                    scraped_date = dateutil.parser.parse(date)
                    # scraped_date = datetime.strptime(date, "%Y-%m-%d")
                    url = '/'+date+'/p/'+product
                    
                    ##@ parse item information in product pages
                    if bsp.find_all('div', id = 'single-product')[0]!='':
                        product_table.loc[indexp,'url']= url
                        product_table.loc[indexp,'scraped_date']= scraped_date
                        vendor_id= readAttr(bsp,'a.gen-user-link',['href'])[0][8:-1]
                        vendor_name= readAttr(bsp,'a.gen-user-link',['href'])[0][8:-1]
                        item_name=readContent(bsp,'h1')[0]
                        item_id=product            
                        product_table.loc[indexp,'vendor_name']= vendor_name
                        product_table.loc[indexp,'vendor_id']= vendor_id
                        product_table.loc[indexp,'item_name']= item_name
                        product_table.loc[indexp,'item_id']= item_id

                        # price
                        price_str = readContent(bsp,'div.product-page-price')[0]
                        if price_str!='':
                            try:
                                price_bitcoin=float(re.search(r'\d+.\d+',price_str).group())
                                product_table.loc[indexp,'price_bitcoin']= price_bitcoin
                            except Exception as e:
                                product_table.loc[indexp,'price_bitcoin']= np.nan
                                write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_price')         
                        
                        # category
                        try:
                            category = parse_category(bsp)[0]
                            category_hierarchy = parse_category(bsp)[1]                       
                            product_table.loc[indexp,'category']= category
                            product_table.loc[indexp,'category_hierarchy']= category_hierarchy
                        except Exception as e:
                            product_table.loc[indexp,'category']= np.nan
                            product_table.loc[indexp,'category_hierarchy']= np.nan
                            write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_category')

                        # ships
                        ships_str = bsp.find_all('div', {'class':"product-page-ships"})[0]
                        ships_from = np.nan
                        ships_to = np.nan
                        if ships_str != '':
                            try:
                                ships_from = re.sub(r'Ships from\s+','',re.search(r'\n\s+Ships from\s+.*\n',ships_str.text).group().strip())
                            except Exception as e:
                                ships_from = np.nan
                                write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_ships_from')

                            try:
                                ships_to = re.sub(r'Ships to\s+','',re.search(r'\n\s+Ships to\s+.*\n',ships_str.text).group().strip())
                            except Exception as e:
                                ships_to = np.nan
                                write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_ships_to')


                        product_table.loc[indexp,'ships_from']= ships_from
                        product_table.loc[indexp,'ships_to']= ships_to
                        
                        # description
                        description_str = bsp.find_all('div', id = "single-product")[0].text
                        try:
                            item_description = description_str[:description_str.find('Brought to you by')]
                            product_table.loc[indexp,'item_description']= item_description
                        except Exception as e:
                            product_table.loc[indexp,'item_description']= np.nan
                            write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_item_description')

                        product_table.loc[indexp,'item_parsed_type']= 'item_item'
                        indexp+=1            

                        ##@ parse vendor information in product pages
                        vendor_table.loc[indexv,'vendor_id']=vendor_id
                        vendor_table.loc[indexv,'vendor_name']=vendor_name
                        vendor_table.loc[indexv,'url']=url
                        vendor_table.loc[indexv,'scraped_date']=scraped_date

                        vendor_rating_str = bsp.find('span', {'class':'gen-user-ratings'}).text

                        try:
                            vendor_rating = re.search(r'\d[.\d]*/5', vendor_rating_str).group()
                            vendor_table.loc[indexv,'vendor_rating']= vendor_rating.replace('/5', '')
                            vendor_table.loc[indexv,'vendor_rating_scale']= 5
                        except Exception as e:
                            vendor_table.loc[indexv,'vendor_rating']= np.nan
                            vendor_table.loc[indexv,'vendor_rating_scale']= 5
                            write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_vendor_rating_in_p')

                        try:
                            vendor_transactions_str = re.search(r'(\d+)[~]*(\d+)\sdeals', vendor_rating_str).group()
                            if '~' in vendor_transactions_str:
                                vendor_table.loc[indexv,'vendor_transactions_lower']= int(re.search(r'(\d+)[~]*(\d+)\sdeals', vendor_rating_str).group(1))
                                vendor_table.loc[indexv,'vendor_transactions_upper']= int(re.search(r'(\d+)[~]*(\d+)\sdeals', vendor_rating_str).group(2))
                            elif '0 deals' in vendor_transactions_str:
                                vendor_table.loc[indexv,'vendor_transactions_lower']= int(re.search(r'(\d+)[~]*(\d+)\sdeals', vendor_rating_str).group(1))
                                vendor_table.loc[indexv,'vendor_transactions_upper']= int(re.search(r'(\d+)[~]*(\d+)\sdeals', vendor_rating_str).group(2))
                        except Exception as e:
                            vendor_table.loc[indexv,'vendor_transactions_lower']= np.nan
                            vendor_table.loc[indexv,'vendor_transactions_upper']= np.nan
                            write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_vendor_transactions_in_p')
                        vendor_table.loc[indexv,'vendor_parsed_type']= 'item_vendor'
                        indexv+=1

                        ##@ parse feedback information in product pages
                        fb_part=bsp.select('div.embedded-feedback-list')[0]
                        if 'No feedbacks found' in fb_part.text:
                            pass
                        else:
                            tb=fb_part.findAll('table')[0]
                            rows=tb.findAll('tr')
                            for row in rows:
                                feedback_table.loc[indexf,'url']=url
                                feedback_table.loc[indexf,'scraped_date']=scraped_date
                                feedback_table.loc[indexf,'vendor_name']=vendor_name
                                feedback_table.loc[indexf,'vendor_id']=vendor_id
                                feedback_table.loc[indexf,'item_name']=item_name
                                feedback_table.loc[indexf,'item_id']=item_id                                                        
                                terms=row.findAll('td')
                                
                                try:
                                    feedback_rating = re.search(r'\d[.\d]*/5', terms[0].text).group()
                                    feedback_table.loc[indexf,'feedback_rating']= float(feedback_rating.replace('/5', ''))
                                    feedback_table.loc[indexf,'feedback_rating_scale']= 5
                                except Exception as e:
                                    feedback_table.loc[indexf,'feedback_rating']= np.nan
                                    feedback_table.loc[indexf,'feedback_rating_scale']= 5
                                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_feedback_rating')

                                try:
                                    feedback_text=terms[1].text
                                    feedback_table.loc[indexf,'feedback_text']=feedback_text
                                except Exception as e:
                                    feedback_table.loc[indexf,'feedback_text']= np.nan                           
                                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_feedback_text')
                                try:
                                    feedback_date_text=terms[2].text
                                    feedback_table.loc[indexf,'feedback_date_text']=feedback_date_text
                                    feedback_date_distance = int(re.search(r'\d+', feedback_date_text).group())
                                    if 'days' in feedback_date_text:
                                        feedback_date = scraped_date - timedelta(days = feedback_date_distance)
                                    elif 'months' in feedback_date_text:
                                        feedback_date = scraped_date - timedelta(days = feedback_date_distance*30)
                                    feedback_table.loc[indexf,'feedback_date']=feedback_date
                                except Exception as e:
                                    feedback_table.loc[indexf,'feedback_date']= np.nan                          
                                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_feedback_date')                                

                                try:
                                    feedback_author=terms[3].text
                                    feedback_table.loc[indexf,'feedback_author']=feedback_author
                                except Exception as e:
                                    feedback_table.loc[indexf,'feedback_author']= np.nan                           
                                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_feedback_author')

                                feedback_table.loc[indexf,'feedback_type']='item_feedback'
            
                                indexf+=1

                except Exception as e:
                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_one_item')                    

    except Exception as e:
        write_error_log (data_dir+date+'/p/',e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'all_items', 'parse_all_items')
    
    ##@ parse vendor pages
    print "parse folder vendor"
    try:
        vendors=[ap for ap in os.listdir(data_dir+date+'/vendor')]
        if vendors!=[]:
            for one_vendor in vendors:
                try:
                    bsv=read_page(data_dir+date+'/vendor/'+one_vendor)
                    scraped_date = dateutil.parser.parse(date)
                    # scraped_date = datetime.strptime(date, "%Y-%m-%d")
                    url = '/'+date+'/vendor/'+one_vendor
                    
                    ##@ parse vendor information in vendor pages                                      
                    if bsv.find_all('div', id = 'middlestuff')[0]!='':                        
                        vendor_id= one_vendor
                        vendor_name= bsv.find('i', {'class':'fa fa-user'}).find_next_sibling("strong").text
                        vendor_table.loc[indexv,'vendor_id']=vendor_id
                        vendor_table.loc[indexv,'vendor_name']=vendor_name
                        vendor_table.loc[indexv,'url']=url
                        vendor_table.loc[indexv,'scraped_date']=scraped_date

                        vendor_rating_str = bsv.find_all('span', {'class':'gen-user-ratings'})[0].text

                        try:
                            vendor_rating = re.search(r'\d[.\d]*/5', vendor_rating_str).group()
                            vendor_table.loc[indexv,'vendor_rating']= vendor_rating.replace('/5', '')
                            vendor_table.loc[indexv,'vendor_rating_scale']= 5
                        except Exception as e:
                            vendor_table.loc[indexv,'vendor_rating']= np.nan
                            vendor_table.loc[indexv,'vendor_rating_scale']= 5
                            write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_vendor_rating')

                        try:
                            vendor_transactions_str = re.search(r'(\d+)[~]*(\d+)\sdeals', vendor_rating_str).group()
                            if '~' in vendor_transactions_str:
                                vendor_table.loc[indexv,'vendor_transactions_lower']= int(re.search(r'(\d+)[~]*(\d+)\sdeals', vendor_rating_str).group(1))
                                vendor_table.loc[indexv,'vendor_transactions_upper']= int(re.search(r'(\d+)[~]*(\d+)\sdeals', vendor_rating_str).group(2))
                            elif '0 deals' in vendor_transactions_str:
                                vendor_table.loc[indexv,'vendor_transactions_lower']= int(re.search(r'(\d+)[~]*(\d+)\sdeals', vendor_rating_str).group(1))
                                vendor_table.loc[indexv,'vendor_transactions_upper']= int(re.search(r'(\d+)[~]*(\d+)\sdeals', vendor_rating_str).group(2))
                        except Exception as e:
                            vendor_table.loc[indexv,'vendor_transactions_lower']= np.nan
                            vendor_table.loc[indexv,'vendor_transactions_upper']= np.nan
                            write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_vendor_transactions')

                        try:
                            vendor_last_login_str = bsv.find_all('div', {'class':'vendorbio-stats-online'})[0].text
                            vendor_table.loc[indexv,'vendor_last_login']= vendor_last_login_str
                        except Exception as e:
                            vendor_table.loc[indexv,'vendor_last_login']= np.nan
                            write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_vendor_last_login')

                        try:
                            vendor_description = bsv.find_all('div', {'class':'vendorbio-description'})[0].text
                            vendor_table.loc[indexv,'vendor_description']= vendor_description
                        except Exception as e:
                            vendor_table.loc[indexv,'vendor_description']= np.nan
                            write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'vendor_description')

                        try:
                            vendor_pgp = bsv.find_all('span', {'class':'pgptoken'})[0].text
                            if 'vendor_pgp' != '':
                                vendor_table.loc[indexv,'vendor_pgp']= 1
                            else:
                                vendor_table.loc[indexv,'vendor_pgp']= 0
                        except Exception as e:
                            vendor_table.loc[indexv,'vendor_pgp']= 0
                            write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'vendor_pgp')

                        try:
                            vendor_verification = bsv.find_all('div', {'class':'vendor-verification'})[0].text
                            if 'Verified' in vendor_verification:
                                vendor_table.loc[indexv,'vendor_verification']= 1
                            else:
                                vendor_table.loc[indexv,'vendor_verification']= 0
                        except Exception as e:
                            vendor_table.loc[indexv,'vendor_verification']= 0
                            write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'vendor_verification')

                        indexv+=1                           

                        ##@ parse feedback information in vendor pages
                        fb_part=bsv.select('div.embedded-feedback-list')[0]
                        if 'No feedbacks found' in fb_part.text:
                            pass
                        else:
                            tb=fb_part.findAll('table')[0]
                            rows=tb.findAll('tr')
                            for row in rows:
                                feedback_table.loc[indexf,'url']=url
                                feedback_table.loc[indexf,'scraped_date']=scraped_date
                                feedback_table.loc[indexf,'vendor_name']=vendor_name
                                feedback_table.loc[indexf,'vendor_id']=vendor_id                                                        
                                terms=row.findAll('td')
                                
                                try:
                                    feedback_rating = re.search(r'\d[.\d]*/5', terms[0].text).group()
                                    feedback_table.loc[indexf,'feedback_rating']= float(feedback_rating.replace('/5', ''))
                                    feedback_table.loc[indexf,'feedback_rating_scale']= 5
                                except Exception as e:
                                    feedback_table.loc[indexf,'feedback_rating']= np.nan
                                    feedback_table.loc[indexf,'feedback_rating_scale']= 5
                                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_feedback_rating')

                                try:
                                    feedback_text=terms[1].text
                                    feedback_table.loc[indexf,'feedback_text']=feedback_text
                                except Exception as e:
                                    feedback_table.loc[indexf,'feedback_text']= np.nan                           
                                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_feedback_text')

                                try:
                                    feedback_item=terms[2].text
                                    feedback_table.loc[indexf,'item_name']=feedback_item
                                    feedback_table.loc[indexf,'item_id']=terms[2].find('a')['href'].replace('/p/','')
                                except Exception as e:
                                    feedback_table.loc[indexf,'item_name']= np.nan
                                    feedback_table.loc[indexf,'item_id']=np.nan                        
                                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'feedback_item')

                                try:
                                    feedback_date_text=terms[3].text
                                    feedback_table.loc[indexf,'feedback_date_text']=feedback_date_text
                                    feedback_date_distance = int(re.search(r'\d+', feedback_date_text).group())
                                    if 'days' in feedback_date_text:
                                        feedback_date = scraped_date - timedelta(days = feedback_date_distance)
                                    elif 'months' in feedback_date_text:
                                        feedback_date = scraped_date - timedelta(days = feedback_date_distance*30)
                                    feedback_table.loc[indexf,'feedback_date']=feedback_date
                                except Exception as e:
                                    feedback_table.loc[indexf,'feedback_date']= np.nan                          
                                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_feedback_date')

                                try:
                                    feedback_author=terms[4].text
                                    feedback_table.loc[indexf,'feedback_author']=feedback_author
                                except Exception as e:
                                    feedback_table.loc[indexf,'feedback_author']= np.nan                           
                                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'parse_feedback_author')

                                feedback_table.loc[indexf,'feedback_type']='vendor_feedback'

                                indexf+=1

                        ##@ parse item information in vendor pages
                        item_part=bsv.find_all('div',id = 'product-list')[0]
                        if item_part.find('table') == None:
                            pass
                        else:
                            tb=item_part.findAll('table')[0]
                            rows=tb.findAll('tr')
                            for row in rows[1:]:
                                product_table.loc[indexp,'url']=url
                                product_table.loc[indexp,'scraped_date']=scraped_date
                                product_table.loc[indexp,'vendor_name']=vendor_name
                                product_table.loc[indexp,'vendor_id']=vendor_id                                                        
                                terms=row.findAll('td')
                                
                                try:
                                    vendor_item_name=terms[1].find('a').text
                                    product_table.loc[indexp,'item_name']=vendor_item_name
                                    vendor_item_id=terms[1].find('a')['href'].replace('/p/','')
                                    product_table.loc[indexp,'item_id']=vendor_item_id
                                    vendor_item_description=terms[1].find('span').text
                                    product_table.loc[indexp,'item_description']=vendor_item_description

                                except Exception as e:
                                    product_table.loc[indexp,'item_name']= np.nan
                                    product_table.loc[indexp,'item_id']=np.nan  
                                    product_table.loc[indexp,'item_description']=np.nan                      
                                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'vendor_item_description')

                                try:
                                    vendor_item_price=float(terms[2].text.strip().replace(' BTC',''))
                                    product_table.loc[indexp,'price_bitcoin']=vendor_item_price
                                except Exception as e:
                                    product_table.loc[indexp,'price_bitcoin']= np.nan                          
                                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'vendor_item_price_bitcoin')

                                try:
                                    vendor_item_ships=terms[3].text.strip().split('\n')
                                    ships_from = vendor_item_ships[0].strip()
                                    ships_to = vendor_item_ships[1].strip()
                                    product_table.loc[indexp,'ships_from']=ships_from
                                    product_table.loc[indexp,'ships_to']=ships_to
                                except Exception as e:
                                    product_table.loc[indexp,'ships_from']= np.nan
                                    product_table.loc[indexp,'ships_to']= np.nan                           
                                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_item', 'vendor_item_ships')

                                product_table.loc[indexp,'item_parsed_type']='vendor_item'

                                indexp+=1                                  

                except Exception as e:
                    write_error_log (url,e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'one_vendor', 'parse_one_vendor')                    

    except Exception as e:
        write_error_log (data_dir+date+'/vendor/',e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'all_vendors', 'parse_all_vendors')


    try:
        product_table.to_csv(result_dir+date+'_item.csv',index=False, encoding = 'utf-8') 
    except Exception as e:
        write_error_log (result_dir+date+'_writing_item',e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'writing_item', 'writing_item')

    try:
        feedback_table.to_csv(result_dir+date+'_feedback.csv',index=False, encoding = 'utf-8')
    except Exception as e:
        write_error_log (result_dir+date+'_writing_feedback',e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'writing_feedback', 'writing_feedback')

    try:
        vendor_table.to_csv(result_dir+date+'_vendor.csv',index=False, encoding = 'utf-8')
    except Exception as e:
        write_error_log (result_dir+date+'_writing_vendor',e, sys.exc_info()[0], sys.exc_info()[2].tb_lineno,'writing_vendor', 'writing_vendor')
   
    error_log_table.to_csv(error_log_file, index = False, encoding = 'utf-8')

start_ctime = ctime()
start_time = time()
print ("start at , ", start_ctime)
if __name__=='__main__':
    try:
        p=Pool(Number_Processor)
        p.map(parsing_table,dates)
        p.close()
        p.join()
    except Exception as e:
        print (e)
end_time = time()
end_ctime = ctime()
print ("end at , ", end_ctime)
print ("running %s seconds!" % (end_time - start_time))


##@ merge all tables
print ("======================================")
print ("now begin to merge all tables: ")

feedback_tables=[fb for fb in os.listdir(result_dir) if fb[-12:]=='feedback.csv']
fb1=pd.read_csv(result_dir+feedback_tables[0],lineterminator='\n')
for table in feedback_tables[1:]:
    fbX=pd.read_csv(result_dir+table,lineterminator='\n')
    fb1=fb1.append(fbX)
fb1.index=range(len(fb1))
fb1.drop_duplicates(subset=['vendor_id', 'vendor_name', 'item_id', 'item_name', 'feedback_text', 'feedback_date'], inplace =True)
fb1.to_csv(result_dir+websitename+'_feedback_merge.csv',index=False)

vendor_tables=[vd for vd in os.listdir(result_dir) if vd[-10:]=='vendor.csv']
vd1=pd.read_csv(result_dir+vendor_tables[0],lineterminator='\n')
for table in vendor_tables[1:]:
    vdX=pd.read_csv(result_dir+table,lineterminator='\n')
    vd1=vd1.append(vdX)
vd1.index=range(len(vd1))
vd1.drop_duplicates(subset=['vendor_id', 'vendor_name'], inplace =True)
vd1.to_csv(result_dir+websitename+'_vendor_merge.csv',index=False)

item_tables=[vd for vd in os.listdir(result_dir) if vd[-8:]=='item.csv']
it1=pd.read_csv(result_dir+item_tables[0],lineterminator='\n')
for table in item_tables[1:]:
    itX=pd.read_csv(result_dir+table,lineterminator='\n')
    it1=it1.append(itX)
it1.index=range(len(it1))
it1.drop_duplicates(subset=['vendor_id', 'vendor_name', 'item_id', 'item_name'], inplace =True)
it1.to_csv(result_dir+websitename+'_item_merge.csv',index=False)

print ("done!")
print ("======================================")
print ("summary:")
print ("start at , ", start_ctime)
print ("end at , ", end_ctime)
print ("running %s seconds!" % (end_time - start_time))
print ("time now: ", ctime())
# =============================================================================
