# -*- coding: utf-8 -*-
"""
Created on Thurs Nov 12 2020
@name:   Realtor Download Propertys Application
@author: Jack Kirby Cook

"""

import sys
import os.path
import time
import warnings
import logging

DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(DIR, os.pardir))
RES_DIR = os.path.join(ROOT_DIR, 'resources')
SAVE_DIR = os.path.join(ROOT_DIR, 'save')
REPOSITORY_DIR = os.path.join(SAVE_DIR, 'uscensus')
QUEUE_FILES = {}
REPORT_FILE = os.path.join(SAVE_DIR, 'uscensus', 'acs.csv')
if not ROOT_DIR in sys.path: sys.path.append(ROOT_DIR)

from parsers import ListParser
from utilities.input import InputParser
from webscraping.webapi import WebURL, WebCache, WebQueue, WebDownloader
from webscraping.webreaders import WebReader, Retrys
from webscraping.webtimers import WebDelayer
from webscraping.webpages import WebRequestPage
from webscraping.webdata import WebJson

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_ACS_WebDelayer', 'USCensus_ACS_WebDownloader', 'USCensus_ACS_WebQueue']
__copyright__ = "Copyright 2020, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


_tag = lambda *args: {'get':'{}'.format(','.join(list(args)))}
_forgeo = lambda key, value: {'for':'{}'.format(':'.join([key, value]))}
_ingeo = lambda **kwargs: {'in':'{}'.format('%20'.join([':'.join([key, value]) for key, value in kwargs.items()]))}
_apikey = lambda apikey: {'key':'{}'.format(str(apikey))}


class USCensus_ACS_Json(WebJson, xpath=r"//body/pre"): pass
class USCensus_ACS_WebDelayer(WebDelayer): pass
class USCensus_ACS_WebReader(WebReader, retrys=Retrys(retries=3, backoff=0.3, httpcodes=(500, 502, 504)), authenticate=None): pass
class USCensus_ACS_WebURL(WebURL, protocol='https', domain='www.api.census.gov', spaceproxy='%20'): 
    def path(self, *args, date, survey, **kwargs): return ['data', '{:04.0f}'.format(int(date)), 'acs', survey]
    def parms(self, *args, tags, geography, apikey, **kwargs): {**_tag(*tags), **_forgeo(geography[-1]), **_ingeo(geography[:-1]), **_apikey(apikey)}


class USCensus_ACS_WebPage(WebRequestPage): 
    def setup(self, *args, **kwargs): pass
    def execute(self, *args, **kwargs): pass
        


class USCensus_ACS_WebCache(WebCache, querys=[], datasets=[]): pass
class USCensus_ACS_WebQueue(WebQueue, querys=[]): pass


class USCensus_ACS_WebDownloader(WebDownloader, delay=25, attempts=10): 
    def execute(self, *args, queue, delayer, **kwargs):
        with USCensus_ACS_WebReader() as session:
            webpage = USCensus_ACS_WebPage(session, delayer=delayer) 
            weburl = USCensus_ACS_WebURL(*args, **kwargs)
            webpage.load(weburl, referer=None)           


def main(*args, **kwargs): 
    webdelayer = USCensus_ACS_WebDelayer('constant', wait=10)
    webqueue = USCensus_ACS_WebQueue(QUEUE_FILES, REPORT_FILE, *args, **kwargs)
    webdownloader = USCensus_ACS_WebDownloader(REPOSITORY_DIR, REPORT_FILE, *args, queue=webqueue, delayer=webdelayer, **kwargs)
    webdownloader(*args, **kwargs)
    while True: 
        if webdownloader.off: break
        if webdownloader.error: break
        time.sleep(15)
    LOGGER.info(str(webdownloader))
    for results in webdownloader.results: print(str(results))
    if not bool(webdownloader): raise webdownloader.error


if __name__ == '__main__':    
    logging.basicConfig(level='INFO', format="[%(levelname)s, %(threadName)s]:  %(message)s")
    listparser = ListParser(pattern=',')
    inputparser = InputParser(assignproxy='=', spaceproxy='_', parsers={'date':int, }, default=str)  
    inputparser(*sys.argv[1:])
    main(*inputparser.inputArgs, **inputparser.inputParms)  
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    