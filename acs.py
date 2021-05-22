# -*- coding: utf-8 -*-
"""
Created on Thurs Nov 12 2020
@name:   USCensus ACS Download Application
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
APIKEY_FILE = os.path.join(RES_DIR, 'apikeys.txt')
TABLES_FILE = os.path.join(DIR, 'tables.csv')
if not ROOT_DIR in sys.path: sys.path.append(ROOT_DIR)

from utilities.input import InputParser
from utilities.dispatchers import clskey_singledispatcher as keydispatcher
from utilities.dataframes import dataframe_fromfile, dataframe_parser
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



def _rangeParser(x, pattern='-'):
    for item in range([int(i) for i in x.split('-')]): str(item)
def _listParser(x, pattern=';'): 
    for item in x.split(pattern[0]): yield str(item)    
def _dictParser(x, pattern=';=&'):
    for item in x.split(pattern[0]): 
        try: key, value = item.split(pattern[1])
        except ValueError: key, value = item, None
        if not value: yield key, value
        elif pattern[2] in value: yield key, list(value.split(pattern[2]))
        else: yield key, str(value)


TABLES = dataframe_parser(dataframe_fromfile(TABLES_FILE, index='table', header=0), parsers={'scope':_dictParser}, defaultparser=str)
GEOGRAPHYS = {'state':'state', 'county':'county', 'subdivision':'county subdivision', 'tract':'tract', 'block':'block group', 'zipcode':'zip code tabulation area'}
LABELS = {'less than ${}':'<${}',  '${} to ${}':'${}|${}', '${} or more':'>${}', 'no chase rent':'$0', 
          'less than {} percent':'<{}%', '{} to {} percent':'{}%|{}%', '{} percent or more':'>{}%', 'not computed':'N/A',
          'under {} years':'<{}YRS', '{} to {} years':'{}YRS|{}YRS', '{} and {} years':'{}YRS|{}YRS', '{} years and over':'>{}YRS', '{} years':'{}YRS',
          'with a mortgage':'w/Mortgage', 'without a mortgage':'w/oMortgage', 'other race':'other', 'more races':'other', 'hispanic':'w/Hispanic', 'not hispanic':'w/oHispnaic',
          'no schooling':'noschool', 'nursery':'noschool', 'kindergarden':'noschool', 'grade':'noschool', 'high school':'highschool', 'ged':'highschool', 'some college':'highschool',
          'less than {} minutes':'<{}MINS', '{} to {} minutes':'{}MINS|{}MINS', '{} or more minutes':'>{}MINS'}
STATES = {'Alabama':'AL', 'Alaska':'AK', 'Arizona':'AZ', 'Arkansas':'AR', 'California':'CA', 'Colorado':'CO', 'Connecticut':'CT', 'Delaware':'DE', 'Florida':'FL', 'Georgia':'GA', 'Hawaii':'HI',
          'Idaho':'ID', 'Illinois':'IL', 'Indiana':'IN', 'Iowa':'IA', 'Kansas':'KS', 'Kentucky':'KY', 'Louisiana':'LA', 'Maine':'ME', 'Maryland':'MD', 'Massachusetts':'MA', 'Michigan':'MI',
          'Minnesota':'MN', 'Mississippi':'MS', 'Missouri':'MO', 'Montana':'MT', 'Nebraska':'NE', 'Nevada':'NV', 'New Hampshire':'NH', 'New Jersey':'NJ', 'New Mexico':'NM', 'New York':'NY',
          'North Carolina':'NC', 'North Dakota':'ND', 'Ohio':'OH', 'Oklahoma':'OK', 'Oregon':'OR', 'Pennsylvania':'PA', 'Rhode Island':'RI', 'South Carolina':'SC', 'South Dakota':'SD', 'Tennessee':'TN',
          'Texas':'TX', 'Utah':'UT', 'Vermont':'VT', 'Virginia':'VA', 'Washington':'WA', 'West Virginia':'WV', 'Wisconsin':'WI', 'Wyoming':'WY'}


_countyParser = lambda x: ' '.join([str(x), 'County']) if not str(x).endswith(' County') else str(x)
_stateParser = lambda x: str(x) if str(x) in STATES.keys() else STATES[str(x)]

_tag = lambda *args: {'get':'{}'.format(','.join(list(args)))}
_forgeo = lambda key, value: {'for':'{}'.format(':'.join([key, value]))}
_ingeo = lambda **kwargs: {'in':'{}'.format('%20'.join([':'.join([key, value]) for key, value in kwargs.items()]))}
_apikey = lambda apikey: {'key':'{}'.format(str(apikey))}
        
        
class USCensus_ACS_Json(WebJson, xpath=r"//body/pre"): pass
class USCensus_ACS_WebDelayer(WebDelayer): pass
class USCensus_ACS_WebReader(WebReader, retrys=Retrys(retries=3, backoff=0.3, httpcodes=(500, 502, 504)), authenticate=None): pass


class USCensus_ACSVars_WebURL(WebURL, protocol='https', domain='api.census.gov'):
    @keydispatcher
    def path(self, key, *args, date, **kwargs): raise KeyError(key)
    @path.register('geography')
    def path(self, *args, date, **kwargs): return ['data', '{:04.0f}'.format(int(date)), 'acs', 'acs5', 'geography', '.json'] 
    @path.register('variable')    
    def path(self, *args, date, **kwargs): return ['data', '{:04.0f}'.format(int(date)), 'acs', 'acs5', 'variables', '.json'] 
    @path.register('group')
    def path(self, *args, date, group=None, **kwargs):  
        if bool(group): return ['data', '{:04.0f}'.format(int(date)), 'acs', 'acs5', 'groups', group, '.json'] 
        else: return ['data', '{:04.0f}'.format(int(date)), 'acs', 'acs5', 'groups', '.json'] 
   
class USCensus_ACS_WebURL(WebURL, protocol='https', domain='api.census.gov', spaceproxy='%20'): 
    def path(self, *args, date, **kwargs): return ['data', '{:04.0f}'.format(int(date)), 'acs', 'acs5']
    def parms(self, *args, tags, geography, apikey, **kwargs): return {**_tag(*tags), **_forgeo(geography[-1]), **_ingeo(geography[:-1]), **_apikey(apikey)}


contents = {'json':USCensus_ACS_Json}
class USCensus_ACS_WebPage(WebRequestPage, contents=contents):
    def setup(self, *args, **kwargs): self.loadAllContents()
    def execute(self, *args, **kwargs): 
        query = {}
        


class USCensus_ACS_WebCache(WebCache, querys=['table', 'geography', 'date'], datasets=[]): pass
class USCensus_ACS_WebQueue(WebQueue, querys=['table', 'geography', 'date']): 
    def table(self, *args, table, **kwargs): return [str(table)]
    def geography(self, *args, state, county, countys, **kwargs): return ['{}, {}'.format(str(state), str(item)) for item in [county, *countys] if item]
    def date(self, *args, date=None, dates=[], **kwargs): return list(set([str(item) for item in [date, *dates] if item]))


class USCensus_ACS_WebDownloader(WebDownloader, delay=25, attempts=10): 
    def execute(self, *args, queue, delayer, **kwargs):
        with USCensus_ACS_WebReader() as session:
            for feedquery in iter(queue):
                webpage = USCensus_ACS_WebPage(session, delayer=delayer) 
                weburl = USCensus_ACS_WebURL(**feedquery)
                webpage.load(weburl, referer=None)     
                webpage.setup(*args, **kwargs)
                for query, dataset, dataframe in webpage(*args, **kwargs): 
                    yield USCensus_ACS_WebCache(query, {dataset:dataframe}) 


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
    sys.argv += ['table=#hh|geo|inc', 'dates=2015-2019', 'state=CA', 'county=Kern']
    logging.basicConfig(level='INFO', format="[%(levelname)s, %(threadName)s]:  %(message)s")
    inputparser = InputParser(proxys={'assign':'=', 'space':'_'}, parsers={'dates':_rangeParser, 'county':_countyParser}, default=str)   
    inputparser(*sys.argv[1:])
    main(*inputparser.inputArgs, **inputparser.inputParms)  
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    