# -*- coding: utf-8 -*-
"""
Created on Thurs 5 27 2021
@name:   USCensus Migration Download Application
@author: Jack Kirby Cook

"""

import sys
import os.path
import time
import warnings
import logging
import pandas as pd

DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(DIR, os.pardir))
RES_DIR = os.path.join(ROOT_DIR, 'resources')
SAVE_DIR = os.path.join(ROOT_DIR, 'save')
REPOSITORY_DIR = os.path.join(SAVE_DIR, 'uscensus')
REPORT_FILE = os.path.join(SAVE_DIR, 'uscensus', 'acs.csv')
APIKEYS_FILE = os.path.join(RES_DIR, 'apikeys.txt')
TABLES_FILE = os.path.join(DIR, 'tables.csv')
if not ROOT_DIR in sys.path: sys.path.append(ROOT_DIR)

from utilities.input import InputParser
from webscraping.webapi import WebURL, WebCache, WebQueue, WebDownloader
from webscraping.webreaders import WebReader, Retrys
from webscraping.webtimers import WebDelayer
from webscraping.webpages import WebJsonPage
from webscraping.webdata import WebJson
from webscraping.webvariables import Geography

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_MIT_WebDelayer', 'USCensus_MIT_WebDownloader', 'USCensus_MIT_WebQueue']
__copyright__ = "Copyright 2020, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")    


with open(APIKEYS_FILE) as file: APIKEYS = {line.split(',')[0]:line.split(',')[1] for line in file.readlines()}

GEOGRAPHYS = {'state':'state', 'county':'county', 'subdivision':'county subdivision'}
TAGS = ['MOVEDNET', 'MOVEDIN', 'FULL2_NAME', 'STATE2', 'COUNTY2', 'MOVEDOUT', 'FULL1_NAME']
STATES = {
    'Alabama':'AL', 'Alaska':'AK', 'Arizona':'AZ', 'Arkansas':'AR', 'California':'CA', 'Colorado':'CO', 'Connecticut':'CT', 'Delaware':'DE', 'Florida':'FL', 'Georgia':'GA', 'Hawaii':'HI',
    'Idaho':'ID', 'Illinois':'IL', 'Indiana':'IN', 'Iowa':'IA', 'Kansas':'KS', 'Kentucky':'KY', 'Louisiana':'LA', 'Maine':'ME', 'Maryland':'MD', 'Massachusetts':'MA', 'Michigan':'MI',
    'Minnesota':'MN', 'Mississippi':'MS', 'Missouri':'MO', 'Montana':'MT', 'Nebraska':'NE', 'Nevada':'NV', 'New Hampshire':'NH', 'New Jersey':'NJ', 'New Mexico':'NM', 'New York':'NY',
    'North Carolina':'NC', 'North Dakota':'ND', 'Ohio':'OH', 'Oklahoma':'OK', 'Oregon':'OR', 'Pennsylvania':'PA', 'Rhode Island':'RI', 'South Carolina':'SC', 'South Dakota':'SD', 'Tennessee':'TN',
    'Texas':'TX', 'Utah':'UT', 'Vermont':'VT', 'Virginia':'VA', 'Washington':'WA', 'West Virginia':'WV', 'Wisconsin':'WI', 'Wyoming':'WY'}


_inverted = lambda x: {v:k for k, v in x.items()}
_range = lambda x: [str(i) for i in range(int(x.split('-')[0]), int(x.split('-')[1])+1)]
_list = lambda x: [str(i) for i in str(x).split('|')]
_state = lambda x: str(x) if str(x) in STATES.values() else STATES[str(x)]
_statename = lambda x: str(x) if str(x) in STATES.keys() else _inverted(GEOGRAPHYS)[str(x)]
_county = lambda x: ' '.join([str(x), 'County']) if not str(x).endswith(' County') else str(x)
_tag = lambda *args: {'get':'{}'.format(','.join(list(args)))}
_forgeo = lambda **kwargs: {'for':'{}'.format('%20'.join([':'.join([key, value]) for key, value in kwargs.items()]))} if kwargs else {}
_ingeo = lambda **kwargs: {'in':'{}'.format('%20'.join([':'.join([key, value]) for key, value in kwargs.items()]))} if kwargs else {}
_apikey = lambda apikey: {'key':'{}'.format(str(apikey))}
    

def _vardata(json): return pd.DataFrame(data=json[1:], columns=json[0])    
def _geodata(json):
    keys = tuple(json[0][1:])
    namesdata = [tuple(str(i[0]).split(', ')[::-1]) for i in json[1:]]
    valuesdata = [tuple(i[1:]) for i in json[1:]]
    return {(keys[-1], names[-1]):values[-1] for names, values in zip(namesdata, valuesdata)}


class USCensus_MIT_WebGeoData(WebJson.customize(dataparser=_geodata)): pass
class USCensus_MIT_WebVarData(WebJson.customize(dataparser=_vardata)): pass

class USCensus_MIT_WebDelayer(WebDelayer): pass
class USCensus_MIT_WebReader(WebReader, retrys=Retrys(retries=3, backoff=0.3, httpcodes=(500, 502, 504)), authenticate=None): pass


class USCensus_ACSGeography_WebURL(WebURL, protocol='https', domain='api.census.gov', path=['data', '{date}', 'acs', 'acs5', 'geography', '.json']): pass
class USCensus_ACS_WebURL(WebURL, protocol='https', domain='api.census.gov', spaceproxy='%20'): 
    def path(self, *args, date, **kwargs): return ['data', str(date), 'acs', 'acs5']
    def parms(self, *args, tags, geography, apikey, **kwargs): 
        assert isinstance(geography, Geography)
        forgeo = {GEOGRAPHYS[key]:(value if value is not None else '*') for key, name, value in geography[-1].items()} 
        ingeo = {GEOGRAPHYS[key]:(value if value is not None else '*') for key, name, value in geography[:-1].items()}
        return {**_tag(*tags), **_forgeo(**forgeo), **_ingeo(**ingeo), **_apikey(apikey)}


contents = {'geodata':USCensus_MIT_WebGeoData, 
            'vardata':USCensus_MIT_WebVarData}
class USCensus_MIT_WebPage(WebJsonPage, contents=contents):
    def setup(self, *args, **kwargs):
        self.loadAllContents()
        return self
    
    def execute(self, *args, date, **kwargs): 
        query = {}
        dataset = ''.format()
        dataframe = self['data'].data
        dataframe['date'] = date
        yield query, dataset, dataframe


class USCensus_MIT_WebCache(WebCache, querys=[], dataset=None): pass
class USCensus_MIT_WebQueue(WebQueue, querys=['date', 'state', 'county']): 
    def date(self, *args, date=None, dates=[], **kwargs): return [str(item) for item in [date, *dates] if item]
    def state(self, *args, state, **kwargs): return [_state(state)]
    def county(self, *args, county=None, countys=[], **kwargs): [_county(item) for item in [county, *countys] if item]


class USCensus_MIT_WebDownloader(WebDownloader, delay=25, attempts=10): 
    def execute(self, *args, queue, delayer, **kwargs):
        with USCensus_MIT_WebReader() as session:
            webpage = USCensus_MIT_WebPage(session, delayer=delayer) 
            for feedquery in iter(queue):
                geography = self.geography(webpage, **feedquery)
                geography[list(GEOGRAPHYS.keys())[-1]] = {'name':None, 'value': None}
                url = USCensus_ACS_WebURL(tags=TAGS, geography=geography, date=feedquery['date'], apikey=APIKEYS['uscensus'])
                webpage.load(url, referer=None).setup()
                for query, dataset, dataframe in webpage(**feedquery): 
                    yield USCensus_MIT_WebCache(query, {dataset:dataframe})

    def geography(self, webpage, *args, date, **kwargs):
        geography = Geography(keys=list(GEOGRAPHYS.keys())[:-1])
        for index, key in enumerate(list(GEOGRAPHYS.keys())[:-1], start=1):
            url = USCensus_ACS_WebURL(tags=['NAME'], geography=geography[0:index], date=date, apikey=APIKEYS['uscensus'])   
            geovalues = webpage.load(url, referer=None).setup()['geodata'].data 
            geography[key] = dict(name=kwargs[key], value=geovalues[(key, kwargs[key])])            
        return geography

                               
def main(*args, **kwargs): 
    webdelayer = USCensus_MIT_WebDelayer('constant', wait=15)
    webqueue = USCensus_MIT_WebQueue(REPORT_FILE, *args, **kwargs)
    webdownloader = USCensus_MIT_WebDownloader(REPOSITORY_DIR, REPORT_FILE, *args, queue=webqueue, delayer=webdelayer, **kwargs)
    webdownloader(*args, **kwargs)
    while True: 
        if webdownloader.off: break
        if webdownloader.error: break
        time.sleep(15)
    LOGGER.info(str(webdownloader))
    for results in webdownloader.results: print(str(results))
    if not bool(webdownloader): raise webdownloader.error


if __name__ == '__main__':    
    sys.argv += ['dates=2015-2019', 'state=CA', 'county=Kern']
    logging.basicConfig(level='INFO', format="[%(levelname)s, %(threadName)s]:  %(message)s")
    inputparser = InputParser(proxys={'assign':'=', 'space':'_'}, parsers={'dates':_range, 'countys':_list}, default=str)   
    inputparser(*sys.argv[1:])
    main(*inputparser.inputArgs, **inputparser.inputParms)  
    
    
    
    
    
    