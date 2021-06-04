# -*- coding: utf-8 -*-
"""
Created on Thurs 5 27 2021
@name:   USCensus ACS Migration Download Application
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
__all__ = ['USCensus_ACS_WebDelayer', 'USCensus_ACS_WebDownloader', 'USCensus_ACS_WebQueue']
__copyright__ = "Copyright 2020, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")    


with open(APIKEYS_FILE) as file: APIKEYS = {line.split(',')[0]:line.split(',')[1] for line in file.readlines()}

GEOGRAPHYS = {'state':'state', 'county':'county', 'subdivision':'county subdivision'}
TAGS = {'growth':'MOVEDNET', 'entering':'MOVEDIN', 'exiting':'MOVEDOUT', 'targetname':'FULL2_NAME', 'targetstate':'STATE2', 'targetcounty':'COUNTY2', 'name':'FULL1_NAME'}
STATES = {
    'AL':'Alabama', 'AK':'Alaska','AZ':'Arizona', 'AR':'Arkansas', 'CA':'California', 'CO':'Colorado', 'CT':'Connecticut', 'DE':'Delaware', 'FL':'Florida', 'GA':'Georgia', 
    'HI':'Hawaii', 'ID':'Idaho', 'IL':'Illinois', 'IN':'Indiana', 'IA':'Iowa', 'KS':'Kansas', 'KY':'Kentucky', 'LA':'Louisiana', 'ME':'Maine', 'MD':'Maryland', 'MA':'Massachusetts', 
    'MI':'Michigan', 'MN':'Minnesota', 'MS':'Mississippi', 'MO':'Missouri', 'MT':'Montana', 'NE':'Nebraska', 'NV':'Nevada', 'NH':'New Hampshire', 'NJ':'New Jersey', 'NM':'New Mexico', 
    'NY':'New York', 'NC':'North Carolina', 'ND':'North Dakota', 'OH':'Ohio', 'OK':'Oklahoma', 'OR':'Oregon', 'PA':'Pennsylvania', 'RI':'Rhode Island', 'SC':'South Carolina', 'SD':'South Dakota', 
    'TN':'Tennessee', 'TX':'Texas', 'UT':'Utah', 'VT':'Vermont', 'VA':'Virginia', 'WA':'Washington', 'WV':'West Virginia', 'WI':'Wisconsin', 'WY':'Wyoming'}


_inverted = lambda x: {v:k for k, v in x.items()}
_filter = lambda x: [i for i in x if i is not None]
_range = lambda x: [str(i) for i in range(int(x.split('-')[0]), int(x.split('-')[1])+1)]
_list = lambda x: [str(i) for i in str(x).split('|')]
_state = lambda x: str(x) if str(x) in STATES.values() else STATES[str(x)]
_county = lambda x: ' '.join([str(x), 'County']) if not str(x).endswith(' County') else str(x)
_tag = lambda *args: {'get':'{}'.format(','.join(list(args)))}
_forgeo = lambda **kwargs: {'for':'{}'.format('%20'.join([':'.join([key, value]) for key, value in kwargs.items()]))} if kwargs else {}
_ingeo = lambda **kwargs: {'in':'{}'.format('%20'.join([':'.join([key, value]) for key, value in kwargs.items()]))} if kwargs else {}
_apikey = lambda apikey: {'key':'{}'.format(str(apikey))}
    

def _vardata(json): return pd.DataFrame(data=json[1:], columns=json[0])    
def _geodata(json): return {x[0].split(', ')[0]:x[-1] for x in json[1:]}

class USCensus_WebGeoData(WebJson.customize(dataparser=_geodata)): pass
class USCensus_WebVarData(WebJson.customize(dataparser=_vardata)): pass

class USCensus_ACS_WebDelayer(WebDelayer): pass
class USCensus_ACS_WebReader(WebReader, retrys=Retrys(retries=3, backoff=0.3, httpcodes=(500, 502, 504)), authenticate=None): pass


class USCensus_ACS_WebURL(WebURL, protocol='https', domain='api.census.gov', spaceproxy='%20'): 
    def path(self, *args, dataset, date, **kwargs): return ['data', str(date), 'acs', str(dataset)]
    def parms(self, *args, tags, geography, apikey, **kwargs): 
        assert isinstance(geography, Geography)
        forgeo = {GEOGRAPHYS[key].replace(" ", self.spaceproxy):(value.replace(" ", self.spaceproxy) if value is not None else '*') for key, name, value in geography[-1].items()} 
        ingeo = {GEOGRAPHYS[key].replace(" ", self.spaceproxy):(value.replace(" ", self.spaceproxy) if value is not None else '*') for key, name, value in geography[:-1].items()}
        return {**_tag(*tags), **_forgeo(**forgeo), **_ingeo(**ingeo), **_apikey(apikey)}


contents = {'geodata':USCensus_WebGeoData, 
            'vardata':USCensus_WebVarData}
class USCensus_ACS_WebPage(WebJsonPage, contents=contents):
    def setup(self, *args, **kwargs):
        self.loadAllContents()
        return self
    
    def execute(self, *args, date, state, county, **kwargs): 
        query = {'date':date, 'state':state, 'county':county}
        dataset = '{}_{}_{}'.format('household', 'geography', 'mitgration')
        dataframe = self['data'].data
        dataframe = self.geography(*args, **kwargs)
        dataframe = self.variables(*args, **kwargs)
        dataframe['date'] = date
        yield query, dataset, dataframe

    def geography(self, dataframe, *args, **kwargs):
        dataframe.rename(_inverted(GEOGRAPHYS), axis=1, inplace=True)
        function = lambda x, keys, names, values: str(Geography(keys=keys, names=x.to_dict()[names].split(', ')[::-1], values=[x.to_dict()[value] for value in values]))
        dataframe['geography'] = dataframe.apply(function, result_type='reduce', axis=1, args=(['state', 'county', 'subdivision'], 'name', ['state', 'county', 'subdivision']))
        dataframe['target'] = dataframe.apply(function, result_type='reduce', axis=1, args=(['state', 'county'], 'targetname', ['targestate', 'targetcounty']))
        return dataframe

    def variables(self, dataframe, *args, **kwargs):
        dataframe.rename(_inverted(TAGS), axis=1, inplace=True)
        dataframe = dataframe[['growth', 'entering', 'exiting', 'geography', 'target']](dataframe, *args, **kwargs) 
        return dataframe


class USCensus_ACS_WebCache(WebCache, querys=['date', 'state', 'county'], dataset='{}_{}_{}'.format('household', 'geography', 'mitgration')): pass
class USCensus_ACS_WebQueue(WebQueue, querys=['date', 'state', 'county']): 
    def date(self, *args, date=None, dates=[], **kwargs): return [str(item) for item in [date, *dates] if item]
    def state(self, *args, state, **kwargs): return [_state(state)]
    def county(self, *args, county=None, countys=[], **kwargs): return [_county(item) for item in [county, *countys] if item]


class USCensus_ACS_WebDownloader(WebDownloader, delay=25, attempts=10): 
    def execute(self, *args, queue, delayer, **kwargs):
        with USCensus_ACS_WebReader() as session:
            webpage = USCensus_ACS_WebPage(session, delayer=delayer) 
            for feedquery in iter(queue):
                for geography in self.geographys(webpage, **feedquery):
                    url = USCensus_ACS_WebURL(dataset='flows', tags=list(TAGS.values()), geography=geography, date=feedquery['date'], apikey=APIKEYS['uscensus'])
                    webpage.load(url, referer=None).setup()
                    for query, dataset, dataframe in webpage(**feedquery): yield USCensus_ACS_WebCache(query, {dataset:dataframe})
                        
    def geographys(self, webpage, *args, date, state, county, **kwargs):
        geography = Geography(keys=['state'], names=[None], values=[None])
        url = USCensus_ACS_WebURL(dataset='acs5', tags=['NAME'], geography=geography, date=date, apikey=APIKEYS['uscensus'])
        states = webpage.load(url, referer=None).setup()['geodata'].data
        geography = Geography(keys=['state', 'county'], names=[state, None], values=[states[state], None])
        url = USCensus_ACS_WebURL(dataset='acs5', tags=['NAME'], geography=geography, date=date, apikey=APIKEYS['uscensus'])
        countys = webpage.load(url, referer=None).setup()['geodata'].data
        geography = Geography(keys=['state', 'county', 'subdivision'], names=[state, county, None], values=[states[state], countys[county], None])
        url = USCensus_ACS_WebURL(dataset='acs5', tags=['NAME'], geography=geography, date=date, apikey=APIKEYS['uscensus'])
        subdivisions = webpage.load(url, referer=None).setup()['geodata'].data
        geographys = [Geography(keys=['state', 'county', 'subdivision'], names=[state, county, name], values=[states[state], countys[county], value]) for name, value in subdivisions.items()]
        for geography in geographys: yield geography

        
def main(*args, **kwargs): 
    webdelayer = USCensus_ACS_WebDelayer('constant', wait=15)
    webqueue = USCensus_ACS_WebQueue(REPORT_FILE, *args, **kwargs)
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
    sys.argv += ['dates=2017-2019', 'state=CA', 'county=Kern']
    logging.basicConfig(level='INFO', format="[%(levelname)s, %(threadName)s]:  %(message)s")
    inputparser = InputParser(proxys={'assign':'=', 'space':'_'}, parsers={'dates':_range, 'countys':_list}, default=str)   
    inputparser(*sys.argv[1:])
    main(*inputparser.inputArgs, **inputparser.inputParms)  
    
    
    
    
    
    