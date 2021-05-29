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
import pandas as pd
import regex as re
from parse import parse

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
from utilities.dataframes import dataframe_fromfile, dataframe_parser
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

TABLES = dataframe_parser(dataframe_fromfile(TABLES_FILE, index='table', header=0), parsers={}, defaultparser=str)
DATASETS = list(set(TABLES[['universe', 'index', 'header']].apply(lambda x: '_'.join([i for i in x if pd.notnull(i)]), axis=1).to_numpy()))
VARIABLES = {
    'less than ${}':'<${}',  '${} to ${}':'${}|${}', '${} or more':'>${}', 'no chase rent':'$0', 
    'less than {} percent':'<{}%', '{} to {} percent':'{}%|{}%', '{} percent or more':'>{}%', 'not computed':'N/A',
    'under {} years':'<{}YRS', '{} to {} years':'{}YRS|{}YRS', '{} and {} years':'{}YRS|{}YRS', '{} years and over':'>{}YRS', '{} years':'{}YRS',
    'with a mortgage':'w/Mortgage', 'without a mortgage':'w/oMortgage', 'other race':'other', 'more races':'other', 'hispanic':'w/Hispanic', 'not hispanic':'w/oHispnaic',
    'no schooling':'noschool', 'nursery':'noschool', 'kindergarden':'noschool', 'grade':'noschool', 'high school':'highschool', 'ged':'highschool', 'some college':'highschool',
    'less than {} minutes':'<{}MINS', '{} to {} minutes':'{}MINS|{}MINS', '{} or more minutes':'>{}MINS'}
GEOGRAPHYS = {
    'state':'state', 'county':'county', 'subdivision':'county subdivision', 'tract':'tract', 'block':'block group', 'zipcode':'zip code tabulation area',
    'combined':'combined statistical area', 'metro':'metropolitan statistical area/micropolitan statistical area', 'division':'metropolitan division',
    'congress':'congressional district', 'upperlegislative':'state legislative district (upper chamber)', 'lowerlegislative':'state legislative district (lower chamber)', 
    'elementary':'school district (elementary)', 'secondary':'school district (secondary)', 'unified':'school district (unified)'}
STATES = {'AL':'Alabama', 'AK':'Alaska','AZ':'Arizona', 'AR':'Arkansas', 'CA':'California', 'CO':'Colorado', 'CT':'Connecticut', 'DE':'Delaware', 'FL':'Florida', 'GA':'Georgia', 
          'HI':'Hawaii', 'ID':'Idaho', 'IL':'Illinois', 'IN':'Indiana', 'IA':'Iowa', 'KS':'Kansas', 'KY':'Kentucky', 'LA':'Louisiana', 'ME':'Maine', 'MD':'Maryland', 'MA':'Massachusetts', 
          'MI':'Michigan', 'MN':'Minnesota', 'MS':'Mississippi', 'MO':'Missouri', 'MT':'Montana', 'NE':'Nebraska', 'NV':'Nevada', 'NH':'New Hampshire', 'NJ':'New Jersey', 'NM':'New Mexico', 
          'NY':'New York', 'NC':'North Carolina', 'ND':'North Dakota', 'OH':'Ohio', 'OK':'Oklahoma', 'OR':'Oregon', 'PA':'Pennsylvania', 'RI':'Rhode Island', 'SC':'South Carolina', 'SD':'South Dakota', 
          'TN':'Tennessee', 'TX':'Texas', 'UT':'Utah', 'VT':'Vermont', 'VA':'Virginia', 'WA':'Washington', 'WV':'West Virginia', 'WI':'Wisconsin', 'WY':'Wyoming'}


_inverted = lambda x: {v:k for k, v in x.items()}
_range = lambda x: [str(i) for i in range(int(x.split('-')[0]), int(x.split('-')[1])+1)]
_list = lambda x: [str(i) for i in str(x).split('|')]
_state = lambda x: str(x) if str(x) in STATES.values() else STATES[str(x)]
_county = lambda x: ' '.join([str(x), 'County']) if not str(x).endswith(' County') else str(x)
_tag = lambda *args: {'get':'{}'.format(','.join(list(args)))}
_forgeo = lambda **kwargs: {'for':'{}'.format('%20'.join([':'.join([key, value]) for key, value in kwargs.items()]))} if kwargs else {}
_ingeo = lambda **kwargs: {'in':'{}'.format('%20'.join([':'.join([key, value]) for key, value in kwargs.items()]))} if kwargs else {}
_apikey = lambda apikey: {'key':'{}'.format(str(apikey))}
    

def _labels(string):
    for key, value in VARIABLES.items():
        if key == string: return value
        content = parse(key, string)
        if not content: continue
        return value.format(*content.fixed)        

def _tables(querys):
    for query in querys: yield query, TABLES.loc[query['table'], :].squeeze().to_dict()

def _variables(json): return {key:value['label'] for key, value in json['variables'].items() if str(key).endswith('E')}    
def _geography(json): 
    items = [(items['name'], items.get('requires', [])) for items in json['fips']]
    required = {}
    for (key, values) in items:
        try: required[key] = list({*required[key], *values})
        except KeyError: required[key] = list(values)
    return required
    
def _vardata(json): return pd.DataFrame(data=json[1:], columns=json[0])    
def _geodata(json):
    keys = tuple(json[0][1:])
    namesdata = [tuple(str(i[0]).split(', ')[::-1]) for i in json[1:]]
    valuesdata = [tuple(i[1:]) for i in json[1:]]
    return {(keys[-1], names[-1]):values[-1] for names, values in zip(namesdata, valuesdata)}


class USCensus_ACS_WebGeography(WebJson.customize(dataparser=_geography)): pass
class USCensus_ACS_WebVariables(WebJson.customize(dataparser=_variables)): pass
class USCensus_ACS_WebGroups(WebJson.customize(dataparser=_variables)): pass
class USCensus_ACS_WebGeoData(WebJson.customize(dataparser=_geodata)): pass
class USCensus_ACS_WebVarData(WebJson.customize(dataparser=_vardata)): pass

class USCensus_ACS_WebDelayer(WebDelayer): pass
class USCensus_ACS_WebReader(WebReader, retrys=Retrys(retries=3, backoff=0.3, httpcodes=(500, 502, 504)), authenticate=None): pass


class USCensus_ACSGeography_WebURL(WebURL, protocol='https', domain='api.census.gov', path=['data', '{date}', 'acs', 'acs5', 'geography', '.json']): pass
class USCensus_ACSVariable_WebURL(WebURL, protocol='https', domain='api.census.gov', path=['data', '{date}', 'acs', 'acs5', 'variables', '.json']): pass  
class USCensus_ACSGroup_WebURL(WebURL, protocol='https', domain='api.census.gov', path=['data', '{date}', 'acs', 'acs5', 'groups', '{group}', '.json']): pass  
class USCensus_ACS_WebURL(WebURL, protocol='https', domain='api.census.gov', spaceproxy='%20'): 
    def path(self, *args, date, **kwargs): return ['data', str(date), 'acs', 'acs5']
    def parms(self, *args, tags, geography, apikey, **kwargs): 
        assert isinstance(geography, Geography)
        forgeo = {GEOGRAPHYS[key]:(value if value is not None else '*') for key, name, value in geography[-1].items()} 
        ingeo = {GEOGRAPHYS[key]:(value if value is not None else '*') for key, name, value in geography[:-1].items()}
        return {**_tag(*tags), **_forgeo(**forgeo), **_ingeo(**ingeo), **_apikey(apikey)}

pd.set_option('max_columns', 50)
contents = {'geography':USCensus_ACS_WebGeography, 
            'variables':USCensus_ACS_WebVariables, 
            'groups':USCensus_ACS_WebGroups, 
            'geodata':USCensus_ACS_WebGeoData, 
            'vardata':USCensus_ACS_WebVarData}
class USCensus_ACS_WebPage(WebJsonPage, contents=contents):
    def setup(self, *args, **kwargs):
        self.loadAllContents()
        return self
    
    def execute(self, *args, table, universe, index, header, scope, date, geography, **kwargs): 
        query = {'table':table, 'date':date, 'geography':geography, **{key:value for key, value in kwargs.items() if key in GEOGRAPHYS.keys()}}
        dataset = '{}_{}_{}'.format(universe, index, header)
        dataset = dataset if not str(dataset).endswith('_') else dataset[:-1]
        dataframe = self['vardata'].data
        dataframe = self.variables(dataframe, *args, universe=universe, index=index, header=header, **kwargs)
        dataframe = self.geography(dataframe, *args, **kwargs)
        dataframe['date'] = date
        dataframe['scope'] = scope
        if pd.notnull(header): dataframe = dataframe[[universe, index, header, 'scope', 'date']]
        else: dataframe = dataframe[[universe, index, 'scope', 'date']]
        yield query, dataset, dataframe

    def geography(self, dataframe, *args, **kwargs):
        geokeys = [column for column in dataframe.columns if column in GEOGRAPHYS.values()]
        function = lambda x: str(Geography(keys=list(geokeys), names=x.to_dict()['NAME'].split(', ')[::-1], values=[x.to_dict()[geokey] for geokey in geokeys]))
        dataframe['geography'] = dataframe.apply(function, result_type='reduce', axis=1)
        return dataframe
    
    def variables(self, dataframe, *args, variables, label, universe, index, header, **kwargs):
        idVars = [column for column in dataframe.columns if column not in variables.keys()]
        valVars = list(variables.keys())
        if pd.isnull(header): 
            assert len(valVars) == 1
            dataframe.rename({valVars[0]:universe}, axis=1, inplace=True)
            return dataframe
        else: 
            assert len(valVars) > 1
            dataframe = dataframe.melt(id_vars=idVars, value_vars=valVars, var_name=header, value_name=universe, ignore_index=True)
            function = lambda x: re.findall(label, variables[x])[0]
            dataframe[header] = dataframe[header].apply(function)
            dataframe[header] = dataframe[header].apply(_labels)
            return dataframe


class USCensus_ACS_WebCache(WebCache, querys=['table', 'date', 'geography', 'state', 'county'], datasets=DATASETS): pass
class USCensus_ACS_WebQueue(WebQueue, querys=['table', 'date', 'geography', 'state', 'county']): 
    def table(self, *args, table, **kwargs): return [str(table)]    
    def date(self, *args, date=None, dates=[], **kwargs): return [str(item) for item in [date, *dates] if item]
    def geography(self, *args, geography, **kwargs): return [str(geography)]
    def state(self, *args, state, **kwargs): return [_state(state)]
    def county(self, *args, county=None, countys=[], **kwargs): return [_county(item) for item in [county, *countys] if item]


class USCensus_ACS_WebDownloader(WebDownloader, delay=25, attempts=10): 
    def execute(self, *args, queue, delayer, **kwargs):
        with USCensus_ACS_WebReader() as session:
            webpage = USCensus_ACS_WebPage(session, delayer=delayer) 
            for feedquery, tablequery in _tables(iter(queue)):
                geography = self.geography(webpage, **feedquery)
                geography[feedquery['geography']] = {'name':None, 'value': None}
                variables = self.variables(webpage, **tablequery, **feedquery)
                url = USCensus_ACS_WebURL(tags=['NAME', *list(variables.keys())], geography=geography, date=feedquery['date'], apikey=APIKEYS['uscensus'])
                webpage.load(url, referer=None).setup()
                for query, dataset, dataframe in webpage(variables=variables, **feedquery, **tablequery): 
                    yield USCensus_ACS_WebCache(query, {dataset:dataframe})

    def geography(self, webpage, *args, date, **kwargs):
        url = USCensus_ACSGeography_WebURL(date=date)
        orders = webpage.load(url, referer=None).setup()['geography'].data
        orders = {_inverted(GEOGRAPHYS)[key]:[_inverted(GEOGRAPHYS)[value] for value in values if value in GEOGRAPHYS.values()] for key, values in orders.items() if key in GEOGRAPHYS.values()}
        orders = [[value for value in values if value in kwargs.keys()] + [key] for key, values in orders.items() if key in kwargs.keys()]
        order = max(orders, key=lambda x: len(x))
        geography = Geography(keys=order)
        for index, key in enumerate(order, start=1):
            url = USCensus_ACS_WebURL(tags=['NAME'], geography=geography[0:index], date=date, apikey=APIKEYS['uscensus'])   
            geovalues = webpage.load(url, referer=None).setup()['geodata'].data 
            geography[key] = dict(name=kwargs[key], value=geovalues[(key, kwargs[key])])            
        return geography

    def variables(self, webpage, *args, date, group, label, **kwargs):
        url = USCensus_ACSGroup_WebURL(date=date, group=group)
        variables = webpage.load(url, referer=None).setup()['groups'].data
        variables = {key:value for key, value in variables.items() if re.findall(label, value)} 
        return variables
                               

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
    sys.argv += ['table=#hh|geo|inc', 'dates=2015-2019', 'geography=tract', 'state=CA', 'county=Kern']
    logging.basicConfig(level='INFO', format="[%(levelname)s, %(threadName)s]:  %(message)s")
    inputparser = InputParser(proxys={'assign':'=', 'space':'_'}, parsers={'dates':_range, 'countys':_list}, default=str)   
    inputparser(*sys.argv[1:])
    main(*inputparser.inputArgs, **inputparser.inputParms)  
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    