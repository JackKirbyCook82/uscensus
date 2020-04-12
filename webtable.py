# -*- coding: utf-8 -*-
"""
Created on Sat Oct 19 2019
@name:   USCensus Website Table Application
@author: Jack Kirby Cook

"""

import sys
import os.path
import json

import tables as tbls
from parsers import ListParser, DictParser, DictorListParser
from specs import specs_fromfile
from variables import Variables, Date, Geography
from utilities.inputparsers import InputParser
from webscraping.webreaders import WebReader

from uscensus.urlapi import USCensus_ACS_URLAPI
from uscensus.webapi import USCensus_WebAPI
from uscensus.cleaners import USCensus_Cleaner
from uscensus.website import USCensus_Variable_WebQuery
from uscensus.webquery import query

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['acs_webapi', 'cleaner', 'variables']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


APIKEY = 'f98e5cb368f964cde784b85a0b22035efc3a3498'
DIR = os.path.dirname(os.path.realpath(__file__))
SPECS_FILE = os.path.join(DIR, 'specs.csv')
ROOT_DIR = os.path.abspath(os.path.join(DIR, os.pardir))
SAVE_DIR = os.path.join(ROOT_DIR, 'save')

specsparsers = {'databasis': DictorListParser(pattern=';=')}
specs = specs_fromfile(SPECS_FILE, specsparsers)
custom_variables = Variables.create(**specs, name='USCensus')
noncustom_variables = Variables.load('date', 'geography', 'geopath', name='USCensus')
variables = custom_variables.update(noncustom_variables)

headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}
retry = {'retries':3, 'backoff':0.3, 'httpcodes':(500, 502, 504)}
webreader = WebReader(delay=3, headers=headers, retry=retry)
cleaner = USCensus_Cleaner(variables)
acs_urlapi = USCensus_ACS_URLAPI(APIKEY)
acs_variable_webquery = USCensus_Variable_WebQuery(acs_urlapi, webreader, tolerance=0)
acs_webapi = USCensus_WebAPI(SAVE_DIR, acs_urlapi, webreader, variable_webquery=acs_variable_webquery, saving=True)

AGGS = {'households':'sum', 'population':'sum', 'structures':'sum'}


def acsfeed(*args, tableID, universe, index, header, scope, **kwargs):
    dataframe = acs_webapi(*args, tableID=tableID, universe=universe, index=index, header=header, scope=scope, **kwargs)
    dataframe = cleaner(dataframe, *args, **kwargs)
    flattable = tbls.FlatTable(dataframe, variables=variables, name=tableID)
    arraytable = flattable[[universe, index, header, 'date', *scope.keys()]].unflatten(universe, aggs=AGGS)
    arraytable = arraytable.squeeze(*scope.keys()).sortall(ascending=True)   
    return arraytable


def main(*inputArgs, tableID, **inputParms): 
    print(str(variables), '\n')
    print(str(inputparser), '\n')  
    if tableID in query:
        tableParms = query(tableID)
        arraytable = acsfeed(*inputArgs, tableID=tableID, **tableParms, **inputParms)
        print(str(arraytable))
    else: 
        print("{} Table Not Supported: '{}'".format('USCensus', tableID))
        print(' '.join(['USCensus Tables', json.dumps(query.tableIDs, sort_keys=False, indent=3, separators=(',', ' : '), default=str)]))    
    

if __name__ == '__main__':  
    tbls.set_options(linewidth=110, maxrows=40, maxcolumns=10, threshold=100, precision=3, fixednotation=True, framechar='=')
    tbls.show_options()
    
    dictparser = DictParser(pattern=',|')
    listparser = ListParser(pattern=',')
    geography_parser = lambda item: Geography(dictparser(item))
    dates_parser = lambda items: [Date.fromstr(item) for item in listparser(items)]
    date_parser = lambda item: Date.fromstr(item) if item else None
    variable_parsers = {'geography':geography_parser, 'dates':dates_parser}
    inputparser = InputParser(assignproxy='=', spaceproxy='_', parsers=variable_parsers)
    
    print(repr(inputparser))
    print(repr(query))
    print(repr(acs_urlapi))
    print(repr(acs_webapi), '\n')  
    
    sys.argv.extend(['tableID=', 
                     'geography=state|48,county|157,tract|*,block|*', 
                     'dates=2015,2016,2017',
                     'date=2018'])
    inputparser(*sys.argv[1:])  
    main(*inputparser.inputArgs, **inputparser.inputParms)















