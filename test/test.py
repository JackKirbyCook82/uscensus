# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 2019
@name:   Test USCensus WebAPI
@author: Jack Kirby Cook

"""

import time
import os.path
import pandas as pd
import numpy as np

from variables.geography import Geography
from variables.date import Date
from parsers import ListParser, DictParser, FormatParser, FormatValueParser
from parsers.valuegenerators import NumGenerator, ValueGenerator 
from parsers.valueformatters import ValueFormatter

from uscensus import USCensus_WebAPI


_LINEWIDTH = 75
_DIR = os.path.dirname(os.path.realpath(__file__))
_aslist = lambda items: [items] if not isinstance(items, (list, tuple)) else items

pd.set_option("display.max_rows", 30)
pd.set_option("display.max_columns", 20)
np.set_printoptions(linewidth=_LINEWIDTH)


registry = dict()
def test(name):
    def decorator(function):        
        def wrapper(*args, **kwargs):
            print('='*_LINEWIDTH)
            print('TESTING: {}\n'.format(name.upper()))           
            starttime = time.time()
            obj = function(*args, **kwargs)
            endtime = time.time()
            print('\nSUCCESS: {:.6f} milliseconds\n'.format((endtime - starttime) * 1000))
            return obj  
        registry[name] = wrapper
        return wrapper
    return decorator


@test('webapi')
def test_webapi():    
    apikey = 'f98e5cb368f964cde784b85a0b22035efc3a3498'
    headers = {'USER_AGENT' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}
    tables_file = os.path.join(_DIR, 'myuscensustables.csv')
    repository = _DIR
    
    listparser = ListParser(';')
    dictparser = DictParser(pattern=';=', emptyvalues=False)
    tagsparser = FormatParser(NumGenerator(*'&-/'), pattern=';|&=')    
    conceptsparser = FormatValueParser(ValueGenerator.createall(*'&-/'), ValueFormatter.createall(delimiter='-'), pattern=';|&=') 
    
    tables_parsers = dict(parms=listparser, tags=tagsparser, preds=dictparser, concepts=conceptsparser)
    webapi = USCensus_WebAPI(apikey, tables_file=tables_file, tables_parsers=tables_parsers, repository=repository, headers=headers)
    webapi.setitems(universe='households', index='geography', headers='income')
    webapi['tenure'] = 'Owner'
    webapi['value'] = '<$10K US'

    geography = Geography(**{'state':48, 'county':157, 'subdivision':'*'})
    date = Date(year=2017)   
    estimate = 5       
    
    print(repr(webapi), '\n')
    print(str(webapi), '\n')
    print(webapi.selections, '\n')
    print(webapi(geography=geography, date=date, estimate=estimate))

    

    
def main(*args, **kwargs):    
    for key, value in kwargs.items():
        if value: registry[key]()
        
    
if __name__ == "__main__": 
    test = {'webapi' : True}
    main(**test)





