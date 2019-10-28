# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus Website Standards
@author: Jack Kirby cook

"""

import os.path
import json
import re
import pandas as pd
from collections import namedtuple as ntuple
from parse import parse

from utilities.dataframes import dataframe_fromfile

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Geography', 'USCensus_Variable']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""
    

_LABELDELIMITER = '!!'
_LABELALL = '*'
_LABELFORMATS = ('lowernum', 'uppernum', 'num')

_unspace_nums = lambda string: re.sub(r'(\d)\s+(\d)', r'\1\2', string)
_uncomma_nums =lambda string: re.sub('(?<=\d),(?=\d)', '', string)
_unformat_nums = lambda string: re.sub('\d+', '{}', string)
_remove_formatnames = lambda string: re.sub('|'.join(_LABELFORMATS), '', string)
_remove_commas = lambda string: re.sub(',', '', string)
_lowercase = lambda string: string.lower()

_DIR = os.path.dirname(os.path.realpath(__file__))
_GEOGRAPHY = dataframe_fromfile(os.path.join(_DIR, 'geography.csv'), index='geography', header=0, forceframe=True).transpose().to_dict()
_VARIABLES = dataframe_fromfile(os.path.join(_DIR, 'variables.csv'), index=None, header=0, forceframe=True)
_VARIABLES['variables'] = _VARIABLES['label'].apply(lambda x: _remove_formatnames(x))
_VARIABLES = _VARIABLES.set_index('variables', drop=True).drop_duplicates(keep='first')

_isnull = lambda value: pd.isnull(value) if not isinstance(value, (list, tuple, dict)) else False
_findall = lambda char, string: [i for i, c in enumerate(string) if c == char]
_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]


USCensus_GeographySgmts = ntuple('USCensus_GeographySgmts', 'geography apigeography shapegeography shapedir shapefile value')
class USCensus_Geography(USCensus_GeographySgmts):
    def __str__(self): 
        namestr = self.__class__.__name__
        jsonstr = json.dumps(self._asdict(), sort_keys=False, indent=3, separators=(',', ' : '), default=str)      
        return ' '.join([namestr, jsonstr])
    
    def __new__(cls, geokey, geovalue=None):
        geosgmts = {key:(value if not _isnull(value) else None) for key, value in _GEOGRAPHY[geokey].items()}
        return super().__new__(cls, geography=geokey, **geosgmts, value=geovalue)    
        
   
USCensus_VariableSgmts = ntuple('USCensus_VariableSgmts', 'tag group label')
class USCensus_Variable(USCensus_VariableSgmts):
    def __str__(self): 
        namestr = self.__class__.__name__
        jsonstr = json.dumps(self._asdict(), sort_keys=False, indent=3, separators=(',', ' : '), default=str)      
        return ' '.join([namestr, jsonstr])
        
    def __new__(cls, tag, *args, label, date, group=None, **kwargs):   
        label = tuple(label.format(date=str(date)).split(_LABELDELIMITER))   
        return super().__new__(cls, tag, group, label)

    def commonality(self, label): return _remove_commas(_uncomma_nums(_unspace_nums(label)))

    @property
    def concept(self):  
        variable = self.commonality(self.label[-1])
        if variable in _VARIABLES.index.values: pass
        else: variable = _unformat_nums(variable)
        
        label = _VARIABLES.loc[variable, 'label'] 
        concept = _VARIABLES.loc[variable, 'concept']

        unformated = parse(label, self.commonality(self.label[-1]))
        concept = concept.format(**unformated.named)
        return concept
    
    def __ne__(self, other): return not self.__eq__(other)
    def __eq__(self, other):
        function = lambda items: [_remove_commas(_lowercase(item)) for item in items]
        if isinstance(other, type(self)): return super().__eq__(other)
        elif isinstance(other, tuple): 
            if other[-1] == _LABELALL: return function(other[:-1]) == function(self.label[:-1])
            else: return function(other) == function(self.label)
        else: TypeError(type(other)) 
    


    
       
