# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus Website Standards
@author: Jack Kirby cook

"""

from abc import ABC, abstractmethod
import os.path
import json
import re
import pandas as pd
from collections import namedtuple as ntuple
from parse import parse

from utilities.dataframes import dataframe_fromfile, dataframe_parser

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Geography_WebQuery', 'USCensus_Variable_WebQuery']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""
    

_remove_nums = lambda string: re.sub('\d+', '{}', string)
_unspace_nums = lambda string: re.sub(r'(\d)\s+(\d)', r'\1\2', string)
_uncomma_nums =lambda string: re.sub('(?<=\d),(?=\d)', '', string)
_unspace = lambda string: re.sub(' ', '', string)
_uncomma = lambda string: re.sub(',', '', string)
_lowercase = lambda string: string.lower()

_indexparser = lambda string: _lowercase(_uncomma(_uncomma_nums(_unspace_nums(string))))
_labelparser = lambda string: _lowercase(_uncomma(_uncomma_nums(_unspace_nums(string))))

_DIR = os.path.dirname(os.path.realpath(__file__))
_GEOGRAPHY = dataframe_fromfile(os.path.join(_DIR, 'geography.csv'), index='geography', header=0, forceframe=True).transpose().to_dict()
_VARIABLES = dataframe_fromfile(os.path.join(_DIR, 'variables.csv'), index=None, header=0, forceframe=True)
_VARIABLES = dataframe_parser(_VARIABLES, parsers={'variable':_indexparser}).set_index('variable', drop=True).to_dict()['concept']
_VARIABLE_DELIMITER = '!!'
_GEOGRAPHY_DELIMITER = '› '
_ALL = '*'

_isnull = lambda value: pd.isnull(value) if not isinstance(value, (list, tuple, dict)) else False
_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]


USCensus_Variable_Sgmts = ntuple('USCensus_Variable_Sgmts', '')
class USCensus_Variable(USCensus_Variable_Sgmts): 
    def __str__(self): 
        namestr = self.__class__.__name__
        jsonstr = json.dumps(self._asdict(), sort_keys=False, indent=3, separators=(',', ' : '), default=str)      
        return ' '.join([namestr, jsonstr])    
    
    def __new__(cls, tag, group, label):
        pass
    

USCensus_Geography_Sgmts = ntuple('USCensus_Geography_Sgmts', '')
class USCensus_Geography(USCensus_Geography_Sgmts): 
    def __str__(self): 
        namestr = self.__class__.__name__
        jsonstr = json.dumps(self._asdict(), sort_keys=False, indent=3, separators=(',', ' : '), default=str)      
        return ' '.join([namestr, jsonstr])      
    
    def __new__(cls, forgeo, geolevel, ingeo):
        pass


class USCensus_WebQuery(ABC):
    def __init__(self, urlapi, webreader): self.__urlapi, self.__webreader = urlapi, webreader       
    @property
    def series(self): return self.__urlapi.series
    @property
    def survey(self): return self.__urlapi.survey   
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader
    @abstractmethod
    def download(self, *args, **kwargs): pass

   
class USCensus_Variable_WebQuery(USCensus_WebQuery):
    def download(self, *args, group=None, date, **kwargs):
        if not group: url = self.urlapi.query(*args, query='variables', filetype='json', date=date, **kwargs)   
        else: url = self.urlapi.query(*args, query=['groups', group], filetype='json', date=date, **kwargs)   
        jsondata = self.webreader(str(url), *args, **kwargs)
        return [USCensus_Variable(tag=key, group=values['group'], label=values['label']) for key, values in jsondata['variables'].items()]
        
    def __call__(self, *args, **kwargs):
        pass


class USCensus_Geography_WebQuery(USCensus_WebQuery):
    def download(self, *args, geography, **kwargs):
        url = self.urlapi.query(*args, query='', filetype='json', **kwargs)
        jsondata = self.webreader(str(url), *args, **kwargs)
        return [USCensus_Geography(forgeo=item['name'], geolevel=item['geoLevelDisplay'], ingeo=item.get('requires', [])) for item in jsondata['fips']]

    def __call__(self, *args, **kwargs):
        pass






#USCensus_GeographySgmts = ntuple('USCensus_GeographySgmts', 'geography apigeography shapegeography shapedir shapefile value')
#class USCensus_Geography(USCensus_GeographySgmts):
#    def __new__(cls, geokey, geovalue=None):
#        geosgmts = {key:(value if not _isnull(value) else None) for key, value in _GEOGRAPHY[geokey].items()}
#        return super().__new__(cls, geography=geokey, **geosgmts, value=geovalue)    
        

#USCensus_VariableSgmts = ntuple('USCensus_VariableSgmts', 'tag group label variable')
#class USCensus_Variable(USCensus_VariableSgmts):
#    def __new__(cls, tag, *args, label, date, group=None, **kwargs):   
#        items = tuple(label.format(date=str(date)).split(_DELIMITER))   
#        return super().__new__(cls, tag, group, items[:-1], items[-1])
#
#    @property
#    def concept(self):
#        variable = self.format_variable(self.variable)       
#        if variable in _VARIABLES.keys(): pass
#        else: variable = _remove_nums(variable)
#        unformated = parse(self.variable, variable)
#        try: concept = _VARIABLES[variable].format(*unformated.fixed)
#        except AttributeError: concept = _VARIABLES[variable]
#        return concept
#
#    def format_label(self, *label): return set([_labelparser(item) for item in label])
#    def format_variable(self, variable): return _indexparser(variable)
#
#    def strict_label_match(self, label): return self.format_label(*label) == self.format_label(*self.label)
#    def inclosed_label_match(self, label): pass
#    
#    def strict_variable_match(self, variable): return self.format_variable(variable) == self.format_variable(self.variable)
#    def all_variable_match(self, variable): return variable == _ALL
#
#    def match(self, label, variable, strict=True):
#        assert isinstance(label, tuple)
#        assert isinstance(variable, str)
#        if strict: return self.strict_label_match(label) and (self.strict_variable_match(variable) or self.all_variable_match(variable))
#        else: return self.strict_label_match(label) and (self.strict_variable_match(variable) or self.all_variable_match(variable))



    
    
    
#    def variablequery(self, *args, group=None, labels, variables, date, **kwargs):   
#        assert len(labels) == len(variables)
#        labels = [tuple([item.format(date=str(date)) for item in label]) for label in labels]
#        if not group: url = self.urlapi.query(*args, query='variables', filetype='json', date=date, **kwargs)   
#        else: url = self.urlapi.query(*args, query=['groups', group], filetype='json', date=date, **kwargs)   
#        variabledata = self.webreader(str(url), *args, **kwargs)
#        uscensus_variables =  [USCensus_Variable(tag=key, date=date, **items) for key, items in variabledata['variables'].items()]
#        
#        variables = [item for label, variable in zip(labels, variables) for item in uscensus_variables if item.match(label, variable, strict=True)]
#        if not variables: variables = [item for label, variable in zip(labels, variables) for item in uscensus_variables if item.match(label, variable, strict=False)]            
#        if not variables: raise ValueError(variables)
#        return variables           

#    def geographyquery(self, *args, geography, **kwargs):
#        uscensus_geographys = [USCensus_Geography(geokey=geokey, geovalue=geovalue) for geokey, geovalue in geography.items()]
#        return uscensus_geographys   
