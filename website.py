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

from utilities.dispatchers import clskey_singledispatcher as keydispatcher
from utilities.dataframes import dataframe_fromfile, dataframe_parser

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Geography_WebQuery', 'USCensus_Variable_WebQuery', 'USCensus_APIGeography']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""
    

_DIR = os.path.dirname(os.path.realpath(__file__))
_VARIABLE_DELIMITER = '!!'
_GEOGRAPHY_DELIMITER = '› '
_ALL = '*'


_remove_nums = lambda string: re.sub('\d+', '{}', string)
_unspace_nums = lambda string: re.sub(r'(\d)\s+(\d)', r'\1\2', string)
_uncomma_nums =lambda string: re.sub('(?<=\d),(?=\d)', '', string)
_unspace = lambda string: re.sub(' ', '', string)
_uncomma = lambda string: re.sub(',', '', string)
_lowercase = lambda string: string.lower()

_variableparser = lambda string: _lowercase(_uncomma(_uncomma_nums(_unspace_nums(string))))
_labelparser = lambda string: _lowercase(_uncomma(_uncomma_nums(_unspace_nums(string))))

_isnull = lambda value: pd.isnull(value) if not isinstance(value, (list, tuple, dict)) else False
_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]


_GEOGRAPHY = dataframe_fromfile(os.path.join(_DIR, 'geography.csv'), index='geography', header=0, forceframe=True).transpose().to_dict()
_GEOGRAPHY = {geography:{key:(value if not _isnull(value) else None) for key, value in items.items() } for geography, items in _GEOGRAPHY.items()}
_VARIABLES = dataframe_fromfile(os.path.join(_DIR, 'variables.csv'), index=None, header=0, forceframe=True)
_VARIABLES = dataframe_parser(_VARIABLES, parsers={'variable':_variableparser}).set_index('variable', drop=True).to_dict()['concept']


USCensus_Variable_Sgmts = ntuple('USCensus_Variable_Sgmts', 'tag, group, label variable')
class USCensus_Variable(USCensus_Variable_Sgmts): 
    def __str__(self): 
        namestr = self.__class__.__name__
        jsonstr = json.dumps(self._asdict(), sort_keys=False, indent=3, separators=(',', ' : '), default=str)      
        return ' '.join([namestr, jsonstr])    
    
    def __new__(cls, tag, group, date, label):
        label = tuple(label.format(date=str(date)).split(_VARIABLE_DELIMITER))  
        return super().__new__(cls, tag, group, label[:-1], label[-1])
    
    @property
    def concept(self):
        variable = self.format_variable(self.variable)       
        if variable in _VARIABLES.keys(): pass
        else: variable = _remove_nums(variable)
        unformated = parse(variable, self.variable)
        if unformated is None: concept = _VARIABLES[variable]
        else: concept = _VARIABLES[variable].format(*unformated.fixed)  
        return concept

    def format_label(self, *label): return [_labelparser(item) for item in label]
    def format_variable(self, variable): return _variableparser(variable)
    
    def strict_variable_match(self, *variables): return any([self.format_variable(item) == self.format_variable(self.variable) for item in variables])
    
    @keydispatcher('method')
    def label_match(self, *label, method, **kwargs): raise KeyError(method) 
    @label_match.register('strict')
    def strict_label_match(self, *label, method, **kwargs): return self.format_label(*label) == self.format_label(*self.label)
    @label_match.register('exact')
    def exact_label_match(self, *label, method, **kwargs): return set(self.format_label(*label)) == set(self.format_label(*self.label))   
    @label_match.register('under')
    def under_label_match(self, *label, method, **kwargs): return all([item in set(self.format_label(*label)) for item in set(self.format_label(*self.label))])
    @label_match.register('over')
    def over_label_match(self, *label, method, **kwargs): return all([item in set(self.format_label(*self.label)) for item in set(self.format_label(*label))])
        
    def match(self, label, method='strict'):
        label, variable = label[:-1], label[-1]
        label_match = self.label_match(method=method, *label)
        variable_match = any([self.strict_variable_match(variable), variable == _ALL])
        return label_match and variable_match
        

USCensus_APIGeography_Sgmts = ntuple('USCensus_APIGeography_Sgmts', 'geography apigeography shapegeography shapedir shapefile value')
class USCensus_APIGeography(USCensus_APIGeography_Sgmts):
    def __new__(cls, geokey, geovalue=None):
        geosgmts = {key:(value if not _isnull(value) else None) for key, value in _GEOGRAPHY[geokey].items()}
        return super().__new__(cls, geography=geokey, **geosgmts, value=geovalue)  


USCensus_Geography_Sgmts = ntuple('USCensus_Geography_Sgmts', 'geography required geographylevel')
class USCensus_Geography(USCensus_Geography_Sgmts): 
    def __str__(self): 
        namestr = self.__class__.__name__
        jsonstr = json.dumps(self._asdict(), sort_keys=False, indent=3, separators=(',', ' : '), default=str)      
        return ' '.join([namestr, jsonstr])      
    
    def __new__(cls, forgeography, geographylevel, ingeography):
        apigeography = {value['apigeography']:key for key, value in _GEOGRAPHY.items() if value['apigeography']}
        ingeography = [apigeography[item] for item in ingeography]
        return super().__new__(cls, forgeography, _aslist(ingeography), geographylevel)

    @property
    def apigeography(self): return _GEOGRAPHY[self.geography]['apigeography']
    @property
    def shapedir(self): return _GEOGRAPHY[self.geography]['shapedir']
    @property
    def shapefile(self): return _GEOGRAPHY[self.geography]['shapefile']
    @property
    def shapegeography(self): return _GEOGRAPHY[self.geography]['shapegeography']


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
        return [USCensus_Variable(tag=key, group=values['group'], date=date, label=values['label']) for key, values in jsondata['variables'].items()]

    def generator(self, labels, variables):
        for label in labels:
            for method in ('strict', 'exact', 'over', 'under'):      
                matches = [variable for variable in variables if variable.match(label, method=method)]
                if matches: break
            if matches: 
                for item in matches: yield item
            else: raise ValueError(label)
                
    def __call__(self, *args, labels, date, **kwargs):
        labels = [tuple([item.format(date=str(date)) for item in label]) for label in labels]
        variables = self.download(*args, date=date, **kwargs)
        return [item for item in self.generator(labels, variables)]


class USCensus_Geography_WebQuery(USCensus_WebQuery):
    def download(self, *args, **kwargs):
        url = self.urlapi.query(*args, query='geography', filetype='json', **kwargs)
        jsondata = self.webreader(str(url), *args, **kwargs)
        return [USCensus_Geography(forgeography=item['name'], geographylevel=item['geoLevelDisplay'], ingeography=item.get('requires', [])) for item in jsondata['fips']]

    def __call__(self, *args, geography, **kwargs):
        uscensus_geographys = self.download(*args, **kwargs)
        return [item for item in uscensus_geographys if item.geography in geography.keys()]
        







