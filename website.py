# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus Website Objects
@author: Jack Kirby cook

"""

import os.path
import json
import re
import pandas as pd
from collections import namedtuple as ntuple
from collections import OrderedDict as ODict
from parse import parse

from variables import Geography, Geopath
from utilities.dispatchers import clskey_singledispatcher as keydispatcher
from utilities.dataframes import dataframe_fromfile, dataframe_parser, dataframe_fromjson

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['geographies', 'variables', 'USCensus_Geography_WebQuery', 'USCensus_Variable_WebQuery', 'USCensus_APIGeography', 'USCensus_APIVariable']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""
    

_DIR = os.path.dirname(os.path.realpath(__file__))
_VARIABLE_DELIMITER = '!!'
_GEOGRAPHY_DELIMITER = '› '
_ALL = '*'

_remove_nums = lambda string: re.sub('\d+', '{}', string)
_unspace_nums = lambda string: re.sub(r'(\d)\s+(\d)', r'\1\2', string)
_uncomma_nums = lambda string: re.sub('(?<=\d),(?=\d)', '', string)
_unspace = lambda string: re.sub(' ', '', string)
_uncomma = lambda string: re.sub(',', '', string)
_unquote = lambda string: re.sub(r'"', '', string)
_lowercase = lambda string: string.lower()

_variableparser = lambda string: _lowercase(_unquote(_uncomma(_uncomma_nums(_unspace_nums(string)))))
_labelparser = lambda string: _lowercase(_unquote(_uncomma(_uncomma_nums(_unspace_nums(string)))))

_isnull = lambda value: pd.isnull(value) if not isinstance(value, (list, tuple, dict)) else False
_aslist = lambda items: [items] if not isinstance(items, (list, tuple)) else list(items)
_filterempty = lambda items: [item for item in _aslist(items) if item]
_tonumber = lambda num: int(float(num)) if not bool(float(num) % 1) else float(num)

_GEOGRAPHY = dataframe_fromfile(os.path.join(_DIR, 'geography.csv'), index='geography', header=0, forceframe=True).transpose().to_dict()
_GEOGRAPHY = {geography:{key:(value if not _isnull(value) else None) for key, value in items.items() } for geography, items in _GEOGRAPHY.items()}
_VARIABLES = dataframe_fromfile(os.path.join(_DIR, 'variables.csv'), index=None, header=0, forceframe=True)
_VARIABLES = dataframe_parser(_VARIABLES, parsers={'variable':_variableparser}).set_index('variable', drop=True)
_CONCEPTS = _VARIABLES['concept'].to_dict()
_ADJUSTS = {key:float(value) for key, value in _VARIABLES['adjust'].to_dict().items() if not _isnull(value)}

def geographies(): return list(set(_GEOGRAPHY.keys()))
def variables(): return list(set(_VARIABLES.values()))

class USCensus_APIGeography(ntuple('USCensus_APIGeography', 'geography apigeography shapegeography shapedir shapefile value')):
    def __new__(cls, geokey, geovalue=None):
        geosgmts = {key:(value if not _isnull(value) else None) for key, value in _GEOGRAPHY[geokey].items()}
        return super().__new__(cls, geography=geokey, **geosgmts, value=geovalue)  
    

class USCensus_APIVariable(ntuple('USCensus_APIVariable', 'tag group label variable')): 
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
        variablekey = variable
        if variablekey not in _CONCEPTS.keys(): variablekey = _remove_nums(variablekey)       
        if variablekey not in _CONCEPTS.keys(): return self.tag
  
        concept = _CONCEPTS[variablekey]
        adjust = _tonumber(_ADJUSTS.get(variablekey, 0))
        content = parse(variablekey, variable)
        content = tuple(content.fixed) if content is not None else tuple()      
        if not content: return concept
        else: return concept.format(*[_tonumber(i) + adjust for i in content])
        
    def format_label(self, *label): return [_labelparser(item) for item in label]
    def format_variable(self, variable): return _variableparser(variable)
    
    def strict_variable_match(self, *variables): return any([self.format_variable(item) == self.format_variable(self.variable) for item in variables])
    
    @keydispatcher
    def label_match(self, method, label, **kwargs): raise KeyError(method) 
    @label_match.register('strict')
    def strict_label_match(self, label, **kwargs): return self.format_label(*label) == self.format_label(*self.label)
    @label_match.register('exact')
    def exact_label_match(self, label, **kwargs): return set(self.format_label(*label)) == set(self.format_label(*self.label))   
    @label_match.register('under')
    def under_label_match(self, label, **kwargs): return all([item in set(self.format_label(*label)) for item in set(self.format_label(*self.label))])
    @label_match.register('over')
    def over_label_match(self, label, **kwargs): return all([item in set(self.format_label(*self.label)) for item in set(self.format_label(*label))])
    @label_match.register('underdiff')
    def underdiff_label_match(self, label, tolerance=1, **kwargs): return len(set(self.format_label(*self.label))) - len(set(self.format_label(*label))) <= tolerance
    @label_match.register('overdiff')
    def overdiff_label_match(self, label, tolerance=1, **kwargs): return len(set(self.format_label(*label))) - len(set(self.format_label(*self.label))) <= tolerance
    @label_match.register('totaldiff')
    def totaldiff_label_match(self, label, tolerance=1, **kwargs): return self.overdiff_label_match(label, tolerance=tolerance, **kwargs) and self.underdiff_label_match(label, tolerance=tolerance, **kwargs)
    
    def match(self, label, method='strict', **kwargs):
        label, variable = label[:-1], label[-1]
        label_match = self.label_match(method, label, **kwargs)
        variable_match = any([self.strict_variable_match(variable), variable == _ALL])
        return label_match and variable_match


class USCensus_Variable_WebQuery(object):
    def __init__(self, urlapi, webreader, tolerance=0):
        self.__urlapi = urlapi
        self.__webreader = webreader
        self.__tolerance = tolerance
         
    @property
    def series(self): return self.__urlapi.series
    @property
    def survey(self): return self.__urlapi.survey   
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader        
        
    def download(self, *args, group=None, date, **kwargs):
        if not group: url = self.urlapi(*args, query='variables', filetype='json', date=date, **kwargs)   
        else: url = self.urlapi(*args, query=['groups', group], filetype='json', date=date, **kwargs)   
        jsondata = self.webreader(str(url), *args, method='get', datatype='json', **kwargs)
        return [USCensus_APIVariable(tag=key, group=values['group'], date=date, label=values['label']) for key, values in jsondata['variables'].items()]

    def getmatches(self, label, variables, method, **kwargs):
        return [variable for variable in variables if variable.match(label, method=method, **kwargs)]

    def generator(self, labels, variables):
        for label in labels:
            for method in ('strict', 'exact', 'over', 'under'):      
                matches = self.getmatches(label, variables, method)
                if matches: break
            for tolerance in range(self.__tolerance if not matches else 0): 
                matches = self.getmatches(label, variables, method='underdiff', tolerance=tolerance)
                if matches: break            
            if matches: 
                for item in matches: yield item
            else: raise ValueError(label)
                
    def __call__(self, *args, labels, date, **kwargs):
        labels = [tuple([item.format(date=str(date)) for item in label]) for label in labels]
        variables = self.download(*args, date=date, **kwargs)
        return [item for item in self.generator(labels, variables)]


class USCensus_Geography_WebQuery(object):
    def __init__(self, urlapi, webreader):
        self.__urlapi = urlapi
        self.__webreader = webreader
         
    @property
    def series(self): return self.__urlapi.series
    @property
    def survey(self): return self.__urlapi.survey   
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader     

    def download(self, *args, **kwargs):
        url = self.urlapi(*args, tags='NAME', **kwargs)    
        data = self.webreader(str(url), *args, method='get', datatype='json', **kwargs)
        return data

    def parser(self, data, *args, geography, **kwargs):
        apigeographys = [USCensus_APIGeography(geokey, geovalue) for geokey, geovalue in geography.items()]
        dataframe = dataframe_fromjson(data, header=0, forceframe=True)
        dataframe['NAME'] = dataframe['NAME'].apply(lambda x: x.split(', ')[-1])
        dataframe = dataframe.rename({item.apigeography:item.geography for item in apigeographys}, axis='columns') 
        return dataframe

    def execute(self, *args, **kwargs):
        data = self.download(*args, **kwargs)
        dataframe = self.parser(data, *args, **kwargs)
        return dataframe

    def generator(self, *args, geopaths, **kwargs):
        assert all([isinstance(geopath, Geopath) for geopath in geopaths])
        for geopath in _filterempty([*_aslist(geopaths), *_aslist(kwargs.get('geopath', None))]):
            geography = Geography()
            for geokey, geoname in geopath.items():
                geography = Geography(ODict([(key, value) for key, value in geography.items()] + [(geokey, Geography.allChar)]))
                geovalue = self.execute(*args, geography=geography, **kwargs).loc['NAME', geokey]
                geography = Geography(ODict([(key, geovalue) if key == geokey else (key, value) for key, value in geography.items()]))            
            yield geopath, geography
       
    def __call__(self, *args, **kwargs):
        geodata = [[str(geopath), str(geography)] for geopath, geography in self.generator(*args, **kwargs)]       
        return pd.DataFrame(geodata, columns=['geopath', 'geography'])










