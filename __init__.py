# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus WebAPI Object
@author: Jack Kirby cook

"""

from webdata.webreaders import WebReader
from webdata.webapi import WebAPI
from utilities.dataframes import dataframe_fromdata
from variables.geography import Geography
import pandas as pd

from uscensus.urlapi import USCensus_URLAPI

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_WebAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


_DATEFORMATS = {'geoseries':'{year:04.0f}', 'yearseries':'{year:04.0f}', 'timeseries':'{year:04.0f}-{month:02.0f}'}
_WEBKEYS = ('series', 'survey', 'tags', 'preds', 'concepts')

_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]


class USCensus_WebAPI(WebAPI):
    webdatatype = 'json'

    def __init__(self, apikey, *args, **kwargs):        
        self.__urlapi = USCensus_URLAPI(apikey, *args, **kwargs)
        self.__webreader = WebReader(self.webdatatype, *args, **kwargs)      
        super().__init__(*args, **kwargs)

    def __repr__(self): return '{}(apikey={}, repository={})'.format(self.__class__.__name__, self.__urlapi.apikey, self.repository)
     
    # KEYS
    @property
    def scopekeys(self): return [column for column in self.tablesdata.columns if column not in set([*self.tablekeys, *self.webkeys])]
    @property
    def webkeys(self): return list(_WEBKEYS)

    # FILES
    def filename(self, *args, tablesID, geography, dates, estimate=5, **kwargs):
        return '_'.join([str(tablesID).format(estimate), geography.geoid, *[str(date) for date in _aslist(dates)]])

    # PROCESSORS
    def webreader(self, *args, tags, **kwargs):
        url = self.__urlapi(*args, tags=['NAME', *tags], **kwargs)  
        return self.__webreader(url)   

    def webdataparser(self, webdata, *args, **kwargs):  
        return dataframe_fromdata(self.webdatatype, webdata, header=0, forceframe=True)
    
    def webtableparser(self, webtable, *args, **kwargs):
        webtable = self.__renameheaders(webtable, *args, **kwargs)
        webtable = self.__consolidate(webtable, *args, **kwargs)
        return self.__combinegeography(webtable, *args, **kwargs)
        
    def webtablescope(self, webtable, *args, geography, estimate, date, **kwargs):
        webtable['date'] = str(date)
        scopekwargs = {key:kwargs[key] for key in self.scopekeys if key in kwargs.keys()}
        for key, value in scopekwargs.items(): 
            if key not in webtable.columns: webtable[key] = value
        return webtable
    
    # UNIQUE PROCESSORS
    def __renameheaders(self, webtable, *args, tags, concepts, **kwargs):    
        assert len(tags) == len(concepts)
        return webtable.rename({tag:concept for tag, concept in zip(tags, concepts)} , axis='columns')      

    def __consolidate(self, webtable, *args, universe, headers, concepts, **kwargs):
        return webtable.melt(id_vars=[column  for column in webtable.columns if column not in _aslist(concepts)], var_name=headers, value_name=universe)
    
    def __combinegeography(self, webtable, *args, geography, **kwargs):
        geokeys = list(geography.keys())
        apigeokeys = [self.__urlapi.geography[key] for key in geography.keys()]        
        geofunction = lambda values: str(Geography(**{key:value for key, value in zip(geokeys, values)}))
        geonamefunction = lambda value: ' & '.join(['='.join([key, name]) for key, name in zip(geokeys, value.split(', '))])
        webtable['geography'] = webtable[apigeokeys].apply(geofunction, axis=1)
        webtable['geographyname'] = webtable['NAME'].apply(geonamefunction)
        return webtable.drop(['NAME', *apigeokeys], axis=1)

    # ENGINES
    def execute(self, *args, geography, dates, estimate=5, **kwargs):
        tables = {}
        for date in _aslist(dates):
            for tableID, values in self.tablequeue.items():
                tables[(date, tableID)] = self.download(*args, geography=geography, date=date, estimate=estimate, **values, **kwargs)
        return pd.concat(tables.values(), axis=0)












    