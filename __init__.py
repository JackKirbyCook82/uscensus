# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus WebAPI
@author: Jack Kirby cook

"""

from webdata.webreaders import WebReader
from webdata.webapi import WebAPI
from utilities.dataframes import dataframe_fromdata
from variables.geography import Geography

from uscensus.urlapi import USCensus_URLAPI

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_WebAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


_DATEFORMATS = {'geoseries':'{year:04.0f}', 'yearseries':'{year:04.0f}', 'timeseries':'{year:04.0f}-{month:02.0f}'}
_PARMSKEY = 'parms'
_WEBKEYS = ('series', 'survey', 'tags', 'preds', 'concepts')


_aslist = lambda items: [items] if not isinstance(items, (list, tuple)) else items


class USCensus_WebAPI(WebAPI):
    webdatatype = 'json'
    filedatatype = 'csv'

    def __init__(self, apikey, *args, **kwargs):        
        self.__urlapi = USCensus_URLAPI(apikey, *args, **kwargs)
        self.__webreader = WebReader(self.webdatatype, *args, **kwargs)        
        super().__init__(*args, **kwargs)

    def __repr__(self): return '{}(apikey={}, repository={})'.format(self.__class__.__name__, self.__urlapi.apikey, self.repository)
     
    @property
    def scopekeys(self): return [column for column in self.tables.columns if column not in set([self.parmskey, *self.tablekeys, *self.webkeys])]
    @property
    def webkeys(self): return list(_WEBKEYS)

    def filename(self, tableID, *args, series, geography, date, estimate=None, **kwargs):
        return '_'.join([tableID.format(estimate), geography.geoid, _DATEFORMATS[series].format(year=date.year, month=date.month), self.filedatatype])


    def webkwargs(self, tableID, *args, **kwargs): return {key:value for key, value in self.tables.loc[tableID].to_dict().items() if key in self.webkeys}       

    def webreader(self, *args, **kwargs):
        url = self.__urlapi(*args, **kwargs)  
        return self.__webreader(url)   

    def webdataparser(self, webdata, *args, **kwargs):  
        return dataframe_fromdata(self.webdatatype, webdata, header=0, forceframe=True)
    
    def webtableparser(self, webtable, *args, **kwargs):
        webtable = self.__renameheaders(webtable, *args, **kwargs)
        webtable = self.__consolidate(webtable, *args, **kwargs)
        return self.__combinegeography(webtable, *args, **kwargs)
        
    def webtablescope(self, webtable, *args, geography, estimate=None, date, **kwargs):
        webtable['date'] = str(date)
        for key, value in kwargs.items(): 
            if key not in webtable.columns: webtable[key] = value
        return webtable
    
    def __renameheaders(self, webtable, *args, tags, concepts, **kwargs):    
        assert len(tags) == len(concepts)
        return webtable.rename({tag:concept for tag, concept in zip(tags, concepts)} , axis='columns')      

    def __consolidate(self, webtable, *args, universe, headers, concepts, **kwargs):
        return webtable.melt(id_vars=[column  for column in webtable.columns if column not in _aslist(concepts)], var_name=headers, value_name=universe)
    
    def __combinegeography(self, webtable, *args, geography, **kwargs):
        geokeys = list(geography.keys())
        apigeokeys = [self.__urlapi.geography[key] for key in geography.keys()]        
        function = lambda values: str(Geography(**{key:value for key, value in zip(geokeys, values)}))
        webtable['geography'] = webtable[apigeokeys].apply(function, axis=1)
        return webtable.drop(apigeokeys, axis=1)

    
    
    
    
    