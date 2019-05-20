# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus WebAPI
@author: Jack Kirby cook

"""

from webdata.webreaders import WebReader
from webdata.webapi import WebAPI

from uscensus.urlapi import USCensus_URLAPI

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_WebAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


_DATEFORMATS = {'geoseries':'{year:04.0f}', 'yearseries':'{year:04.0f}', 'timeseries':'{year:04.0f}-{month:02.0f}'}


class USCensus_WebAPI(WebAPI):
    webdatatype = 'json'

    def __init__(self, apikey, *args, **kwargs):        
        self.__urlapi = USCensus_URLAPI(apikey, *args, **kwargs)
        self.__webreader = WebReader(self.webdatatype, *args, **kwargs)        
        super().__init__(*args, **kwargs)

    def __repr__(self): return '{}(apikey={}, repository={})'.format(self.__class__.__name__, self.__urlapi.apikey, self.repository)
    
    @property
    def webkeys(self): return ['series', 'survey', 'tags', 'preds']
    @property
    def headerkeys(self): return ['tags', 'concepts']
    @property
    def parmskey(self): return 'parms'
    @property
    def scopekeys(self): return [column for column in self.tables.columns if column not in set([self.parmskey, *self.tablekeys, *self.webkeys, *self.headerkeys])]

    def filename(self, tableID, *args, series, geography, date, estimate=None, **kwargs):
        return '_'.join([tableID.format(estimate), geography.geoid, _DATEFORMATS[series].format(year=date.year, month=date.month), 'csv'])
   
    def webparms(self, tableID, *args, **kwargs): return {key:kwargs[key] for key in self.tables.loc[ tableID, self.parmskey]}  
    def webkwargs(self, tableID, *args, **kwargs): return {key:value for key, value in self.tables.loc[tableID].to_dict().items() if key in self.webkeys}       

    def webreader(self, *args, **kwargs):
        url = self.__urlapi(*args, **kwargs)  
        webdata = self.__webreader(url)
        return webdata   

    def webdataparser(self, webdata, *args, **kwargs):  
        pass
    
    
    
    
    