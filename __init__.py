# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus WebAPI Objects
@author: Jack Kirby cook

"""

import os.path
import csv

from webdata.webreaders import WebReader
from webdata.webapi import WebRequestAPI, FTPDownloadAPI
from utilities.dataframes import dataframe_fromjson
from variables.geography import Geography

from uscensus.urlapi import USCensus_URLAPI

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_WebAPI', 'USCensus_FTPAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


_DIR = os.path.dirname(os.path.realpath(__file__))
_DATEFORMATS = {'geoseries':'%Y', 'yearseries':'%Y', 'timeseries':'%Y-%m'}
_WEBKEYS = ('series', 'survey', 'tags', 'preds', 'concepts')

_GEOFILE = 'geography.csv'
_GEOAPIKEYS = {}
_GEODIRNAMES = {}

with open(os.path.join(_DIR, _GEOFILE), mode='r') as infile:
    reader = csv.reader(infile)   
    for row in reader:      
        _GEOAPIKEYS[row[0]] = row[1]
        _GEODIRNAMES[row[0]] = row[2]

_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]


class USCensus_WebAPI(WebRequestAPI):
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
    def filename(self, *args, tableID, geography, date, estimate, **kwargs):
        return tableID.format(estimate=estimate, date=date, geoid=geography.geoid)

    # PROCESSORS
    def webreader(self, *args, tags, **kwargs):
        url = self.__urlapi(*args, tags=['NAME', *_aslist(tags)], **kwargs)  
        return self.__webreader(url)   

    def webdataparser(self, webdata, *args, **kwargs):  
        return dataframe_fromjson(webdata, header=0, forceframe=True)
    
    def webtableparser(self, webtable, *args, **kwargs):
        webtable = self.__renameheaders(webtable, *args, **kwargs)
        webtable = self.__consolidate(webtable, *args, **kwargs)
        webtable = self.__splitgeoname(webtable, *args, **kwargs)  
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
        apigeokeys = [_GEOAPIKEYS[key] for key in geography.keys()]        
        geofunction = lambda values: str(Geography(**{key:value for key, value in zip(geokeys, values)}))
        webtable['geography'] = webtable[apigeokeys].apply(geofunction, axis=1)
        return webtable.drop(apigeokeys, axis=1)

    def __splitgeoname(self, webtable, *args, geography, **kwargs):
        geokeys = list(geography.keys())
        geonamefunction = lambda value: ' & '.join(['='.join([key, name]) for key, name in zip(geokeys, value.split(', '))])
        webtable['name'] = webtable['NAME'].apply(geonamefunction)
        return webtable.drop('NAME', axis=1)

    # ENGINES
    def generator(self, *args, tableIDs, dates, **kwargs):
        for date in _aslist(dates): 
            for tableID, parms in self.tablequeue.items():
                date.setformat(_DATEFORMATS[parms['series']])
                yield {'tableID':tableID, 'date':date, **parms, **kwargs}
                
    def execute(self, *args, geography, date, estimate=5, **kwargs):
        return self.download(*args, geography=geography, date=date, estimate=estimate, **kwargs)


class USCensus_FTPAPI(FTPDownloadAPI):
    def __init__(self, *args, username='anonymous', password='anonymous', **kwargs): 
        super().__init__(*args, domain='ftp2.census.gov', filetype='zip', username=username, password=password, **kwargs)
    
    def __repr__(self): return '{}(username={}, password={}, repository={})'.format(self.__class__.__name__, self.username, self.password, self.repository)

    def serverpath(self, *args, date, geography, **kwargs): 
        return '/'.join(['geo','tiger','TIGER{date}'.format(date=date.year), _GEODIRNAMES[geography[-1].getkey(-1)]])
    
    def serverfilenames(self, *args, geography, date, **kwargs):
        items = ['tl_{year}_us_{forgeo}'.format(year=date.year, forgeo=_GEODIRNAMES[geography.getkey(-1)].lower())]
        if 'state' in geography.keys(): items.append('tl_{year}_{state}_{forgeo}'.format(state=geography.getvalue('state'), year=date.year, forgeo=_GEODIRNAMES[geography.getkey(-1)].lower()))
        return items
        
    ###############################################################################################################
    def clientfilename(self, *args, date, geography, **kwargs): 
        raise Exception('UNDER CONSTRUCTION')
    ###############################################################################################################
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    