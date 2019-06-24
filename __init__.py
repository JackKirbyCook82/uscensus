# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus WebAPI Objects
@author: Jack Kirby cook

"""

import os.path
import pandas as pd
import csv

from webdata.webreaders import WebReader
from webdata.webapi import WebRequestAPI, FTPDownloadAPI
from utilities.dataframes import dataframe_fromjson
from variables.geography import Geography
from variables.date import Date

from uscensus.urlapi import USCensus_URLAPI

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_WebAPI', 'USCensus_Shapefiles']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


_DIR = os.path.dirname(os.path.realpath(__file__))
_DATEFORMATS = {'geoseries':'%Y', 'yearseries':'%Y', 'timeseries':'%Y-%m'}
_WEBKEYS = ('series', 'survey', 'tags', 'preds', 'concepts')

_GEOTABLEID = 'acs5detail_geo'
_GEOPARMS = dict(series='geoseries', survey='acs-detail', tags=['NAME'], date=Date(year=2017), estimate=5, preds={})
_GEODOMAIN = 'ftp2.census.gov'
_GEOPATH = ['geo','tiger','TIGER{}']
_GEOFILE = 'geography.csv'
_GEOFTPFILENAME = 'tl_{year}_{geoid}_{geodir}'
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
    def filename(self, *args, tablesID, geography, dates, estimate=5, **kwargs):
        series = list(set([parms['series'] for parms in self.tablequeue.values()]))
        assert len(series) == 1
        for date in _aslist(dates): date.setformat(_DATEFORMATS[series[0]])
        return '_'.join([str(tablesID).format(estimate), geography.geoid, *[str(date) for date in dates]])

    # PROCESSORS
    def webreader(self, *args, tags, **kwargs):
        url = self.__urlapi(*args, tags=tags, **kwargs)  
        return self.__webreader(url)   

    def webdataparser(self, webdata, *args, **kwargs):  
        return dataframe_fromjson(webdata, header=0, forceframe=True)
    
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
    def execute(self, *args, geography, dates, estimate=5, **kwargs):
        tables = {}
        for date in _aslist(dates):
            for tableID, parms in self.tablequeue.items():
                date.setformat(_DATEFORMATS[parms['series']])
                tables[(str(date), tableID)] = self.download(*args, geography=geography, date=date, estimate=estimate, **parms, **kwargs)
        return pd.concat(tables.values(), axis=0)

#########################################################################################################################
# (1): LOAD or DOWNLOAD DF[geo, name]
# (2): FTP DOWNLOAD SF(geo) & BaseSF(geo)
# (3): READ SF(geo) to GeoDF[shape]
# (4): COMBINE DF[geo, name] with GeoDF[shape]
# (5): RETURN GeoDF[geo, name, shape] & baseSF(geo)

    def geography(self, *args, geography, **kwargs):   
        try: return self.load(*args, tablesID=_GEOTABLEID, dates=[_GEOPARMS['date']], geography=geography, **kwargs)
        except FileNotFoundError:
            webdata = self.webreader(*args, geography=geography, **_GEOPARMS, **kwargs)
            webtable = self.webdataparser(webdata, *args, **kwargs)
            webtable = self.__combinegeography(webtable, *args, geography=geography, **kwargs)
            webtable = self.__splitgeoname(webtable, *args, geography=geography, **kwargs)            
            self.save(webtable, *args, tablesID=_GEOTABLEID, dates=[_GEOPARMS['date']], geography=geography, **kwargs)
            return webtable
#########################################################################################################################


class USCensus_ShapefileAPI(FTPDownloadAPI):
    def __init__(self, *args, year=2018, username='anonymous', password='anonymous', **kwargs): 
        domain, path = _GEODOMAIN, [item.format(year) for item in _GEOPATH]
        self.__year = year
        super().__init__(*args, domain=domain, path=path, filetype='zip', username=username, password=password, **kwargs)
    
    def __repr__(self): return '{}(username={}, password={}, year={}, repository={})'.format(self.__class__.__name__, self.username, self.password, str(self.__year), self.repository)
    
    def pathnames(self, *args, geography, **kwargs):
        yield _GEODIRNAMES[geography[-1].getkey(-1)]
    
    def filenames(self, *args, geography, **kwargs): 
        geodir = _GEODIRNAMES[geography[-1].getkey(-1)]
        for i in reversed(range(len(geography))): 
            geoid = geography[slice(0, i)].geoid
            yield _GEOFTPFILENAME.format(year=self.__year, geoid=geoid if geoid else 'us', geodir=geodir.lower())




    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    