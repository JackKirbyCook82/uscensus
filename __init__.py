# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus WebAPI Objects
@author: Jack Kirby cook

"""

import os.path
import io
import zipfile

from webdata.webreaders import WebReader
from webdata.webapi import WebAPI
from utilities.dataframes import dataframe_fromjson, geodataframe_fromdir
from variables.geography import Geography

from uscensus.urlapi import USCensus_URLAPI, USCensus_ShapeFile_URLAPI

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_WebAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


_DATEFORMATS = {'geoseries':'%Y', 'yearseries':'%Y', 'timeseries':'%Y-%m'}
_WEBKEYS = ('series', 'survey', 'tags', 'preds', 'concepts')

_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]


class USCensus_WebAPI(WebAPI):
    webdatatype = 'json'
    shapedatatype = 'zip'

    def __init__(self, apikey, *args, **kwargs):        
        self.__urlapi = USCensus_URLAPI(apikey, *args, **kwargs)
        self.__webreader = WebReader(self.webdatatype, *args, **kwargs)          
        self.__shapeurlapi = USCensus_ShapeFile_URLAPI(self.shapedatatype)
        self.__shapewebreader = WebReader(self.shapedatatype, *args, **kwargs)            
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
        webtable = self.__geoname(webtable, *args, **kwargs)  
        webtable = self.__geography(webtable, *args, **kwargs)
        webtable = self.__geoid(webtable, *args, **kwargs)
        return webtable
        
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
    
    def __geography(self, webtable, *args, geography, **kwargs):
        geokeys = list(geography.keys())
        geoapikeys = [self.__urlapi.geoapikey(key) for key in geography.keys()]        
        function = lambda values: str(Geography({key:value for key, value in zip(geokeys, values)}))
        webtable['geography'] = webtable[geoapikeys].apply(function, axis=1)
        return webtable.drop(geoapikeys, axis=1)
    
    def __geoname(self, webtable, *args, geography, **kwargs):
        geokeys = list(geography.keys())
        function = lambda value: ' & '.join(['='.join([key, name]) for key, name in zip(geokeys, value.split(', '))])
        webtable['geoname'] = webtable['NAME'].apply(function)
        return webtable.drop('NAME', axis=1)

    def __geoid(self, webtable, *args, geography, **kwargs):
        function = lambda value: str(Geography.fromstr(value).geoid)
        webtable['geoid'] = webtable['geography'].apply(function)
        return webtable

    def __shapegeography(self, shapetable, *args, geography, **kwargs):
        geokeys = list(geography.keys())
        shapeapikeys = [self.__shapeurlapi.shapeapikey(key) for key in geography.keys()]     
        function = lambda values: str(Geography({key:value for key, value in zip(geokeys, values)}))
        shapetable['geography'] = shapetable[shapeapikeys].apply(function, axis=1)
        return shapetable.drop(shapeapikeys, axis=1)                

    # ENGINES
    def generator(self, *args, tableIDs, dates, **kwargs):
        for date in _aslist(dates): 
            for tableID, parms in self.tablequeue.items():
                date.setformat(_DATEFORMATS[parms['series']])
                yield {'tableID':tableID, 'date':date, **parms, **kwargs}
                
    def execute(self, *args, geography, date, estimate=5, **kwargs):
        return self.download(*args, geography=geography, date=date, estimate=estimate, **kwargs)

    def getshapefile(self, *args, redownload=False, **kwargs):
        directory = self.directory(self.__shapeurlapi.filename(*args, **kwargs))
        if not os.path.exists(directory) or redownload: 
            url = self.__shapeurlapi(*args, **kwargs)
            webdata = self.__shapewebreader(url)
            content = zipfile.ZipFile(io.BytesIO(webdata))
            content.extractall(path=directory)

    def shapes(self, *args, geography, **kwargs):
        directory = self.directory(self.__shapeurlapi.filename(*args, geography=geography, **kwargs))
        shapetable = geodataframe_fromdir(directory)
        shapetable.columns = map(str.lower, shapetable.columns)  
        shapetable['geoid'] = shapetable['geoid'].apply(str)              
        shapetable = shapetable.loc[shapetable['geoid'].str.startswith(geography.geoid.replace('X', ''))].reset_index(drop=True)
        shapetable = self.__shapegeography(shapetable, *args, geography=geography, **kwargs)
        return shapetable[['geography', 'geoid', 'geometry']]
    
    
    
    
    
    
    
    