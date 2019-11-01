# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 2019
@name:   USCensus Shapes
@author: Jack Kirby Cook

"""

import os.path
import io
import zipfile

from utilities.dataframes import geodataframe_fromdir
from webdata.url import URLAPI

from uscensus.urlapi import USCensus_APIGeography

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Shape_URLAPI', 'USCensus_Shape_Downloader', 'USCensus_ShapeAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


class USCensus_Shape_URLAPI(URLAPI):
    def __repr__(self): return '{}()'.format(self.__class__.__name__)  
    
    def protocol(self, *args, **kwargs): return 'https'
    def domain(self, *args, **kwargs): return 'www2.census.gov'    
    
    def path(self, *args, shape, date, geography, **kwargs): 
        usgeography = USCensus_APIGeography(shape)
        shapedir = usgeography.shapedir
        try: year = str(date.year)
        except: year = str(date)        
        geoids = {'state':str(geography.getvalue('state')), 'county':str(geography.getvalue('state'))+str(geography.getvalue('county'))}
        shapefile = usgeography.shapefile.format(year=year, **geoids)
        return ['geo', 'tiger', 'TIGER{year}'.format(year=year), shapedir, shapefile]
    

class USCensus_Shape_Downloader(object):
    def __repr__(self): return '{}(repository={})'.format(self.__class__.__name__, self.__repository)  
    def __init__(self, repository, urlapi, webreader):
        self.__urlapi = urlapi
        self.__webreader = webreader
        self.__repository = repository       
        
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader
    @property
    def repository(self): return self.__repository    

    def directory(self, shape, *args, date, state='', county='', **kwargs): 
        usgeography = USCensus_APIGeography(shape)
        try: year = str(date.year)
        except: year = str(date)
        geoids = {}
        if state: geoids['state'] =  str(state)
        if state and county: geoids['county'] = str(state) + str(county)
        directoryname = usgeography.shapefile.format(year=year, **geoids)
        return os.path.join(self.repository, directoryname)    
    
    def download(self, shape, *args, **kwargs):
        url = self.urlapi.query(*args, shape=shape, filetype='zip', **kwargs)
        shapezipfile = self.webreader(url, *args, **kwargs)
        shapecontent = zipfile.ZipFile(io.BytesIO(shapezipfile))
        shapecontent.extractall(path=self.directory(*args, shape=shape, **kwargs))    
    
    def __call__(self, shape, *args, date, geography, **kwargs):
        self.download(shape, *args, date=date, **geography.asdict(), geography=geography, **kwargs)  

    
class USCensus_ShapeAPI(object):   
    @property
    def repository(self): return self.__repository         
    def __repr__(self): return '{}(repository={})'.format(self.__class__.__name__, self.__repository)  
    def __init__(self, repository): self.__repository = repository       

    def downloaded(self, shape, *args, **kwargs): return os.path.exists(self.directory(shape, *args, **kwargs))
    def directory(self, shape, *args, date, state='', county='', **kwargs): 
        usgeography = USCensus_APIGeography(shape)
        try: year = str(date.year)
        except: year = str(date)
        geoids = {}
        if state: geoids['state'] =  str(state)
        if state and county: geoids['county'] = str(state) + str(county)
        directoryname = usgeography.shapefile.format(year=year, **geoids)
        return os.path.join(self.repository, directoryname)       

    def __call__(self, *args, date, geography, **kwargs): 
        geotable = self.geotable(*args, date=date, geography=geography, **geography.asdict(), **kwargs) 
        basetable = self.basetable(*args, date=date, geography=geography, **geography.asdict(), **kwargs)   
        return geotable, basetable
    
    def __getitem__(self, shape):        
        def wrapper(*args, date, geography, **kwargs): 
            return self.itemtable(shape, *args, date=date, geography=geography, **geography.asdict(), **kwargs)
        return wrapper

    def load(self, shape, *args, **kwargs):
        dataframe = geodataframe_fromdir(self.directory(shape, *args, **kwargs))   
        return dataframe

    def geotable(self, *args, geography, **kwargs):
        shape = geography.getkey(-1)
        assert self.downloaded(shape, *args, geography=geography, **kwargs) 
        geodataframe = self.load(shape, *args, geography=geography, **kwargs)
        geodataframe = self.__parser(shape, geodataframe, *args, geography=geography, **kwargs)        
        return geodataframe
    
    def basetable(self, *args, geography, **kwargs):
        shape = geography.getkey(-2)
        assert self.downloaded(shape, *args, geography=geography, **kwargs)
        basedataframe = self.load(shape, *args, geography=geography[:-1], **kwargs)
        basedataframe = self.__parser(shape, basedataframe, *args, geography=geography[:-1], **kwargs)
        return basedataframe

    def itemtable(self, shape, *args, **kwargs):
        assert self.downloaded(shape, *args, **kwargs)
        shapedataframe = self.load(shape, *args, **kwargs)
        shapedataframe = self.__itemparser(shape, shapedataframe, *args, **kwargs)
        return shapedataframe

    def __parser(self, shape, geodataframe, *args, geography, **kwargs):
        GeographyClass = geography.__class__
        usgeographys = [USCensus_APIGeography(geokey, geovalue) for geokey, geovalue in geography.items()]           
        function = lambda values: str(GeographyClass({key:value for key, value in zip(geography.keys(), values)}))
        geodataframe.columns = map(str.lower, geodataframe.columns)          
        geodataframe['geography'] = geodataframe[[usgeography.shapegeography for usgeography in usgeographys]].apply(function, axis=1)

        geodataframe = geodataframe[['geography', 'geometry']]
        mask = geodataframe['geography'].apply(lambda x: GeographyClass.fromstr(x) in geography)
        geodataframe = geodataframe[mask]
        geodataframe = geodataframe.set_index('geography', drop=True)      
        return geodataframe
    
    def __itemparser(self, shape, shapedataframe, *args, **kwargs):
        return shapedataframe['geometry']
        



        

    
















