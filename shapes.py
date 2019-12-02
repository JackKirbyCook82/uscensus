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
__all__ = ['USCensus_Shape_URLAPI', 'USCensus_Shape_Downloader', 'USCensus_Shape_FileAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


class USCensus_Shape_URLAPI(URLAPI):
    def protocol(self, *args, **kwargs): return 'https'
    def domain(self, *args, **kwargs): return 'www2.census.gov'    
    
    def path(self, *args, shape, date, geography, **kwargs): 
        usgeography = USCensus_APIGeography(shape)
        shapedir = usgeography.shapedir    
        geoids = {'state':str(geography.getvalue('state')), 'county':str(geography.getvalue('state'))+str(geography.getvalue('county'))}
        shapefile = usgeography.shapefile.format(year=date, **geoids)
        return ['geo', 'tiger', 'TIGER{year}'.format(year=date), shapedir, shapefile]
    

class USCensus_Shape_Downloader(object):
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader
    @property
    def repository(self): return self.__repository    
    @property
    def date(self): return self.__date 
    
    def __repr__(self): return "{}(repository='{}', date='{}')".format(self.__class__.__name__, self.__repository, self.__date)  
    def __init__(self, repository, date, urlapi, webreader):
        self.__urlapi = urlapi
        self.__webreader = webreader
        self.__repository = repository   
        self.__date = str(date.year) if hasattr(date, 'year') else str(date)
        
    def directory(self, shape, *args, geography, **kwargs): 
        usgeography = USCensus_APIGeography(shape)
        geoids = {}
        try: geoids['state'] = geography.cut('state').geoid
        except: pass
        try: geoids['county'] = geography.cut('county').geoid
        except: pass
        directoryname = usgeography.shapefile.format(year=self.date, **geoids)
        return os.path.join(self.repository, directoryname)    
    
    def download(self, shape, *args, **kwargs):
        url = self.urlapi.query(*args, shape=shape, filetype='zip', **kwargs)
        shapezipfile = self.webreader(url, *args, method='get', **kwargs)
        shapecontent = zipfile.ZipFile(io.BytesIO(shapezipfile))
        shapecontent.extractall(path=self.directory(*args, shape=shape, **kwargs))    
    
    def __call__(self, shape, *args, geography, **kwargs):
        self.download(shape, *args, geography=geography, **kwargs)  

    
class USCensus_Shape_FileAPI(object):   
    @property
    def repository(self): return self.__repository    
    @property
    def date(self): return self.__date
    
    def __repr__(self): return "{}(repository='{}', date='{}')".format(self.__class__.__name__, self.__repository, self.__date)  
    def __init__(self, repository, date): self.__repository, self.__date = repository, str(date.year) if hasattr(date, 'year') else str(date)       

    def downloaded(self, shape, *args, **kwargs): return os.path.exists(self.directory(shape, *args, **kwargs))
    def directory(self, shape, *args, geography, **kwargs): 
        usgeography = USCensus_APIGeography(shape)
        geoids = {}
        try: geoids['state'] = geography.cut('state').geoid
        except: pass
        try: geoids['county'] = geography.cut('county').geoid
        except: pass
        directoryname = usgeography.shapefile.format(year=self.date, **geoids)
        return os.path.join(self.repository, directoryname)       

    def __call__(self, *args, geography, **kwargs): 
        geotable = self.geotable(*args, geography=geography, **kwargs) 
        basetable = self.basetable(*args, geography=geography, **kwargs)   
        return geotable, basetable
    
    def __getitem__(self, shape):        
        def wrapper(*args, geography, **kwargs): 
            return self.itemtable(shape, *args, geography=geography, **geography.asdict(), **kwargs)
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
        



        

    
















