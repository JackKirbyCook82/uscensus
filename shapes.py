# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 2019
@name:   USCensus URLAPI & WEBAPI Objects
@author: Jack Kirby Cook

"""

import os.path
import io
import zipfile

from webdata.url import URL, Protocol, Domain, ZIPPath
from utilities.dataframes import geodataframe_fromdir

from uscensus.urlapi import USCensus_APIGeography

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Shape_URLAPI', 'USCensus_Shape_Downloader', 'USCensus_Shape_FileAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


class USCensus_Shape_URLAPI(object):
    def __call__(self, *args, **kwargs): 
        return URL(self.protocol(*args, **kwargs), self.domain(*args, **kwargs), path=self.path(*args, **kwargs))
    
    def protocol(self, *args, **kwargs): return Protocol('https')
    def domain(self, *args, **kwargs): return Domain('www2.census.gov')
    
    def path(self, *args, shapeID, vintage, geography, **kwargs): 
        usgeography = USCensus_APIGeography(shapeID)
        shapedir = usgeography.shapedir    
        geoids = {key:geography.values()[0:i+1] for key, i in zip(geography.keys(), range(len(geography.values())))}
        shapefile = usgeography.shapefile.format(year=vintage, **geoids)
        return ZIPPath('geo', 'tiger', 'TIGER{year}'.format(year=vintage), shapedir, shapefile)
    

class USCensus_Shape_Downloader(object):
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader
    @property
    def repository(self): return self.__repository    
    @property
    def vintage(self): return self.__vintage 
    
    def __repr__(self): return "{}(repository='{}', vintage='{}')".format(self.__class__.__name__, self.repository, self.vintage)  
    def __init__(self, repository, vintage, urlapi, webreader):
        self.__urlapi = urlapi
        self.__webreader = webreader
        self.__repository = repository   
        self.__vintage = str(vintage.year) if hasattr(vintage, 'year') else str(vintage)
        
    def directory(self, *args, shapeID, geography, **kwargs): 
        usgeography = USCensus_APIGeography(shapeID)
        geoids = {key:geography.values()[0:i+1] for key, i in zip(geography.keys(), range(len(geography.values())))}
        directoryname = usgeography.shapefile.format(year=self.vintage, **geoids)
        return os.path.join(self.repository, directoryname)    
    
    def __call__(self, *args, **kwargs): self.download(*args, **kwargs)      
    def download(self, *args, **kwargs):
        url = self.urlapi(*args, **kwargs)
        shapezipfile = self.webreader(url, *args, method='get', datatype='zip', **kwargs)
        shapecontent = zipfile.ZipFile(io.BytesIO(shapezipfile))
        shapecontent.extractall(path=self.directory(*args, **kwargs))    
    
    
class USCensus_Shape_FileAPI(object):   
    @property
    def repository(self): return self.__repository    
    @property
    def vintage(self): return self.__vintage
    
    def __repr__(self): return "{}(repository='{}', vintage='{}')".format(self.__class__.__name__, self.repository, self.vintage)  
    def __init__(self, repository, vintage): self.__repository, self.__vintage = repository, str(vintage.year) if hasattr(vintage, 'year') else str(vintage)       

    def downloaded(self, *args, **kwargs): return os.path.exists(self.directory(*args, **kwargs))
    def load(self, *args, **kwargs): return geodataframe_fromdir(self.directory(*args, **kwargs))   
    def directory(self, *args, shapeID, geography, **kwargs): 
        usgeography = USCensus_APIGeography(shapeID)
        geoids = {key:geography.values()[0:i+1] for key, i in zip(geography.keys(), range(len(geography.values())))}
        directoryname = usgeography.shapefile.format(year=self.vintage, **geoids)
        return os.path.join(self.repository, directoryname)       

    def __call__(self, *args, shapeID, geography, **kwargs): 
        assert self.downloaded(*args, shapeID=shapeID, geography=geography, **kwargs)
        geodataframe = self.load(*args, shapeID=shapeID, geography=geography, **kwargs)
        if shapeID in geography.keys(): geodataframe = self.__geographyparser(*args, shapeID=shapeID, geography=geography, **kwargs)
        else: geodataframe = self.__landmarkparser(*args, shapeID=shapeID, geography=geography, **kwargs)
        return geodataframe
    
    def __getitem__(self, shapeID):        
        def wrapper(*args, geography, **kwargs): return self(*args, shapeID=shapeID, geography=geography, **kwargs)
        return wrapper

    def __geographyparser(self, geodataframe, *args, geography, **kwargs):
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
    
    def __landmarkparser(self, shapedataframe, *args, **kwargs):
        return shapedataframe['geometry']
        



        

    
















