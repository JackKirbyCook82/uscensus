# -*- coding: utf-8 -*-
"""
Created on Fri Nov 30 2019
@name:   USCensus Geocoding
@author: Jack Kirby Cook

"""

import math
from collections import OrderedDict as ODict

from webdata.url import URLAPI
from webdata.webapi import WebAPI
from utilities.dataframes import dataframe_fromcsv

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_GeoCoder_URLAPI', 'USCensus_GeoCoder_WebAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


_SPACEPROXY = '+'
_COMMAPROXY = '%2C'
_FILENAME = 'address_geography_{geoid}'

_version = lambda version: [('benchmark', version)]
_vintage = lambda vintage: [('vintage', vintage)]
_datatype = lambda datatype: [('format', datatype)]
_address = lambda address: [('address', address.replace(' ', _SPACEPROXY).replace(',', _COMMAPROXY))]
_street = lambda street: [('street', street.replace(' ', _SPACEPROXY))]
_city = lambda city: [('city', city)]
_state = lambda state: [('state', state)]
_zipcode = lambda zipcode: [('zipcode', zipcode)]
_format = lambda datatype: [('format', datatype)]
_coordinate = lambda direction, value: [(direction, '{}/{}'.format(*math.modf(value)))]


class USCensus_GeoCoder_URLAPI(URLAPI):
    def __repr__(self): return "{}(version='{}', vintage='{}')".format(self.__class__.__name__, self.__version, self.__vintage)    
    def __init__(self, version, vintage): self.__version, self.__vintage = version, vintage
    
    @property
    def version(self): return ODict(_version(self.__version))
    @property
    def vintage(self): return ODict(_vintage(self.__vintage))
    
    def protocol(self, *args, **kwargs): return 'https'
    def domain(self, *args, **kwargs): return 'geocoding.geo.census.gov'      
    def path(self, *args, **kwargs): return ['geocoder', 'geographies', 'addressbatch']
    def parms(self, *args, **kwargs): return {}

    
class USCensus_GeoCoder_WebAPI(WebAPI):
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader
    @property
    def repository(self): return self.__repository      

    def __init__(self, repository, urlapi, webreader, saving=False):
        self.__urlapi = urlapi
        self.__webreader = webreader
        super().__init__('USCensusGeoCoder', repository=repository, saving=saving)
    
    def filename(self, *args, geography, **kwargs): return _FILENAME.format(geoid=geography.geoid)
    def generator(self, *args, **kwargs): yield self.download(*args, **kwargs)
    
    def download(self, *args, file, **kwargs):
        url = self.urlapi(*args, **kwargs)   
        data = ODict(self.urlapi.version + self.urlapi.vintage)
        files = {'addressFile':open(file, 'rb')}
        data = self.webreader(str(url), *args, method='post', data=data, files=files, **kwargs)
        dataframe = dataframe_fromcsv(data, header=0, forceframe=True)
        return dataframe
    
    
            

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    