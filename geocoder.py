# -*- coding: utf-8 -*-
"""
Created on Fri Nov 30 2019
@name:   USCensus Geocoding
@author: Jack Kirby Cook

"""

import math
from collections import OrderedDict as ODict

from webdata.url import URLAPI
from utilities.dispatchers import clskey_singledispatcher as keydispatcher

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_GeoCoder_URLAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


_SPACEPROXY = '+'
_COMMAPROXY = '%2C'
_SEARCHFOR = {'address':'locations', 'geography':'geographies'}
_SEARCHBY = {'address':'onelineaddress', 'location':'address', 'coordinates':'coordinates', 'file':'addressbatch'}
_SEARCHTYPE = {'csv', 'json', 'html'}

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
    def __init__(self, version, vintage, datatype): self.__version, self.__vintage = version, vintage
    def protocol(self, *args, **kwargs): return 'https'
    def domain(self, *args, **kwargs): return 'geocoding.geo.census.gov'  
    
    @property
    def version(self): return ODict(_version(self.__version))
    @property
    def vintage(self): return ODict(_vintage(self.__vintage))
    
    def path(self, *args, searchfor, searchby, searchtype, **kwargs): 
        assert all([searchfor in _SEARCHFOR, searchby in _SEARCHBY, searchtype in _SEARCHTYPE])
        return ['geocoder', _SEARCHFOR[searchfor], _SEARCHBY[searchby]]
    def parms(self, *args, searchfor, searchby, searchtype, **kwargs): 
        if searchtype == 'cvs': return {}
        parms = self.searchbyparms(searchby, *args, **kwargs) + _version(self.__version) + _format(self.__datatype)
        if searchfor == 'geography': return ODict(parms + _vintage(self.__vintage)) 
        else: return ODict(parms) 
    
    @keydispatcher
    def searchbyparms(self, searchby, *args, **kwargs): raise KeyError(searchby) 
    @searchbyparms.register('address')
    def searchbyparms_address(self, *args, address, **kwargs): 
        return ODict(_address(address))
    @searchbyparms.register('location')
    def searchbyparms_location(self, *args, street, city, state, zipcode, **kwargs): 
        return ODict(_street(street) + _city(city) + _state(state) + _zipcode(zipcode))
    @searchbyparms.register('coordinates')
    def searchbyparms_coordinate(self, *args, x, y, **kwargs): 
        return ODict(_coordinate('x', x) + _coordinate('y', y))    

    
class USCensus_GeoCoder_WebAPI(object):
    pass

    
class USCensus_GeoCoder_Downloader(object):
    pass    

            
class USCensus_GeoCoder_FileAPI(object):
    pass


    
    
    
    
    
    
    
    