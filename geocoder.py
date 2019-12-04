# -*- coding: utf-8 -*-
"""
Created on Tues Dec 3 2019
@name:   USCensus WebAPI Object
@author: Jack Kirby Cook

"""

import csv
import math
import tempfile
import pandas as pd
from collections import OrderedDict as ODict

from webdata.url import URLAPI
from webdata.webapi import WebAPI
from utilities.dispatchers import clskey_singledispatcher as keydispatcher
from utilities.dataframes import dataframe_fromjson, dataframe_fromcsv

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Geocoder_WebAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


_SPACEPROXY = '+'
_COMMAPROXY = '%2C'
_FILENAME = '{addressesID}.csv'
_APIHEADERS = ODict([('addressID', 'Unique ID'), ('street', 'Street address'), ('city', 'City'), ('state','State' ), ('zipcode', 'ZIP')])
_OUTPUTHEADERS = ODict([('state', 'STATE'), ('county', 'COUNTY'), ('tract', 'TRACT')])

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

_aslist = lambda items: [items] if not isinstance(items, (list, tuple)) else list(items)
_filterempty = lambda items: [item for item in _aslist(items) if item]


class USCensus_Geocoder_URLAPI(URLAPI):
    def __repr__(self): return "{}(version='{}', vintage='{}')".format(self.__class__.__name__, self.__version, self.__vintage)    
    def __init__(self, version, vintage): self.__version, self.__vintage = version, vintage
    
    @property
    def version(self): return ODict(_version(self.__version))
    @property
    def vintage(self): return ODict(_vintage(self.__vintage))
    
    def protocol(self, *args, **kwargs): return 'https'
    def domain(self, *args, **kwargs): return 'geocoding.geo.census.gov'      
    def path(self, *args, batch=False, **kwargs): return ['geocoder', 'geographies', 'addressbatch' if batch else 'address']
    def parms(self, *args, batch=False, **kwargs): 
        if batch: return {}
        address = kwargs['address']
        return ODict(_street(address.street) + _city(address.city) + _state(address.state) + _zipcode(address.zipcode) + _version(self.__version) + _vintage(self.__vintage) + _format('json'))


class USCensus_Geocoder_WebAPI(WebAPI):
    def __init__(self, repository, urlapi, webreader, batchthreshold=10, batchsplit=10000, saving=False):
        self.__urlapi = urlapi
        self.__webreader = webreader
        self.__batchthreshold = batchthreshold
        self.__batchsplit = batchsplit
        super().__init__('USCensusGeocoder', repository=repository, saving=saving)
        
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader
            
    def filename(self, *args, addressesID, **kwargs): return _FILENAME.format(addressesID=addressesID) 
    
    @keydispatcher
    def download(self, method, *args, **kwargs): raise KeyError(method)
    @keydispatcher
    def parser(self, method, data, *args, **kwargs): raise KeyError(method)    
           
    @download.register('request')
    def download_request(self, *args, address, **kwargs):
        url = self.urlapi(*args, address=address, batch=False, **kwargs)
        data = self.webreader(url, *args, method='get', **kwargs)
        return data
    
    @download.register('file')
    def download_file(self, *args, addressesID, addresses, **kwargs):       
        with tempfile.NamedTemporaryFile(suffix='.csv') as tempcsv:
            writer = csv.writer(tempcsv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(list(_APIHEADERS.values()))
            for count, address in enumerate(addresses, 0): 
                writer.writerow([addressesID + str(count), *address.values()[1:]])
            url = self.urlapi(*args, batch=True, **kwargs)
            data = self.webreader(url, *args, method='post', data={**self.urlapi.version, **self.urlapi.vintage}, files={'addressFile':tempcsv}, **kwargs) 
        return data
    
    @parser.register('request')
    def parser_request(self, data, *args, **kwargs): 
        data = data['geographies']['Census Blocks']
        dataframe = dataframe_fromjson(data, header=None, forceframe=True)[_OUTPUTHEADERS.values()]
        dataframe = dataframe.rename({value:key for key, value in _OUTPUTHEADERS.items()})
        return dataframe
    
    @parser.register('file')
    def parser_file(self, data, *args, **kwargs): 
        dataframe = dataframe_fromcsv(data, header=0, forceframe=True)[_OUTPUTHEADERS.values()]
        dataframe = dataframe.rename({value:key for key, value in _OUTPUTHEADERS.items()})
        return dataframe
           
    def execute(self, method, *args, **kwargs):
        data = self.download(method, *args, **kwargs)
        dataframe = self.parser(method, data, *args, **kwargs)
        return dataframe
    
    def generator(self, *args, addresses={}, **kwargs):
        for addressesID, addresses in addresses.items():
            try: dataframe = self.load(*args, addressesID=addressesID, **kwargs)
            except FileNotFoundError: 
                if len(addresses) <= self.__batchthreshold: dataframe = pd.concat([self.download('request', *args, addressesID=addressesID, address=address, **kwargs) for address in addresses], axis=0)
                elif len(addresses) <= self.__batchsplit: dataframe = self.execute('file', *args, addressesID=addressesID, addresses=addresses, **kwargs) 
                else: dataframe = pd.concat([self.download('file', *args, addressesID=addressesID, addresses=addresses[i:i+self.__batchsplit], **kwargs) for i in range(0, len(addresses), self.__batchsplit)], axis=0)                   
                if self.saving: self.save(dataframe, *args, addressesID=addressesID, **kwargs)
            yield dataframe                    
            






    









