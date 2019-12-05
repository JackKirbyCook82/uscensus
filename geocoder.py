# -*- coding: utf-8 -*-
"""
Created on Tues Dec 3 2019
@name:   USCensus URLAPI & WebAPI Objects
@author: Jack Kirby Cook

"""

import csv
import math
import tempfile
import pandas as pd
from collections import OrderedDict as ODict

from webdata.url import URL, Protocol, Domain, Path, Parms
from utilities.dispatchers import clskey_singledispatcher as keydispatcher
from utilities.dataframes import dataframe_fromjson, dataframe_fromcsv

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Geocoder_URLAPI', 'USCensus_Geocoder_WebAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


_SPACEPROXY = '+'
_COMMAPROXY = '%2C'
_APIHEADER = ('Unique ID', 'Street address', 'City', 'State', 'ZIP')
_ADDRESSHEADER = ('street', 'city', 'state', 'zipcode')
_ADDRESSFORMAT = '{street}, {city}, {state} {zipcode}'
_GEOGRAPHYHEADER = ('state', 'county', 'tract')

_version = lambda version: Parms(benchmark=version)
_vintage = lambda vintage: Parms(vintage=vintage)
_datatype = lambda datatype: Parms(**{'format':datatype})
_address = lambda address: Parms(address=address.replace(' ', _SPACEPROXY).replace(',', _COMMAPROXY))
_street = lambda street: Parms(street=street.replace(' ', _SPACEPROXY))
_city = lambda city: Parms(city=city)
_state = lambda state: Parms(state=state)
_zipcode = lambda zipcode: Parms(zipcode=zipcode)
_coordinate = lambda direction, value: Parms(direction='{}/{}'.format(*math.modf(value)))

_aslist = lambda items: [items] if not isinstance(items, (list, tuple)) else list(items)
_filterempty = lambda items: [item for item in _aslist(items) if item]


class USCensus_Geocoder_URLAPI(object):
    def __repr__(self): return "{}(version='{}', vintage='{}')".format(self.__class__.__name__, self.__version, self.__vintage)    
    def __init__(self, version, vintage): self.__version, self.__vintage = version, vintage
    
    @property
    def version(self): return self.__version
    @property
    def vintage(self): return self.__vintage
    
    def __call__(self, *args, query, **kwargs):
        assert query in ('location', 'geographies', 'addressbatch')
        if query == 'addressbatch': return URL(self.protocol(*args, **kwargs), self.domain(*args, **kwargs), self.path(*args, query=query, **kwargs))
        else: return URL(self.protocol(*args, **kwargs), self.domain(*args, **kwargs), self.path(*args, query=query, **kwargs), self.parms(*args, **kwargs))
    
    def protocol(self, *args, **kwargs): return Protocol('https')
    def domain(self, *args, **kwargs): return Domain('geocoding.geo.census.gov')      
    def path(self, *args, query, **kwargs): return Path('geocoder', 'geographies', query)
    def parms(self, *args, address, **kwargs): 
        return ODict(_street(address.street) + _city(address.city) + _state(address.state) + _zipcode(address.zipcode) + _version(self.__version) + _vintage(self.__vintage) + _datatype('json'))


class USCensus_Geocoder_WebAPI(object):
    def __init__(self, urlapi, webreader, singlebatch_threshold=10, maxbatch_threshold=10000):
        self.__urlapi = urlapi
        self.__webreader = webreader
        self.__singlebatch_threshold = singlebatch_threshold
        self.__maxbatch_threshold = maxbatch_threshold
        
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader
    
    @keydispatcher
    def download(self, method, *args, **kwargs): raise KeyError(method)
    @keydispatcher
    def parser(self, method, data, *args, **kwargs): raise KeyError(method)    
           
    @download.register('request')
    def download_request(self, *args, **kwargs):
        url = self.urlapi(*args, query='geographies', **kwargs)
        data = self.webreader(str(url), *args, method='get', **kwargs)
        return data
    
    @download.register('file')
    def download_file(self, *args, addressID, addresses, **kwargs):       
        with tempfile.NamedTemporaryFile(suffix='.csv') as tempcsv:
            writer = csv.writer(tempcsv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(list(_APIHEADER))
            for count, address in enumerate(addresses, addressID): writer.writerow([str(count), *address.values()])
            url = self.urlapi(*args, batch='addressbatch', **kwargs)
            data = self.webreader(url, *args, method='post', data={'benchmark':self.urlapi.version, 'vintage':self.urlapi.vintage}, files={'addressFile':tempcsv}, **kwargs) 
        return data
    
    @parser.register('request')
    def parser_request(self, data, *args, addressID, address, **kwargs): 
        data = data['geographies']['Census Blocks']
        geodataframe = dataframe_fromjson(data, header=None, forceframe=True)[list(_GEOGRAPHYHEADER)]
        geodataframe = geodataframe.reindex([addressID])
        adddataframe = pd.DataFrame(address.todict(), index=[addressID])[list(_ADDRESSHEADER)]
        dataframe = pd.concat([geodataframe, adddataframe], axis=1)
        dataframe = dataframe.rename({item.upper():item.lower() for item in _GEOGRAPHYHEADER})
        return dataframe
    
    @parser.register('file')
    def parser_file(self, data, *args, addresses, **kwargs): 
        geodataframe = dataframe_fromcsv(data, header=0, forceframe=True)[list(_GEOGRAPHYHEADER)]
        adddataframe = pd.DataFrame([address.todict() for address in addresses], index=geodataframe.index)[list(_ADDRESSHEADER)]
        dataframe = pd.concat([geodataframe, adddataframe], axis=1)        
        dataframe = dataframe.rename({item.upper():item.lower() for item in _GEOGRAPHYHEADER})
        return dataframe
           
    def execute(self, method, *args, **kwargs):
        data = self.download(method, *args, **kwargs)
        dataframe = self.parser(method, data, *args, **kwargs)
        dataframe = self.compile_address(dataframe, *args, **kwargs)
        dataframe = self.compile_geography(dataframe, *args, **kwargs)
        return dataframe
    
    def __call__(self, *args, addresses=[], **kwargs):
        addresses = _filterempty([*_aslist(addresses), *_aslist(kwargs.get('address', None))])
        if len(addresses) <= self.__singlebatch_threshold: dataframe = pd.concat([self.execute('request', *args, addressID=count, address=address **kwargs) for count, address in enumerate(addresses, 0)], axis=0)
        elif len(addresses) <= self.__maxbatch_threshold: dataframe = self.execute('file', *args, addressID=0, addresses=addresses, **kwargs)
        else: dataframe = pd.concat([self.execute('file', *args, addressID=i, addresses=addresses[i:i+self.__maxbatch_threshold], **kwargs) for i in range(0, len(addresses), self.__maxbatch_threshold)], axis=0)
        return dataframe
        
    def compile_address(self, dataframe, *args, **kwargs):
        dataframe['address'] = dataframe[_ADDRESSHEADER].apply(lambda values: _ADDRESSFORMAT.format(**values.todict()), axis=1)
        dataframe = dataframe.drop(_ADDRESSHEADER, axis=1)
        return dataframe        

    def compile_geography(self, dataframe, *args, **kwargs):
        dataframe['geography'] = dataframe[_GEOGRAPHYHEADER].apply(lambda values: '|'.join(['='.join([column, value]) for column, value in zip(_GEOGRAPHYHEADER, values)]), axis=1)
        dataframe = dataframe.drop(_GEOGRAPHYHEADER, axis=1)
        return dataframe   


    









