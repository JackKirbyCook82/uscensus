# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus URLAPI
@author: Jack Kirby cook

"""

import os.path
from collections import namedtuple as ntuple
import pandas as pd

from webdata.url import URLAPI, Protocol, Domain, Path, JSONPath, HTMLPath, CSVPath, ZIPPath, Parms
from utilities.dataframes import dataframe_fromfile

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_URLAPI', 'USCensus_Geography', 'USCensus_ShapeFile_URLAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""
    

_DIR = os.path.dirname(os.path.realpath(__file__))
_SPACEPROXY = '%20'
_GEOGRAPHY = dataframe_fromfile(os.path.join(_DIR, 'geography.csv'), index='geography', header=0, forceframe=True) 


_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]
_isnull = lambda value: pd.isnull(value) if not isinstance(value, (list, tuple, dict)) else False

_protocolsgmt = lambda protocol: Protocol(protocol)
_domainsgmt = lambda domain: Domain(domain)
_pathsgmt = lambda path, filetype=None: {'json':JSONPath, 'html':HTMLPath, 'csv':CSVPath, 'zip':ZIPPath}.get(filetype, Path)(*path)

_tagsgmt = lambda tags: Parms(get=','.join(tags))
_predsgmt = lambda preds: Parms(**preds)
_keysgmt = lambda key: Parms(key=key)

_forgeosgmt = lambda usgeography: Parms(**{'for':':'.join([usgeography.apigeography.replace(' ', _SPACEPROXY), usgeography.value])})
_ingeosgmt = lambda usgeographys: Parms(**{'in':[':'.join([usgeography.apigeography.replace(' ', _SPACEPROXY), usgeography.value]) for usgeography in usgeographys]})

_datestr = lambda date, dateformat: dateformat.format(year=date.year, month=date.month)
_enddatestr = lambda date, interval, period, dateformat: dateformat.format(year=(date + interval * period).year, month=(date + interval * period).month)
_datesgmt = lambda date, dateformat: Parms(time=_datestr(date, dateformat))
_timesgmt = lambda date, interval, period, dateformat: Parms(time='+'.join(['from', _datestr(date, dateformat), 'to', _enddatestr(date, interval, period, dateformat)]))


USCensus_GeographySgmts = ntuple('USCensus_GeographySgmts', 'geography apigeography shapegeography shapedir shapefile value')
class USCensus_Geography(USCensus_GeographySgmts):
    def __new__(cls, geokey, geovalue=None):
        geosgmts = {key:(value if not _isnull(value) else None) for key, value in _GEOGRAPHY.transpose().to_dict()[geokey].items()}
        return super().__new__(cls, geokey, geosgmts['uscensus_geography'], geosgmts['shape_geography'], geosgmts['shape_dir'], geosgmts['shape_file'], geovalue)
          

class USCensus_URLAPI(URLAPI):
    def __init__(self, apikey, *args, **kwargs): self.__apikey = str(apikey) 
    def __repr__(self): return '{}(series={}, survey={}, apikey={})'.format(self.__class__.__name__, self.series, self.survey, self.__apikey)

    def protocol(self, *args, **kwargs): return _protocolsgmt('https')
    def domain(self, *args, **kwargs): return _domainsgmt('api.census.gov')  
    def path(self, *args, series, survey, query=[], filetype=None, **kwargs): return _pathsgmt(['data', series, *_aslist(survey), *_aslist(query)], filetype)
    def parms(self, *args, tags=[], geography, preds, **kwargs): 
        usgeographys = [USCensus_Geography(geokey, geovalue) for geokey, geovalue in geography.items()]
        return _tagsgmt(tags) + _forgeosgmt(usgeographys[-1]) + _ingeosgmt(usgeographys[:-1]) + _predsgmt(preds) + _keysgmt(self.__apikey)  
        
    # REGISTER SUBCLASSES  
    __registry = {}    
    @classmethod
    def registry(cls): return cls.__registry
    
    @classmethod
    def register(cls, series, survey):  
        def wrapper(subclass):
            name = subclass.__name__
            bases = (subclass, cls)
            newsubclass = type(name, bases, dict(series=series, survey=survey))
            USCensus_URLAPI.__registry[survey] = newsubclass
            return newsubclass
        return wrapper 
    
    @classmethod
    def create(cls, key): return cls.__registry[key]


@USCensus_URLAPI.register('geoseries', 'acsdetail')
class USCensus_ACSDetail_URLAPI:
    def path(self, *args, date, estimate=5, **kwargs): return super().path(*args, series='{year:04.0f}'.format(year=date.year), survey=['acs', 'acs{estimate}'.format(estimate=estimate)], **kwargs)   


class USCensus_ShapeFile_URLAPI(URLAPI):
    def protocol(self, *args, **kwargs): return _protocolsgmt('https')
    def domain(self, *args, **kwargs): return _domainsgmt('www2.census.gov')    
    def path(self, *args, shape, date, geography, filetype='zip', **kwargs): 
        usgeography = USCensus_Geography(shape)
        shapedir = usgeography.shapedir
        shapefile = usgeography.shapefile.format(year=str(date.year), state=geography.get('state', ''), county=geography.get('county', ''))
        return _pathsgmt(['geo', 'tiger', 'TIGER{year}'.format(year=str(date.year)), shapedir, shapefile], filetype)
    











