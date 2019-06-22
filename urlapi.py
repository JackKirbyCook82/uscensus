# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus URLAPI
@author: Jack Kirby cook

"""

import os.path
import csv

from webdata.url import URLAPI, Protocol, Domain, Path, Parms
from utilities.dispatchers import clskey_singledispatcher as keydispatcher

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_URLAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""
    

_DIR = os.path.dirname(os.path.realpath(__file__))
_SPACEPROXY = '%20'
_DATEFORMATS = {'geoseries':'{year:04.0f}', 'yearseries':'{year:04.0f}', 'timeseries':'{year:04.0f}-{month:02.0f}'}
_GEOFILENAME = 'geography.csv'
_SURVEYFILENAME = 'surveys.csv'
_PROTOCOL = 'https'
_DOMAIN = 'api.census.gov'

with open(os.path.join(_DIR, _GEOFILENAME), mode='r') as infile:
    reader = csv.reader(infile)    
    _GEOGRAPHYS = {row[0]:row[1] for row in reader}
    
with open(os.path.join(_DIR, _SURVEYFILENAME), mode='r') as infile:
    reader = csv.reader(infile)
    _SURVEYS = {row[0]:row[1].split(';') for row in reader}

_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]
_date = lambda kwargs: kwargs['date']
_enddate = lambda kwargs: kwargs['date'] + kwargs['interval'] * kwargs['period']

_surveyvalues = lambda kwargs: [item.format(kwargs.get('estimate', 5)) for item in _SURVEYS[kwargs['survey']]]
_geokeys = lambda geography: [_GEOGRAPHYS[key].replace(' ', _SPACEPROXY) for key in geography.keys()]
_geovalues = lambda geography: [value for value in geography.values()]
_datevalue = lambda kwargs: _DATEFORMATS[kwargs['series']].format(year=_date(kwargs).year, month=_date(kwargs).month)
_enddatevalue = lambda kwargs: _DATEFORMATS[kwargs['series']].format(year=_enddate(kwargs).year, month=_enddate(kwargs).month)

_protocolsgmt = lambda protocol: Protocol(protocol)
_domainsgmt = lambda domain: Domain(domain)
_surveysgmt = lambda kwargs: Path(*_surveyvalues(kwargs))
_pathsgmt = lambda path: Path(*path)
_tagsgmt = lambda tags: Parms(get=','.join(tags))
_forgeosgmt = lambda geography: Parms(**{'for':':'.join([_geokeys(geography)[-1], _geovalues(geography)[-1]])})
_ingeosgmt = lambda geography: Parms(**{'in':[':'.join([key, value]) for key, value in zip(_geokeys(geography)[:-1], _geovalues(geography)[:-1])]})
_datesgmt = lambda kwargs: Parms(time=_datevalue(kwargs))
_timesgmt = lambda kwargs: Parms(time='+'.join(['from', _datevalue(kwargs), 'to', _enddatevalue(kwargs)]))
_predsgmt = lambda preds: Parms(**preds)
_keysgmt = lambda key: Parms(key=key)


class USCensus_URLAPI(URLAPI):
    def __init__(self, apikey, *args, **kwargs): self.__apikey = str(apikey)  
    def __repr__(self): return '{}(apikey={})'.format(self.__class__.__name__,self.__apikey)
    
    @property
    def apikey(self): return self.__apikey
    
    def protocol(self, *args, **kwargs): return _protocolsgmt(_PROTOCOL)
    def domain(self, *args, **kwargs): return _domainsgmt(_DOMAIN)    

    @keydispatcher('series')
    def path(self, *args, series, **kwargs): raise ValueError(series)    
    @path.register('geoseries')
    def path_geoseries(self, *args, date, **kwargs): return _pathsgmt(['data', str(date.year)]) + _surveysgmt(kwargs)    
    @path.register('dateseries')
    def path_dateseries(self, *args, **kwargs): return _pathsgmt(['data', 'timeseries']) + _surveysgmt(kwargs)
    @path.register('timeseries')
    def path_timeseries(self, *args, **kwargs): return _pathsgmt(['data', 'timeseries']) + _surveysgmt(kwargs)

    @keydispatcher('series')
    def parms(self, *args, series, **kwargs): raise ValueError(series)    
    @parms.register('geoseries')
    def parms_geoseries(self, *args, tags, geography, preds, **kwargs): return _tagsgmt(tags) + _forgeosgmt(geography) + _ingeosgmt(geography) + _predsgmt(preds) + _keysgmt(self.__apikey)    
    @parms.register('dateseries')
    def parms_dateseries(self, *args, tags, geography, preds, **kwargs): return _tagsgmt(tags) + _forgeosgmt(geography) + _ingeosgmt(geography) + _datesgmt(kwargs) + _predsgmt(preds) + _keysgmt(self.__apikey)    
    @parms.register('timeseries')
    def parms_timeseries(self, *args, tags, geography, preds, **kwargs): return _tagsgmt(tags) + _forgeosgmt(geography) + _ingeosgmt(geography) + _timesgmt(kwargs) + _predsgmt(preds) + _keysgmt(self.__apikey)





        









