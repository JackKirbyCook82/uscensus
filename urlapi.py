# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus URLAPI
@author: Jack Kirby cook

"""

from collections import OrderedDict as ODict

from webdata.url import URLAPI

from uscensus.website import USCensus_Geography

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_ACS_URLAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""
    
 
_SPACEPROXY = '%20'


_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]
_filterempty = lambda items: [item for item in _aslist(items) if item]

_tags = lambda tags: [('get', ','.join(tags))]
_preds = lambda preds: [(key, value) for key, value in preds.items()]
_key = lambda key: [('key', key)]

_forgeo = lambda usgeography: [('for', ':'.join([usgeography.apigeography.replace(' ', _SPACEPROXY), usgeography.value]))]
_ingeos = lambda usgeographys: [('in', ':'.join([usgeography.apigeography.replace(' ', _SPACEPROXY), usgeography.value])) for usgeography in usgeographys]

_datestr = lambda date, dateformat: dateformat.format(year=date.year, month=date.month)
_enddatestr = lambda date, interval, period, dateformat: dateformat.format(year=(date + interval * period).year, month=(date + interval * period).month)
_date = lambda date, dateformat: [('time', _datestr(date, dateformat))]
_time = lambda date, interval, period, dateformat: [('time', '+'.join(['from', _datestr(date, dateformat), 'to', _enddatestr(date, interval, period, dateformat)]))]


class USCensus_URLAPI(URLAPI):
    def __repr__(self): return "{}(series='{}', survey='{}', apikey='{}')".format(self.__class__.__name__, self.series, self.survey, self.__apikey)    
    def __init__(self, apikey): self.__apikey = str(apikey) 
        
    def protocol(self, *args, **kwargs): return 'https'
    def domain(self, *args, **kwargs): return 'api.census.gov'  
    def path(self, *args, seriessgmt, surveysgmt, query=None, **kwargs): 
        return ['data', seriessgmt, *_aslist(surveysgmt), *_filterempty(_aslist(query))]
    def parms(self, *args, tags=[], geography, preds, **kwargs): 
        usgeographys = [USCensus_Geography(geokey, geovalue) for geokey, geovalue in geography.items()]
        return ODict(_tags(tags) + _forgeo(usgeographys[-1]) + _ingeos(usgeographys[:-1]) + _preds(preds) + _key(self.__apikey))  

    @classmethod
    def create(cls, series, survey):  
        def wrapper(subclass):
            name = subclass.__name__
            bases = (subclass, cls)
            return type(name, bases, dict(series=series, survey=survey))
        return wrapper 


@USCensus_URLAPI.create('geoseries', 'acs')
class USCensus_ACS_URLAPI:
    def path(self, *args, survey, date, estimate=5, **kwargs): 
        date.setformat('%Y')
        seriessgmt, surveysgmt = str(date), _filterempty(['acs', 'acs{estimate}'.format(estimate=estimate), survey])
        return super().path(*args, seriessgmt=seriessgmt, surveysgmt=surveysgmt, **kwargs)   















