# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 2018
@name:   USCensus URLAPI Objects
@author: Jack Kirby Cook

"""

from webdata.url import URL, Protocol, Domain, Path, Parms, JSONPath

from uscensus.website import USCensus_APIGeography

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_ACS_URLAPI', 'USCensus_ACSMigration_URLAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""
    
 
_SPACEPROXY = '%20'

_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]
_filterempty = lambda items: [item for item in _aslist(items) if item]

_tags = lambda tags: Parms(get=','.join(tags))
_preds = lambda preds: Parms(**preds)
_key = lambda key: Parms(key=key)

_forgeo = lambda usgeography: Parms(**{'for':':'.join([usgeography.apigeography.replace(' ', _SPACEPROXY), usgeography.value])})
_ingeos = lambda usgeographys: Parms(**{'in':_SPACEPROXY.join([':'.join([usgeography.apigeography.replace(' ', _SPACEPROXY), usgeography.value]) for usgeography in usgeographys])})

_datestr = lambda date, dateformat: dateformat.format(year=date.year, month=date.month)
_enddatestr = lambda date, interval, period, dateformat: dateformat.format(year=(date + interval * period).year, month=(date + interval * period).month)
_date = lambda date, dateformat: Parms(time=_datestr(date, dateformat))
_time = lambda date, interval, period, dateformat: Parms(time='+'.join(['from', _datestr(date, dateformat), 'to', _enddatestr(date, interval, period, dateformat)]))


class USCensus_URLAPI(object):
    def __repr__(self): return "{}(series='{}', survey='{}', apikey='{}')".format(self.__class__.__name__, self.series, self.survey, self.__apikey)    
    def __init__(self, apikey): self.__apikey = str(apikey) 
    
    def __call__(self, *args, query=None, **kwargs):
        if not query: return URL(self.protocol(*args, **kwargs), self.domain(*args, **kwargs), path=self.path(*args, **kwargs), parms=self.parms(*args, **kwargs))
        else: return URL(self.protocol(*args, **kwargs), self.domain(*args, **kwargs), path=self.querypath(*args, query=query, **kwargs))
        
    def protocol(self, *args, **kwargs): return Protocol('https')
    def domain(self, *args, **kwargs): return Domain('api.census.gov')  
    
    def path(self, *args, seriessgmt, surveysgmt, **kwargs): return Path('data', seriessgmt, *_aslist(surveysgmt))
    def querypath(self, *args, seriessgmt, surveysgmt, query, **kwargs): return JSONPath('data', seriessgmt, *_aslist(surveysgmt), *_aslist(query))    
    
    def parms(self, *args, tags=[], geography, preds, **kwargs): 
        usgeographys = [USCensus_APIGeography(geokey, geovalue) for geokey, geovalue in geography.items()]
        return _tags(tags) + _forgeo(usgeographys[-1]) + _ingeos(usgeographys[:-1]) + _preds(preds) + _key(self.__apikey) 

    @classmethod
    def create(cls, series, survey):  
        def wrapper(subclass):
            name = subclass.__name__
            bases = (subclass, cls)
            return type(name, bases, dict(series=series, survey=survey))
        return wrapper 


@USCensus_URLAPI.create('geoseries', 'acs')
class USCensus_ACS_URLAPI:
    def path(self, *args, dataset, date, **kwargs): 
        date.setformat('%Y')
        survey, estimate = dataset.get('survey', None), dataset['estimate'] 
        seriessgmt, surveysgmt = str(date), _filterempty(['acs', 'acs{estimate}'.format(estimate=estimate), survey])
        return super().path(*args, seriessgmt=seriessgmt, surveysgmt=surveysgmt, **kwargs)   

    def querypath(self, *args, dataset, date, **kwargs): 
        date.setformat('%Y')
        survey, estimate = dataset.get('survey', None), dataset['estimate'] 
        seriessgmt, surveysgmt = str(date), _filterempty(['acs', 'acs{estimate}'.format(estimate=estimate), survey])
        return super().querypath(*args, seriessgmt=seriessgmt, surveysgmt=surveysgmt, **kwargs)   









