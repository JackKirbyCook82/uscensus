# -*- coding: utf-8 -*-
"""
Created on Weds Sept 11 2019
@name:   USCensus WebAPI
@author: Jack Kirby cook

"""

import pandas as pd

from utilities.dataframes import dataframe_fromjson
from webdata.webapi import WebAPI

from uscensus.website import USCensus_Variable, USCensus_Geography

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_WebAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""
 

_FILENAMES = {'geoseries':'{tableID}_{survey}_{date}_{geoid}.csv'}
_DATEFORMATS = {'geoseries':'%Y', 'yearseries':'%Y', 'timeseries':'%Y-%m'}


def dataparser(item):
    if pd.isnull(item): return item
    try: return int(float(item)) if not bool(float(item) % 1) else float(item)
    except ValueError: return str(item)

    
class USCensus_WebAPI(WebAPI):
    def __init__(self, repository, urlapi, webreader, saving=True):
        self.__urlapi = urlapi
        self.__webreader = webreader
        super().__init__('USCensus', repository=repository, saving=saving)
        
    @property
    def series(self): return self.__urlapi.series
    @property
    def survey(self): return self.__urlapi.survey
    
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader

    def filename(self, *args, tableID, survey, geography, date, estimate=5, **kwargs):
        filename = _FILENAMES[self.series].format(tableID=tableID, survey=survey.format(estimate=estimate), date=date, geoid=geography.geoid)
        filename = filename.replace('|', '_')
        return filename

    def generator(self, *args, dates, **kwargs):
        for date in dates:
            date.setformat(_DATEFORMATS[self.series])
            try: dataframe = self.load(*args, date=date, **kwargs)
            except FileNotFoundError: 
                dataframe = self.execute(*args, date=date, **kwargs)  
                if self.saving: self.save(dataframe, *args, date=date, **kwargs)
            yield dataframe

    def execute(self, *args, **kwargs):
        varquery = self.variablequery(*args, **kwargs)
        geoquery = self.geographyquery(*args, **kwargs)
        dataframe = self.download(*args, tags=['NAME', *[item.tag for item in varquery]], **kwargs)               
        dataframe = dataframe.rename({item.tag:item.concept for item in varquery}, axis='columns')  
        dataframe = dataframe.rename({item.apigeography:item.geography for item in geoquery}, axis='columns') 
        dataframe = dataframe.rename({'NAME':'geoname'}, axis='columns')                     
        dataframe = self.compile_geography(dataframe, *args, columns=[item.geography for item in geoquery], **kwargs)
        dataframe = self.compile_variable(dataframe, *args, columns=[item.concept for item in varquery], **kwargs)
        dataframe = self.parser(dataframe, *args, **kwargs)
        return dataframe    

    def variablequery(self, *args, group=None, labels, date, **kwargs):   
        labels = [tuple([item.format(date=str(date)) for item in label]) for label in labels]
        if not group: url = self.urlapi.query(*args, query='variables', filetype='json', date=date, **kwargs)   
        else: url = self.urlapi.query(*args, query=['groups', group], filetype='json', date=date, **kwargs)   
        variabledata = self.webreader(str(url), *args, **kwargs)
        variabledata =  [USCensus_Variable(tag=key, date=date, **items) for key, items in variabledata['variables'].items()]
        variables = [item for item in variabledata if any([item == label for label in labels])]
        if not variables: 
            for item in variabledata: print(str(item))
            raise ValueError(variables)
        return variables           

    def geographyquery(self, *args, geography, **kwargs):
        geographys = [USCensus_Geography(geokey=geokey, geovalue=geovalue) for geokey, geovalue in geography.items()]
        return geographys

    def download(self, *args, **kwargs):
        url = self.urlapi(*args, **kwargs)    
        data = self.webreader(str(url), *args, **kwargs)
        dataframe = dataframe_fromjson(data, header=0, forceframe=True)
        return dataframe

    def compile_geography(self, dataframe, *args, columns, **kwargs):
        dataframe['geoname'] = dataframe['geoname'].apply(lambda geoname: '|'.join(geoname.split(', ')))
        dataframe['geography'] = dataframe[columns].apply(lambda values: '|'.join(['='.join([column, value]) for column, value in zip(columns, values)]), axis=1)
        dataframe = dataframe.drop(columns, axis=1)
        return dataframe

    def compile_variable(self, dataframe, *args, columns, **kwargs):
        if len(columns) > 1: dataframe = dataframe.melt(id_vars=[column for column in dataframe.columns if column not in columns], var_name='header', value_name='universe')
        else: dataframe = dataframe.rename({columns[0]:'universe'}, axis='columns')
        return dataframe

    def parser(self, dataframe, *args, universe, header, scope, date, **kwargs):
        dataframe = dataframe.rename({'universe':universe}, axis='columns')
        if header: dataframe = dataframe.rename({'header':header}, axis='columns')
        for key, value in scope.items(): dataframe[key] = str(value)
        dataframe['date'] = str(date)
        dataframe[universe] = dataframe[universe].apply(dataparser)
        return dataframe







        
        