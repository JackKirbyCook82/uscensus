# -*- coding: utf-8 -*-
"""
Created on Weds Sept 11 2019
@name:   USCensus WebAPI Object
@author: Jack Kirby cook

"""

import pandas as pd
import numpy as np

from utilities.dataframes import dataframe_fromjson
from webscraping.webapi import WebAPI

from uscensus.website import USCensus_APIGeography

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_WebAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""
 

_FILENAMES = {'geoseries':'{tableID}_{date}_{geoID}.csv'}
_DATEFORMATS = {'geoseries':'%Y', 'yearseries':'%Y', 'timeseries':'%Y-%m'}
_AGGREGATIONS = {'sum':np.sum, 'avg':np.mean, 'max':np.max, 'min':np.min}

_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]
_filterlist = lambda items: [item for item in _aslist(items) if item]


def dataparser(item):
    if pd.isnull(item): return item
    try: return int(float(item)) if not bool(float(item) % 1) else float(item)
    except ValueError: return str(item)


class USCensus_WebAPI(WebAPI):
    def __init__(self, *args, variable_webquery, **kwargs):
        self.__variablewebquery = variable_webquery
        super().__init__(*args, **kwargs)
             
    @property
    def series(self): return self.urlapi.series
    @property
    def survey(self): return self.urlapi.survey    

    def filename(self, *args, tableID, geography, date, **kwargs):
        filename = _FILENAMES[self.series].format(tableID=tableID, date=date, geoID=geography.geoID)
        filename = filename.replace('|', '_')
        return filename

    def execute(self, *args, geography, agg=None, **kwargs):
        variables = self.__variablewebquery(*args, **kwargs)
        apigeographys = [USCensus_APIGeography(geokey, geovalue) for geokey, geovalue in geography.items()]
        dataframe = self.download(*args, tags=['NAME', *[item.tag for item in variables]], geography=geography, **kwargs)               
        dataframe = dataframe.rename({item.tag:item.concept for item in variables}, axis='columns')  
        dataframe = dataframe.rename({item.apigeography:item.geography for item in apigeographys}, axis='columns') 
        dataframe = dataframe.rename({'NAME':'geopath'}, axis='columns')                  
        dataframe = self.compile_geography(dataframe, *args, columns=[item.geography for item in apigeographys], **kwargs)
        dataframe = self.compile_variable(dataframe, *args, columns=[item.concept for item in variables], **kwargs)
        dataframe = self.parser(dataframe, *args, **kwargs)
        return dataframe    

    def download(self, *args, **kwargs):
        url = self.urlapi(*args, **kwargs)    
        data = self.webreader(str(url), *args, method='get', datatype='json', **kwargs)
        dataframe = dataframe_fromjson(data, header=0, forceframe=True)
        return dataframe

    def compile_geography(self, dataframe, *args, columns, **kwargs):
        dataframe['geopath'] = dataframe['geopath'].apply(lambda values: '|'.join(['='.join([column, value]) for column, value in zip(columns, values.split(', ')[::-1])]))        
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

    def generator(self, *args, dates=[], **kwargs):
        for date in _filterlist([*_aslist(dates), kwargs.get('date', None)]):
            date.setformat(_DATEFORMATS[self.series])
            try: dataframe = self.load(*args, date=date, **kwargs)
            except FileNotFoundError: 
                dataframe = self.execute(*args, date=date, **kwargs)  
                if self.saving: self.save(dataframe, *args, date=date, **kwargs)
            yield dataframe

    def __call__(self, *args, **kwargs):
        try: 
            dataframes = [dataframe for dataframe in self.generator(*args, **kwargs)]           
            dataframe = pd.concat(dataframes, axis=0)
            return dataframe
        except Exception as error:
           raise error




        
        