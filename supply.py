# -*- coding: utf-8 -*-
"""
Created on Thu Dec 5 2019
@name:   USCensus Website Real Estate Supply Side
@author: Jack Kirby Cook

"""

import numpy as np

import tables as tbls
from tables.processors import Calculation
from tables.transformations import Boundary, Reduction, GroupBy, Scale, Cumulate, Consolidate, Interpolate, Unconsolidate, Uncumulate

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, variable_cleaner, variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['supply_calculations']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


AGGS = {'households':'sum', 'population':'sum', 'structures':'sum'}

supply_calculations = Calculation('supply', name='USCensus Real Estate Supply Calculations')

sumcontained = GroupBy(how='contains', agg='sum', ascending=True)
boundary = Boundary()
sumcontained = GroupBy(how='contains', agg='sum', ascending=True)
summation = Reduction(how='summation', by='summation')
normalize = Scale(how='normalize')
uppercumulate = Cumulate(how='upper')
upperconsolidate = Consolidate(how='cumulate', direction='upper')
interpolate = Interpolate(how='linear', fill='extrapolate')
upperunconsolidate = Unconsolidate(how='cumulate', direction='upper')
upperuncumulate = Uncumulate(how='upper')
avgconsolidate = Consolidate(how='average', weight=0.5)


feed_tables = {
    '#pop|geo|cmte': {}, 
    '#hh|geo|cost@renter': {},
    '#hh|geo|cost@owner': {},
    '#hh|geo|cost@owner@mortgage': {},
    '#hh|geo|cost@owner@equity': {},
    '#hh|geo|rent@renter': {},
    '#hh|geo|val@owner': {},          
    '#st|geo|yrblt':{},
    '#st|geo|rm': {},
    '#st|geo|br': {},
    '#st|geo|unit': {},
    '#st|geo|yrocc@renter': {}, 
    '#st|geo|yrocc@owner': {},     
    '#st|geo|yrocc@renter@age1': {},
    '#st|geo|yrocc@renter@age2': {},
    '#st|geo|yrocc@renter@age3': {},
    '#st|geo|yrocc@owner@age1': {},
    '#st|geo|yrocc@owner@age2': {},
    '#st|geo|yrocc@owner@age3': {},
    '#st|geo|occ': {},
    '#st|geo|vac@vacant': {}}

merge_tables = {
    '#st|geo|yrocc|ten': {
        'tables': ['#st|geo|yrocc@renter', '#st|geo|yrocc@owner'],
        'parms': {'axis':'tenure'}},
    '#st|geo|yrocc|age@renter': {
        'tables': ['#st|geo|yrocc@renter@age1', '#st|geo|yrocc@renter@age2', '#st|geo|yrocc@renter@age3'],
        'parms': {'axis':'age'}},
    '#st|geo|yrocc|age@owner': {
        'tables': ['#st|geo|yrocc@owner@age1', '#st|geo|yrocc@owner@age2', '#st|geo|yrocc@owner@age3'],
        'parms': {'axis':'age'}},
    '#st|geo|yrocc|age|ten': {
        'tables': ['#st|geo|yrocc|age@renter', '#st|geo|yrocc|age@owner'],
        'parms': {'axis':'tenure'}}}     
    
interpolate_pipeline = {     
    }
    

@supply_calculations.create(**feed_tables)
def feed_pipeline(tableID, *args, **kwargs):
    queryParms = query(tableID)
    universe, index, header, scope = queryParms['universe'], queryParms['index'], queryParms['header'], queryParms['scope']   
    dataframe = acs_webapi(*args, tableID=tableID, **queryParms, **kwargs)
    dataframe = variable_cleaner(dataframe, *args, **kwargs)   
    flattable = tbls.FlatTable(dataframe, variables=variables, name=tableID)
    arraytable = flattable[[universe, index, header, 'date', *scope.keys()]].unflatten(universe, aggs=AGGS)
    arraytable = arraytable.squeeze(*scope.keys()).sortall(ascending=True).fillneg(np.nan)   
    arraytable = sumcontained(arraytable, axis=header)
    return arraytable


@supply_calculations.create(**merge_tables)
def merge_pipeline(tableID, table, *args, axis, **kwargs):
    others = [arg for arg in args if isinstance(arg, type(table))]
    return tbls.combinations.merge([table, *others], *args, axis=axis, **kwargs)

@supply_calculations.create(**interpolate_pipeline)
def interpolate_pipeline(tableID, table, *args, axis, **kwargs):
    pass

def proxyvalues(x):
    yi = x[0] - round(np.diff(x).min()/2)
    yield yi
    for xi in x:
        yi = 2*xi - yi
        yield yi

@supply_calculations.create(**interpolate_pipeline)
def interpolate_pipeline(tableID, table, *args, data, axis, formatting, values, **kwargs):
    values = [value for value in proxyvalues(values[axis])]        
    table = boundary(table, axis=axis, bounds=(values[0], None))
    table = normalize(table, *args, axis=axis, **kwargs) 
    table = uppercumulate(table, *args, axis=axis, **kwargs)
    table = upperconsolidate(table, *args, axis=axis, **kwargs)
    table = interpolate(table, *args, axis=axis, values=values[:-1], **kwargs).fillneg(fill=0)
    table = upperunconsolidate(table, *args, axis=axis, **kwargs)
    table = upperuncumulate(table, *args, axis=axis, total=1, **kwargs)
    table = avgconsolidate(table, *args, axis=axis, bounds=(values[0], values[-1]), **kwargs)
    return table



