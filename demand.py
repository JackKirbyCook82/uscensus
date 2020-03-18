# -*- coding: utf-8 -*-
"""
Created on Thu Dec 5 2019
@name:   USCensus Website Real Estate Demand Side
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
__all__ = ['demand_calculations']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


AGGS = {'households':'sum', 'population':'sum'}

demand_calculations = Calculation('demand', name='USCensus Real Estate Demand Calculations')

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
    '#hh|geo|inc': {},
    '#hh|geo|inc@renter': {},
    '#hh|geo|inc@owner': {}, 
    '#hh|geo|size@renter': {}, 
    '#hh|geo|size@owner': {}, 
    '#hh|geo|age@renter': {}, 
    '#hh|geo|age@owner': {}, 
    '#hh|geo|age@owner@mortgage': {}, 
    '#hh|geo|age@owner@equity': {},    
    '#hh|geo|child@renter': {}, 
    '#hh|geo|child@owner': {}, 
    '#pop|geo|age@male': {}, 
    '#pop|geo|age@female': {}, 
    '#pop|geo|race': {},
    '#pop|geo|origin': {},
    '#pop|geo|lang@age1': {},
    '#pop|geo|lang@age2': {},
    '#pop|geo|lang@age3': {},
    '#pop|geo|edu@male@age1': {},
    '#pop|geo|edu@male@age2': {},
    '#pop|geo|edu@male@age3': {},
    '#pop|geo|edu@male@age4': {},
    '#pop|geo|edu@male@age5': {},
    '#pop|geo|edu@female@age1': {},
    '#pop|geo|edu@female@age2': {},
    '#pop|geo|edu@female@age3': {},
    '#pop|geo|edu@female@age4': {},
    '#pop|geo|edu@female@age5': {}}
    

merge_tables = {
    '#hh|geo|inc|ten': {
        'tables': ['#hh|geo|inc@renter', '#hh|geo|inc@owner'],
        'parms': {'axis':'tenure'}},
    '#hh|geo|size|ten': {
        'tables': ['#hh|geo|size@renter', '#hh|geo|size@owner'],
        'parms': {'axis':'tenure'}},  
    '#hh|geo|age|ten': {
        'tables': ['#hh|geo|age@renter', '#hh|geo|age@owner'],
        'parms': {'axis':'tenure'}},    
    '#hh|geo|age|mort@owner': {
        'tables': ['#hh|geo|age@owner@mortgage', '#hh|geo|age@owner@equity'],
        'parms': {'axis':'mortgage'}}, 
    '#hh|geo|child': {
        'tables': ['#hh|geo|child@renter', '#hh|geo|child@owner'],
        'parms': {'axis':'tenure'}},   
    '#pop|geo|age|sex': {
        'tables': ['#pop|geo|age@male', '#pop|geo|age@female'],
        'parms': {'axis':'sex'}},     
    '#pop|geo|lang|age': {
        'tables': ['#pop|geo|lang@age1', '#pop|geo|lang@age2', '#pop|geo|lang@age3'],
        'parms': {'axis':'age'}}, 
    '#pop|geo|edu|age@male': {
        'tables': ['#pop|geo|edu@male@age1', '#pop|geo|edu@male@age2', '#pop|geo|edu@male@age3', '#pop|geo|edu@male@age4', '#pop|geo|edu@male@age5'],
        'parms': {'axis':'age'}},
    '#pop|geo|edu|age@female': {
        'tables': ['#pop|geo|edu@female@age1', '#pop|geo|edu@female@age2', '#pop|geo|edu@female@age3', '#pop|geo|edu@female@age4', '#pop|geo|edu@female@age5'],
        'parms': {'axis':'age'}},
    '#pop|geo|edu|age|sex': {
        'tables': ['#pop|geo|edu|age@male', '#pop|geo|edu|age@female'],
        'parms': {'axis':'sex'}}}

summation_tables = {
    '#pop|geo|age': {
        'tables': '#pop|geo|age|sex',
        'parms': {'axis':'sex'}},  
    '#pop|geo|edu|age': {
        'tables': '#pop|geo|edu|age|sex',
        'parms': {'axis':'sex'}}}

interpolate_pipeline = {     
    '#hh|geo|~inc|ten': {
        'tables': '#hh|geo|inc|ten',
        'parms': {'data':'households', 'axis':'income', 'formatting':{'multiplier':'', 'precision':3}}}}


@demand_calculations.create(**feed_tables)
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


@demand_calculations.create(**merge_tables)
def merge_pipeline(tableID, table, *args, axis, **kwargs):
    others = [arg for arg in args if isinstance(arg, type(table))]
    return tbls.combinations.merge([table, *others], *args, axis=axis, **kwargs)
    
@demand_calculations.create(**summation_tables)
def sum_pipeline(tableID, table, *args, axis, **kwargs):
    return summation(table, *args, axis=axis, **kwargs).squeeze(axis)

def proxyvalues(x):
    yi = x[0] - round(np.diff(x).min()/2)
    yield yi
    for xi in x:
        yi = 2*xi - yi
        yield yi

@demand_calculations.create(**interpolate_pipeline)
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



    





