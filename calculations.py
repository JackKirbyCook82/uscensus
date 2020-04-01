# -*- coding: utf-8 -*-
"""
Created on Thu Dec 5 2019
@name:   USCensus Website Calculations
@author: Jack Kirby Cook

"""

import numpy as np

import tables as tbls
from tables.processors import Calculation, Renderer
from tables.transformations import Boundary, Reduction, GroupBy, Scale, Cumulate, Consolidate, Interpolate, Unconsolidate, Uncumulate, Moving

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, variable_cleaner, variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['calculations', 'renderer']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


AGGS = {'households':'sum', 'population':'sum', 'structures':'sum'}

calculations = Calculation('uscensus', name='USCensus Calculations')
renderer = Renderer(style='double', extend=1)

boundary = Boundary()
sumcontained = GroupBy(how='contains', agg='sum', ascending=True)
sumcouple = Reduction(how='summation', by='couple')
summation = Reduction(how='summation', by='summation')
normalize = Scale(how='normalize')
uppercumulate = Cumulate(how='upper')
upperconsolidate = Consolidate(how='cumulate', direction='upper')
interpolate = Interpolate(how='linear', fill='extrapolate')
upperunconsolidate = Unconsolidate(how='cumulate', direction='upper')
upperuncumulate = Uncumulate(how='upper')
avgconsolidate = Consolidate(how='average', weight=0.5)
movingdifference = Moving(how='difference', by='minimum', period=1)


feed_tables = {
    '#agginc|geo': {},
    '#aggrent|geo@renter': {},
    '#aggval|geo|mort@owner': {},
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
    '#hh|geo|cost@renter': {},
    '#hh|geo|cost@owner': {},
    '#hh|geo|cost@owner@mortgage': {},
    '#hh|geo|cost@owner@equity': {},
    '#hh|geo|rent@renter': {},
    '#hh|geo|val@owner': {}, 
    '#hh|geo|mort@owner': {},
    '#hh|geo@renter': {},
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
    '#pop|geo|edu@female@age5': {},
    '#pop|geo|cmte': {},           
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
    '#hh|geo': {
        'tables': ['#hh|geo@renter', '#hh|geo@owner'],
        'parms': {'axis':'tenure'}},        
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
    '#hh|geo|child|ten': {
        'tables': ['#hh|geo|child@renter', '#hh|geo|child@owner'],
        'parms': {'axis':'tenure'}},   
    '#pop|geo|age|sex': {
        'tables': ['#pop|geo|age@male', '#pop|geo|age@female'],
        'parms': {'axis':'sex'}},     
    '#pop|geo|age|lang': {
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
        'parms': {'axis':'sex'}},
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

summation_tables = {
    '#aggval|geo@owner': {
        'tables': '#aggval|geo|mort@owner',
        'parms': {'axis':'mortgage'}},        
    '#pop|geo|age': {
        'tables': '#pop|geo|age|sex',
        'parms': {'axis':'sex'}},  
    '#pop|geo|age|edu': {
        'tables': '#pop|geo|edu|age|sex',
        'parms': {'axis':'sex'}},
    '#pop|geo|lang': {
        'tables':'#pop|geo|age|lang',
        'parms':{'axis':'age'}},      
    '#hh|geo@owner': {
        'tables':'#hh|geo|val@owner',
        'parms':{'axis':'value'}},
    '#hh|geo@renter': {
        'tables':'#hh|geo|rent@owner',
        'parms':{'axis':'rent'}},
    '#hh|geo|~inc': {
        'tables':'#hh|geo|~inc|ten',
        'parms':{'axis':'tenure'}},
    '#hh|geo|~age': {
        'tables':'#hh|geo|~age|ten',
        'parms':{'axis':'tenure'}},
    '#hh|geo|~size': {
        'tables':'#hh|geo|~size|ten',
        'parms':{'axis':'tenure'}},
    '#hh|geo|child': {
        'tables':'#hh|geo|child|ten',
        'parms':{'axis':'tenure'}},        
    '#pop|geo|edu': {
        'tables':'#pop|geo|age|edu',
        'parms':{'axis':'age'}},
    '#st|geo|yrocc|age': {
        'tables':'#st|geo|yrocc|age|ten',
        'parms':{'axis':'tenure'}},
    '#st|geo|yrocc|tenure': {
        'tables':'#st|geo|yrocc|age|ten',
        'parms':{'axis':'age'}},
    '#st|geo|yrocc': {
        'tables':'#st|geo|yrocc|age',
        'parms':{'axis':'age'}}}
    
boundary_pipeline = {
    '#hh|geo|~size|ten': {
        'tables': '#hh|geo|size|ten',
        'parms': {'axis':'size', 'bounds':(0, 7)}}}

interpolate_pipeline = {     
    '#hh|geo|~inc|ten': {
        'tables': '#hh|geo|inc|ten',
        'parms': {'data':'households', 'axis':'income', 'bounds':(0, 200000), 'values':[10000, 25000, 40000, 60000, 80000, 100000, 125000, 150000, 175000, 200000]}},
    '#hh|geo|~age|ten': {
        'tables': '#hh|geo|age|ten',
        'parms': {'data':'households', 'axis':'age', 'bounds':(15, 95), 'values':[20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 75, 85]}},
    '#hh|geo|~val@owner': {
        'tables': '#hh|geo|val@owner',
        'parms': {'data':'households', 'axis':'value', 'bounds':(0, 1500000), 'values':[100000, 150000, 200000, 250000, 300000, 350000, 400000, 500000, 750000, 1000000]}},
    '#hh|geo|~rent@renter': {
        'tables': '#hh|geo|rent@renter',
        'parms': {'data':'households', 'axis':'rent', 'bounds':(0, 4000), 'values':[500, 750, 1000, 1250, 1500, 1750, 2000, 2250, 2500, 2750, 3000, 3500]}},
    '#st|geo|yrocc|~age|ten': {
        'tables':'#st|geo|yrocc|age|ten',
        'parms':{'data':'structures', 'axis':'age', 'bounds':(15, 95), 'values':[20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 75, 85]}},
    '#st|geo|~yrocc|~age|ten': {
        'tables':'#st|geo|yrocc|~age|ten',
        'parms':{'data':'structures', 'axis':'yearoccupied', 'bounds':(1980, 2020), 'values':[1985, 1990, 1995, 2000, 2005, 2010, 2015, 2018]}},
    '#st|geo|~yrocc': {
        'tables':'#st|geo|yrocc',
        'parms':{'data':'structures', 'axis':'yearoccupied', 'bounds':(1980, 2020), 'values':[1985, 1990, 1995, 2000, 2005, 2010, 2015, 2018]}}}

collapse_pipeline = {
    '#hh|geo|~val': {
        'tables': ['#hh|geo|~val@owner', '#hh|geo|~rent@renter'],
        'parms': {'axis':'value', 'collapse':'rent', 'value':0, 'scope':'tenure'}}}
    
ratio_pipeline = {
    'avginc|geo': {
        'tables': ['#agginc|geo', '#hh|geo'],
        'parms': {'topdata':'aggincome', 'bottomdata':'households', 'formatting':{'precision':2}}},          
    'avgval|geo@owner': {
        'tables': ['#aggval|geo@owner', '#hh|geo'],
        'parms': {'topdata':'aggvalue', 'bottomdata':'households', 'formatting':{'precision':2}}},   
    'avgrent|geo@renter': {
        'tables': ['#aggrent|geo@renter', '#hh|geo'],
        'parms': {'topdata':'aggrent', 'bottomdata':'households', 'formatting':{'precision':2}}}}    


rate_pipeline = {
    'Δ%avginc|geo': {
        'tables': '#agginc|geo', 
        'parms': {'data':'aggincome', 'axis':'date', 'formatting':{'precision':2, 'multiplier':'%'}}},          
    'Δ%avgval|geo@owner': {
        'tables': '#aggval|geo@owner',
        'parms': {'data':'aggvalue', 'axis':'date', 'formatting':{'precision':2, 'multiplier':'%'}}},   
    'Δ%avgrent|geo@renter': {
        'tables': '#aggrent|geo@renter',
        'parms': {'data':'aggrent', 'axis':'date', 'formatting':{'precision':2, 'multiplier':'%'}}}}        
        

@calculations.create(**feed_tables)
def feed_pipeline(tableID, *args, **kwargs):
    queryParms = query(tableID)
    universe, index, header, scope = queryParms['universe'], queryParms['index'], queryParms['header'], queryParms['scope']   
    dataframe = acs_webapi(*args, tableID=tableID, **queryParms, **kwargs)
    dataframe = variable_cleaner(dataframe, *args, **kwargs)   
    flattable = tbls.FlatTable(dataframe, variables=variables, name=tableID)
    arraytable = flattable[[universe, index, header, 'date', *scope.keys()]].unflatten(universe, aggs=AGGS)
    arraytable = arraytable.squeeze(*scope.keys()).sortall(ascending=True).fillneg(np.nan)   
    if header: arraytable = sumcontained(arraytable, axis=header)
    return arraytable

@calculations.create(**merge_tables)
def merge_pipeline(tableID, table, other, *args, axis, **kwargs):
    assert isinstance(other, type(table))
    table = tbls.combinations.merge([table, other], *args, axis=axis, **kwargs)
    others = [arg for arg in args if isinstance(arg, type(table))]
    for other in others: table = tbls.combinations.append([table, other], *args, axis=axis, **kwargs)
    return table
    
@calculations.create(**summation_tables)
def sum_pipeline(tableID, table, *args, axis, **kwargs): 
    return summation(table, *args, axis=axis, **kwargs).squeeze(axis)

@calculations.create(**boundary_pipeline)
def boundary_pipeline(tableID, table, *args, axis, bounds, **kwargs): 
    return avgconsolidate(table, *args, axis=axis, bounds=bounds, **kwargs)

def proxyvalues(x):
    yi = max(x[0] - np.floor(np.diff(x).min()/2), 0)
    yield yi
    for xi in x:
        yi = 2*xi - yi
        yield yi
        
@calculations.create(**interpolate_pipeline)
def interpolate_pipeline(tableID, table, *args, data, axis, bounds, values, **kwargs):
    values = [value for value in proxyvalues(values)]    
    retag = {'{data}/total{data}*total{data}'.format(data=data):data}
    table = boundary(table, *args, axis=axis, bounds=(bounds[0], None), **kwargs)
    total = summation(table, *args, axis=axis, retag={data:'total{}'.format(data)}, **kwargs)
    table = tbls.operations.divide(table, total, *args, axis=axis, **kwargs)
    table = uppercumulate(table, *args, axis=axis, **kwargs)
    table = upperconsolidate(table, *args, axis=axis, **kwargs)
    table = interpolate(table, *args, axis=axis, values=values[:-1], **kwargs).fillneg(fill=0)  
    table = upperunconsolidate(table, *args, axis=axis, **kwargs)
    table = upperuncumulate(table, *args, axis=axis, total=1, **kwargs)    
    table = avgconsolidate(table, *args, axis=axis, bounds=(bounds[0], values[-1]), **kwargs)
    table = tbls.operations.multiply(table, total, *args, noncoreaxis=axis, retag=retag, simplify=True, **kwargs)
    return table

@calculations.create(**collapse_pipeline)
def collapse_pipeline(tableID, table, other, *args, axis, collapse, value, scope, **kwargs):
    other = sumcouple(other, *args, axis=collapse, **kwargs).squeeze(collapse)
    other = other.addscope(axis, value, table.variables[axis])
    table = tbls.combinations.append([table, other], *args, axis=axis, noncoreaxes=[collapse, scope], **kwargs)
    return table

@calculations.create(**ratio_pipeline)
def ratio_pipeline(tableID, toptable, bottomtable, *args, topdata, bottomdata, **kwargs):
    table = tbls.operations.divide(toptable, bottomtable, *args, **kwargs).fillinf(np.NaN)
    return table
    
@calculations.create(**rate_pipeline)
def rate_pipeline(tableID, table, *args, data, axis, **kwargs):
    retag = {'delta{data}/{data}'.format(data=data):'{}rate'.format(data)}
    deltatable = movingdifference(table, *args, axis=axis, retag={data:'delta{}'.format(data)}, **kwargs)
    basetable = table[{'date':slice(-1)}]
    table = tbls.operations.divide(deltatable, basetable, *args, retag=retag, **kwargs).fillinf(np.NaN)
    return table
    






