# -*- coding: utf-8 -*-
"""
Created on Thu Dec 5 2019
@name:   USCensus Website Calculations
@author: Jack Kirby Cook

"""

import numpy as np

import tables as tbls
from tables.processors import CalculationProcess, CalculationRenderer
from tables.transformations import Boundary, Reduction, GroupBy, Scale, Cumulate, Consolidate, Interpolate, Unconsolidate, Uncumulate, Moving, Expansion

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, cleaner, variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['process', 'renderer', 'variables']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


AGGS = {'households':'sum', 'population':'sum', 'structures':'sum'}

process = CalculationProcess('uscensus', name='USCensus Calculations')
renderer = CalculationRenderer(style='double', extend=1)

boundary = Boundary()
sumcontained = GroupBy(how='contains', agg='sum', ascending=True)
sumgroup = GroupBy(how='groups', agg='sum', ascending=True)
sumcouple = Reduction(how='summation', by='couple')
summation = Reduction(how='summation', by='summation')
average = Reduction(how='average', by='summation')
normalize = Scale(how='normalize')
expansion = Expansion(how='equaldivision')
uppercumulate = Cumulate(how='upper')
upperconsolidate = Consolidate(how='cumulate', direction='upper')
avgconsolidate = Consolidate(how='average', weight=0.5)
upperunconsolidate = Unconsolidate(how='cumulate', direction='upper')
interpolate = Interpolate(how='linear', fill='extrapolate')
upperuncumulate = Uncumulate(how='upper')
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
    '#pop|geo|age@child': {},
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
    '#hh|geo|ten': {
        'tables': ['#hh|geo@owner', '#hh|geo@renter'],
        'parms': {'axis':'tenure', 'noncoreaxis':'mortgage'}},      
    '#hh|geo|inc|ten': {
        'tables': ['#hh|geo|inc@owner', '#hh|geo|inc@renter'],
        'parms': {'axis':'tenure'}},
    '#hh|geo|size|ten': {
        'tables': ['#hh|geo|size@owner', '#hh|geo|size@renter'],
        'parms': {'axis':'tenure'}},  
    '#hh|geo|age|ten': {
        'tables': ['#hh|geo|age@owner', '#hh|geo|age@renter'],
        'parms': {'axis':'tenure'}},    
    '#hh|geo|age|mort@owner': {
        'tables': ['#hh|geo|age@owner@mortgage', '#hh|geo|age@owner@equity'],
        'parms': {'axis':'mortgage'}}, 
    '#hh|geo|child|ten': {
        'tables': ['#hh|geo|child@owner', '#hh|geo|child@renter'],
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
        'tables': ['#st|geo|yrocc@owner', '#st|geo|yrocc@renter'],
        'parms': {'axis':'tenure'}},
    '#st|geo|yrocc|age@renter': {
        'tables': ['#st|geo|yrocc@renter@age1', '#st|geo|yrocc@renter@age2', '#st|geo|yrocc@renter@age3'],
        'parms': {'axis':'age'}},
    '#st|geo|yrocc|age@owner': {
        'tables': ['#st|geo|yrocc@owner@age1', '#st|geo|yrocc@owner@age2', '#st|geo|yrocc@owner@age3'],
        'parms': {'axis':'age'}},
    '#st|geo|yrocc|age|ten': {
        'tables': ['#st|geo|yrocc|age@owner', '#st|geo|yrocc|age@renter'],
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
    '#hh|geo': {
        'tables':'#hh|geo|ten',
        'parms':{'axis':'tenure'}},           
    '#hh|geo@owner': {
        'tables':'#hh|geo|mort@owner',
        'parms':{'axis':'mortgage'}},                 
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
 
boundary_tables = {
    '#hh|geo|~size|ten': {
        'tables': '#hh|geo|size|ten',
        'parms': {'axis':'size', 'bounds':(0, 7)}},
    '#st|geo|~rm': {
        'tables': '#st|geo|rm',
        'parms': {'axis':'rooms', 'bounds':(0, 9)}},   
    '#st|geo|~br': {
        'tables': '#st|geo|br',
        'parms': {'axis':'bedrooms', 'bounds':(0, 5)}}}

interpolate_tables = {     
    '#hh|geo|~inc|ten': {
        'tables': '#hh|geo|inc|ten',
        'parms': {'data':'households', 'axis':'income', 'bounds':(0, 200000), 
                  'values':[10000, 25000, 40000, 60000, 80000, 100000, 125000, 150000, 175000]}},
    '#hh|geo|~age|ten': {
        'tables': '#hh|geo|age|ten',
        'parms': {'data':'households', 'axis':'age', 'bounds':(15, 95), 
                  'values':[20, 25, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90]}},
    '#hh|geo|~val@owner': {
        'tables': '#hh|geo|val@owner',
        'parms': {'data':'households', 'axis':'value', 'bounds':(1, 1500000), 
                  'values':[100000, 150000, 200000, 250000, 300000, 350000, 400000, 500000, 750000, 1000000]}},
    '#hh|geo|~rent@renter': {
        'tables': '#hh|geo|rent@renter',
        'parms': {'data':'households', 'axis':'rent', 'bounds':(1, 4000), 
                  'values':[500, 750, 1000, 1250, 1500, 1750, 2000, 2500, 3000]}},
    '#st|geo|yrocc|~age|ten': {
        'tables':'#st|geo|yrocc|age|ten',
        'parms':{'data':'structures', 'axis':'age', 'bounds':(15, 95), 
                 'values':[20, 25, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90]}},
    '#st|geo|~yrocc|~age|ten': {
        'tables':'#st|geo|yrocc|~age|ten',
        'parms':{'data':'structures', 'axis':'yearoccupied', 'bounds':(1980, 2020), 
                 'values':[1985, 1990, 1995, 2000, 2005, 2010, 2015, 2018]}},
    '#st|geo|~yrocc': {
        'tables':'#st|geo|yrocc',
        'parms':{'data':'structures', 'axis':'yearoccupied', 'bounds':(1980, 2020), 
                 'values':[1985, 1990, 1995, 2000, 2005, 2010, 2015, 2018]}},
    '#st|geo|~yrblt': {
        'tables':'#st|geo|yrblt',
        'parms':{'data':'structures', 'axis':'yearbuilt', 'bounds':(1930, 2020), 
                 'values':[1960, 1970, 1980, 1990, 2000, 2005, 2010, 2015, 2018]}},
    '#pop|geo|~cmte': {
        'tables':'#pop|geo|cmte',
        'parms':{'data':'population', 'axis':'commute', 'bounds':(0, 120), 
                 'values':[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]}}}

expansion_tables = {
    '#pop|geo|~age@child': {
        'tables':'#pop|geo|age@child',
        'parms':{'axis':'age', 'bounds':(0, 17), 'consolidate':True}}}

collapse_tables = {
    '#hh|geo|~val': {
        'tables': ['#hh|geo|~val@owner', '#hh|geo|~rent@renter'],
        'parms': {'axis':'value', 'collapse':'rent', 'value':0, 'scope':'tenure'}}}

mapping_tables = {
    '#pop|geo|gradelvl': {
        'tables': '#pop|geo|~age@child',
        'parms' : {'fromaxis':'age', 'toaxis':'gradelevel', 
                   'values':{i:i-5 for i in range(5, 18)}}},
    '#pop|geo|schlvl': {
        'tables': '#pop|geo|~gradelvl',
        'parms' : {'fromaxis':'gradelevel', 'toaxis':'schoollevel', 
                   'values':{'KG|1st|2nd|3rd|4th|5th':'Elementary', '6th|7th|8th':'Middle', '9th|10th|11th|12th':'High'}}}}

grouping_tables = {
    '#pop|geo|~gradelvl': {
        'tables': '#pop|geo|gradelvl',
        'parms': {'axis':'gradelevel', 
                  'values':[(0, 1, 2, 3, 4, 5), (6, 7, 8), (9, 10, 11, 12)]}}}

ratio_tables = {
    'avginc|geo': {
        'tables': ['#agginc|geo', '#hh|geo'],
        'parms': {'data':'avgincome', 'topdata':'aggincome', 'bottomdata':'households'}},          
    'avgval|geo@owner': {
        'tables': ['#aggval|geo@owner', '#hh|geo@owner'],
        'parms': {'data':'avgvalue', 'topdata':'aggvalue', 'bottomdata':'households'}},  
    'avgrent|geo@renter': {
        'tables': ['#aggrent|geo@renter', '#hh|geo@renter'],
        'parms': {'data':'avgrent', 'topdata':'aggrent', 'bottomdata':'households'}}}    

rate_tables = {
    'Δ%avginc|geo': {
        'tables': 'avginc|geo', 
        'parms': {'data':'avgincome', 'axis':'date', 'formatting':{'precision':3, 'multiplier':'%'}}},          
    'Δ%avgval|geo@owner': {
        'tables': 'avgval|geo@owner',
        'parms': {'data':'avgvalue', 'axis':'date', 'formatting':{'precision':3, 'multiplier':'%'}}},   
    'Δ%avgrent|geo@renter': {
        'tables': 'avgrent|geo@renter',
        'parms': {'data':'avgrent', 'axis':'date', 'formatting':{'precision':3, 'multiplier':'%'}}}}        

average_tables = {
    'Δ%avginc': {
        'tables': ['Δ%avginc|geo', '#hh|geo'],
        'parms': {'data':'avgincomerate', 'weightdata':'households', 'axis':'geography'}},
    'Δ%avgval@owner': {
        'tables': ['Δ%avgval|geo@owner', '#hh|geo@owner'],
        'parms': {'data':'avgvaluerate', 'weightdata':'households', 'axis':'geography'}},
    'Δ%avgrent@renter': {
        'tables': ['Δ%avgrent|geo@renter', '#hh|geo@renter'],
        'parms': {'data':'avgrentrate', 'weightdata':'households', 'axis':'geography'}}}
     
 
@process.create(**feed_tables)
def feed_pipeline(tableID, *args, **kwargs):
    queryParms = query(tableID)
    universe, index, header, scope = queryParms['universe'], queryParms['index'], queryParms['header'], queryParms['scope']   
    dataframe = acs_webapi(*args, tableID=tableID, **queryParms, **kwargs)
    dataframe = cleaner(dataframe, *args, **kwargs)   
    flattable = tbls.FlatTable(dataframe, variables=variables, name=tableID)
    arraytable = flattable[[universe, index, header, 'date', *scope.keys()]].unflatten(universe, aggs=AGGS)
    arraytable = arraytable.squeeze(*scope.keys()).sortall(ascending=True).fillneg(np.nan)   
    if header: arraytable = sumcontained(arraytable, axis=header)    
    return arraytable

@process.create(**merge_tables)
def merge_pipeline(tableID, table, other, *args, axis, noncoreaxis=None, **kwargs):
    assert isinstance(other, type(table))
    table = tbls.combinations.merge([table, other], *args, axis=axis, noncoreaxis=noncoreaxis, **kwargs)
    others = [arg for arg in args if isinstance(arg, type(table))]
    for other in others: table = tbls.combinations.append([table, other], *args, axis=axis, **kwargs)
    return table
    
@process.create(**summation_tables)
def summation_pipeline(tableID, table, *args, axis, **kwargs): 
    return summation(table, *args, axis=axis, **kwargs).squeeze(axis)

@process.create(**boundary_tables)
def boundary_pipeline(tableID, table, *args, axis, bounds, **kwargs): 
    return avgconsolidate(table, *args, axis=axis, bounds=bounds, **kwargs)

@process.create(**mapping_tables)
def mapping_pipeline(tableID, table, *args, fromaxis, toaxis, values, **kwargs):
    return table.reaxis(fromaxis, toaxis, values, variables, *args, **kwargs)

@process.create(**grouping_tables)
def grouping_pipeline(tableID, table, *args, axis, values, **kwargs):
    return sumgroup(table, *args, axis=axis, values=values, **kwargs)

@process.create(**interpolate_tables)
def interpolate_pipeline(tableID, table, *args, data, axis, bounds, values, **kwargs):  
    retag = {'{data}/total{data}*total{data}'.format(data=data):data}
    table = boundary(table, *args, axis=axis, bounds=(bounds[0], None), **kwargs)
    total = summation(table, *args, axis=axis, retag={data:'total{}'.format(data)}, **kwargs)
    table = tbls.operations.divide(table, total, *args, axis=axis, **kwargs)
    table = uppercumulate(table, *args, axis=axis, **kwargs)
    table = upperconsolidate(table, *args, axis=axis, **kwargs)
    table = interpolate(table, *args, axis=axis, values=[bounds[0], *values], **kwargs).fillneg(fill=0)  
    table = upperunconsolidate(table, *args, axis=axis, **kwargs)
    table = upperuncumulate(table, *args, axis=axis, total=1, **kwargs) 
    table = tbls.operations.multiply(table, total, *args, noncoreaxis=axis, retag=retag, simplify=True, **kwargs)
    table = boundary(table, *args, axis=axis, bounds=bounds, **kwargs)
    return table

@process.create(**expansion_tables)
def expansion_pipeline(tableID, table, *args, axis, bounds, consolidate=True, **kwargs):
    table = expansion(table, *args, axis=axis, bounds=bounds, **kwargs)
    return table

@process.create(**collapse_tables)
def collapse_pipeline(tableID, table, other, *args, axis, collapse, value, scope, **kwargs):
    other = sumcouple(other, *args, axis=collapse, **kwargs).squeeze(collapse)
    other = other.addscope(axis, value, table.variables[axis])
    table = tbls.combinations.append([table, other], *args, axis=axis, noncoreaxes=[collapse, scope], **kwargs)
    return table

@process.create(**ratio_tables)
def ratio_pipeline(tableID, toptable, bottomtable, *args, data, topdata, bottomdata, **kwargs):
    retag = {'{}/{}'.format(topdata, bottomdata):'{}'.format(data)}
    table = tbls.operations.divide(toptable, bottomtable, *args, retag=retag, **kwargs).fillinf(np.NaN)
    return table
    
@process.create(**rate_tables)
def rate_pipeline(tableID, table, *args, data, axis, **kwargs):
    retag = {'delta{data}/{data}'.format(data=data):'{data}rate'.format(data=data)}
    deltatable = movingdifference(table, *args, axis=axis, retag={data:'delta{}'.format(data)}, **kwargs)
    basetable = table.isel(date=slice(-1))
    table = tbls.operations.divide(deltatable, basetable, *args, retag=retag, **kwargs).fillinf(np.NaN)
    return table
    
@process.create(**average_tables)
def average_pipeline(tableID, table, *args, data, weightdata, axis, **kwargs):
    if not weightdata: return average(table, *args, axis=axis, **kwargs).squeeze(axis)
    weights = args[0] if isinstance(args[0], type(table)) else None  
    weights = normalize(weights.sel(**table.headers), *args, axis=axis, **kwargs)
    table = tbls.operations.multiply(table, weights, *args, **kwargs)
    table = summation(table, *args, axis=axis, retag={'{}*{}'.format(data, weightdata):data}, **kwargs).squeeze(axis)
    return table

            


















