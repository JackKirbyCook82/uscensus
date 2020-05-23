# -*- coding: utf-8 -*-
"""
Created on Thu Dec 5 2019
@name:   USCensus Website Calculations
@author: Jack Kirby Cook

"""

import numpy as np
from scipy import stats

import tables as tbls
from tables.processors import CalculationProcess, CalculationRenderer
from tables.transformations import Scale, Boundary, Moving, Reduction, GroupBy, Expansion, Extension, Cumulate, Uncumulate, Consolidate, Unconsolidate, Interpolate

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
normalize = Scale(how='normalize')
upperconsolidate = Consolidate(how='cumulate', direction='upper')
avgconsolidate = Consolidate(how='average', weight=0.5)
upperunconsolidate = Unconsolidate(how='cumulate', direction='upper')
uppercumulate = Cumulate(how='upper')
upperuncumulate = Uncumulate(how='upper')
sumcouple = Reduction(how='summation', by='couple')
summation = Reduction(how='summation', by='summation')
average = Reduction(how='average', by='summation')
sumcontained = GroupBy(how='contains', agg='sum', ascending=True)
sumgroup = GroupBy(how='groups', agg='sum', ascending=True)
movingdifference = Moving(how='difference', by='minimum', period=1)
expansion = Expansion(how='division')
extension = Extension(how='distribution')
interpolate = Interpolate(how='linear', fill='extrapolate')


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
    '#pop|geo|pi@age1': {},
    '#pop|geo|pi@age2': {},
    '#pop|geo|pi@age3': {},
    '#pop|geo|pi@age4': {},
    '#pop|geo|pi@age5': {},
    '#pop|geo|pi@age6': {},
    '#pop|geo|pi@age7': {},
    '#pop|geo|pi@age8': {},
    '#pop|geo|pi@age9': {},
    '#pop|geo|pi@age10': {},
    '#pop|geo|age@male': {}, 
    '#pop|geo|age@female': {}, 
    '#pop|geo|age@youth': {},
    '#pop|geo|race': {},
    '#pop|geo|origin': {}, 
    '#pop|geo@age1@lang': {},    
    '#pop|geo|eng@age1@lang1': {},
    '#pop|geo|eng@age1@lang2': {},
    '#pop|geo|eng@age1@lang3': {},
    '#pop|geo|eng@age1@lang4': {},
    '#pop|geo@age2@lang': {},    
    '#pop|geo|eng@age2@lang1': {},
    '#pop|geo|eng@age2@lang2': {},
    '#pop|geo|eng@age2@lang3': {},
    '#pop|geo|eng@age2@lang4': {},
    '#pop|geo@age3@lang': {},    
    '#pop|geo|eng@age3@lang1': {},
    '#pop|geo|eng@age3@lang2': {},
    '#pop|geo|eng@age3@lang3': {},
    '#pop|geo|eng@age3@lang4': {},
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
    '#pop|geo|pi|age': {
        'tables': ['#pop|geo|pi@age1', '#pop|geo|pi@age2', '#pop|geo|pi@age3', '#pop|geo|pi@age4', '#pop|geo|pi@age5', '#pop|geo|pi@age6', '#pop|geo|pi@age7', '#pop|geo|pi@age8', '#pop|geo|pi@age9', '#pop|geo|pi@age10'],
        'parms': {'axis':'age'}},    
    '#pop|geo|age|sex': {
        'tables': ['#pop|geo|age@male', '#pop|geo|age@female'],
        'parms': {'axis':'sex'}},     
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
        'parms': {'axis':'tenure'}},
    '#pop|geo|age@lang': {
        'tables': ['#pop|geo@age1@lang', '#pop|geo@age2@lang' , '#pop|geo@age3@lang'],
        'parms': {'axis':'age'}},        
    '#pop|geo|eng|age|@lang1': {
        'tables': ['#pop|geo|eng@age1@lang1', '#pop|geo|eng@age2@lang1' , '#pop|geo|eng@age3@lang1'],
        'parms': {'axis':'age'}},
    '#pop|geo|eng|age|@lang2': {
        'tables': ['#pop|geo|eng@age1@lang2', '#pop|geo|eng@age2@lang2' , '#pop|geo|eng@age3@lang2'],
        'parms': {'axis':'age'}},
    '#pop|geo|eng|age|@lang3': {
        'tables': ['#pop|geo|eng@age1@lang3', '#pop|geo|eng@age2@lang3' , '#pop|geo|eng@age3@lang3'],
        'parms': {'axis':'age'}},
    '#pop|geo|eng|age|@lang4': {
        'tables': ['#pop|geo|eng@age1@lang4', '#pop|geo|eng@age2@lang4' , '#pop|geo|eng@age3@lang4'],
        'parms': {'axis':'age'}},
    '#pop|geo|eng|age|lang': {
        'tables': ['#pop|geo|age@lang', '#pop|geo|eng|age|@lang1' , '#pop|geo|eng|age|@lang2', '#pop|geo|eng|age|@lang3', '#pop|geo|eng|age|@lang4'],
        'parms': {'axis':'language', 'coreaxis':'english', 'fill':0}}}

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
    '#hh|geo': {
        'tables':'#hh|geo|ten',
        'parms':{'axis':'tenure'}},  
    '#pop|geo': {
        'tables':'#pop|geo|race',
        'parms':{'axis':'race'}},   
    '#st|geo': {
        'tables':'#st|geo|unit',
        'parms':{'axis':'unit'}},         
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
        'parms':{'axis':'age'}},
    '#pop|geo|pi': {
        'tables':'#pop|geo|pi|~age',
        'parms':{'axis':'age'}},
    '#st|geo|sqft': {
        'tables':'#st|geo|unit|sqft',
        'parms':{'axis':'unit'}},
    '#pop|geo|eng|lang': {
        'tables':'#pop|geo|eng|age|lang',
        'parms':{'axis':'age'}},     
    '#pop|geo|lang': {
        'tables':'#pop|geo|eng|lang',
        'parms':{'axis':'english'}}, 
    '#pop|geo|eng': {
        'tables':'#pop|geo|eng|lang',
        'parms':{'axis':'language'}}}

boundary_tables = {
    '#hh|geo|~size|ten': {
        'tables': '#hh|geo|size|ten',
        'parms': {'axis':'size', 'bounds':(0, 7)}},
    '#st|geo|~rm': {
        'tables': '#st|geo|rm',
        'parms': {'axis':'rooms', 'bounds':(0, 9)}},   
    '#st|geo|~br': {
        'tables': '#st|geo|br',
        'parms': {'axis':'bedrooms', 'bounds':(0, 5)}},
    '#st|geo|~sqft': {
        'tables': '#st|geo|sqft',
        'parms': {'axis':'sqft', 'bounds':(0, 4000)}}}

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
                 'values':[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]}},
    '#pop|geo|pi|~age': {
        'tables':'#pop|geo|pi|age',
        'parms':{'data':'population', 'axis':'age', 'bounds':(0, 95), 
                 'values':[20, 25, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90]}},}

expansion_tables = {
    '#pop|geo|~age@youth': {
        'tables':'#pop|geo|age@youth',
        'parms':{'axis':'age', 'bounds':(0, 17)}},
    '#pop|geo|~age': {
        'tables':'#pop|geo|age',
        'parms':{'axis':'age', 'bounds':(0, 95)}}}

extension_tables = {
    '#st|geo|unit|sqft': {
        'tables':'#st|geo|unit', 'bounds':(0, None),
        'parms':{'axis':'sqft', 'basis':'unit', 
                 'values':[300, 500, 750, 1000, 1250, 1500, 1750, 2000, 2250, 2500, 3000],   
                 'functions': {'House':lambda size: np.round(stats.beta(2, 2, loc=1000, scale=4000-1000).rvs(int(size)) / 25) * 25,
                               'Apartment':lambda size: np.round(stats.beta(2, 2, loc=200, scale=1500-200).rvs(int(size)) / 25) * 25, 
                               'Mobile':lambda size: np.round(stats.beta(2, 2, loc=100, scale=800-100).rvs(int(size)) / 25) * 25, 
                               'Vehicle':lambda size: np.round(stats.beta(2, 2, loc=0, scale=100-0).rvs(int(size)) / 25) * 25}}}}

collapse_tables = {
    '#hh|geo|equity': {
        'tables': ['#hh|geo|~val@owner', '#hh|geo|~rent@renter'],
        'parms': {'axis':'value', 'collapse':'rent', 'value':0, 'tag':'value', 'scope':'tenure'}},
    '#hh|geo|lease': {
        'tables': ['#hh|geo|~rent@renter', '#hh|geo|~val@owner'],
        'parms': {'axis':'rent', 'collapse':'value', 'value':0, 'tag':'rent', 'scope':'tenure'}}}

mapping_tables = {
    '#pop|geo|gradelvl': {
        'tables': '#pop|geo|~age@youth',
        'parms' : {'fromaxis':'age', 'toaxis':'gradelevel', 
                   'values':{i:i-5 for i in range(5, 18)}}},
    '#pop|geo|schlvl': {
        'tables': '#pop|geo|~gradelvl',
        'parms' : {'fromaxis':'gradelevel', 'toaxis':'schoollevel', 
                   'values':{'KG|1st|2nd|3rd|4th|5th':'Elementary', '6th|7th|8th':'Middle', '9th|10th|11th|12th':'High'}}},
    '#pop|geo|inclvl': {
        'tables': '#pop|geo|~pi',
        'parms' : {'fromaxis':'%poverty', 'toaxis':'incomelevel', 
                   'values':{'<100%':'Poverty', '100%|200%':'NonLiving', '200%|300%':'Living', '300%|400%':'Thriving', '>500%':'Wealthy'}}}}

grouping_tables = {
    '#pop|geo|~gradelvl': {
        'tables': '#pop|geo|gradelvl',
        'parms': {'axis':'gradelevel', 'values':[(0, 1, 2, 3, 4, 5), (6, 7, 8), (9, 10, 11, 12)]}},
    '#pop|geo|~pi' : {
        'tables': '#pop|geo|pi',
        'parms': {'axis':'%poverty', 'values':[1, 2, 3, 4, 5]}}}

ratio_tables = {
    'avginc|geo': {
        'tables': ['#agginc|geo', '#hh|geo'],
        'parms': {'data':'avgincome', 'topdata':'aggincome', 'bottomdata':'households', 'fill':np.nan}},          
    'avgval|geo@owner': {
        'tables': ['#aggval|geo@owner', '#hh|geo@owner'],
        'parms': {'data':'avgvalue', 'topdata':'aggvalue', 'bottomdata':'households', 'fill':np.nan}},  
    'avgrent|geo@renter': {
        'tables': ['#aggrent|geo@renter', '#hh|geo@renter'],
        'parms': {'data':'avgrent', 'topdata':'aggrent', 'bottomdata':'households', 'fill':np.nan}},
    'pop/hh|geo': {
        'tables': ['#pop|geo', '#hh|geo'],
        'parms': {'topdata':'population', 'bottomdata':'households', 'fill':np.nan}},
    'pop/str|geo': {
        'tables': ['#pop|geo', '#st|geo'],
        'parms': {'topdata':'population', 'bottomdata':'structures', 'fill':np.nan}}}    

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
   
average_tables = {}    

 
@process.create(**feed_tables)
def feed_pipeline(tableID, *args, **kwargs):
    queryParms = query(tableID)
    universe, index, header, scope = queryParms['universe'], queryParms['index'], queryParms['header'], queryParms['scope']   
    dataframe = acs_webapi(*args, tableID=tableID, **queryParms, **kwargs)
    dataframe = cleaner(dataframe, *args, **kwargs)   
    flattable = tbls.FlatTable(dataframe, variables=variables, name=tableID)
    arraytable = flattable[[universe, index, header, 'date', *scope.keys()]].unflatten(universe, aggs=AGGS)
    for axis in scope.keys(): arraytable = arraytable.squeeze(axis).sortall(ascending=True).fillneg(np.nan)   
    if header: arraytable = sumcontained(arraytable, axis=header)    
    return arraytable

@process.create(**merge_tables)
def merge_pipeline(tableID, table, other, *args, axis, noncoreaxis=None, coreaxis=None, fill=None, **kwargs):
    table = tbls.combinations.merge([table, other], *args, axis=axis, noncoreaxis=noncoreaxis, coreaxis=coreaxis, **kwargs)
    others = [arg for arg in args if isinstance(arg, type(table))]
    for other in others: table = tbls.combinations.append([table, other], *args, axis=axis, noncoreaxis=noncoreaxis, coreaxis=coreaxis, **kwargs)
    if fill is not None: table = table.fillna(fill)
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
def expansion_pipeline(tableID, table, *args, axis, bounds, **kwargs):
    table = expansion(table, *args, axis=axis, bounds=bounds, **kwargs)
    return table

@process.create(**extension_tables)
def extension_pipeline(tableID, table, *args, axis, basis, functions, **kwargs):
    functions = {(key if isinstance(key, (tuple)) else (key,)):value for key, value in functions.items()}
    table = table.addscope(axis, variables[axis].fromall(), variables[axis])
    tables = [extension(table.sel(**{basis:item}), *args, axis=axis, basis=basis, function=functions[item.value], **kwargs) for item in table.headers[basis]]
    table = tables.pop(0)
    if not tables: return table
    else: table = tbls.combinations.merge([table, tables.pop(0)], *args, axis=basis, **kwargs)
    for other in tables: table = tbls.combinations.append([table, other], *args, axis=basis, **kwargs)
    return table

@process.create(**collapse_tables)
def collapse_pipeline(tableID, table, other, *args, axis, collapse, tag, value, scope, **kwargs):
    other = sumcouple(other, *args, axis=collapse, **kwargs).squeeze(collapse)
    other = other.addscope(axis, table.variables[axis](value), table.variables[axis])
    table = tbls.combinations.append([table, other], *args, axis=axis, noncoreaxes=[collapse, scope], **kwargs)
    table = table.retag(**{axis:tag})
    return table

@process.create(**ratio_tables)
def ratio_pipeline(tableID, toptable, bottomtable, *args, data=None, topdata, bottomdata, fill=None, **kwargs):
    retag = {'{}/{}'.format(topdata, bottomdata):'{}'.format(data)} if data else {}
    table = tbls.operations.divide(toptable, bottomtable, *args, retag=retag, **kwargs)
    if fill is not None: table = table.fillinf(np.NaN)
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

            


















