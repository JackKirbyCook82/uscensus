# -*- coding: utf-8 -*-
"""
Created on Sat Oct 19 2019
@name:   USCensus Macro Rent/Value/Income Calculations
@author: Jack Kirby Cook

"""

import numpy as np

import tables.operations as tblops
from tables.transformations import Scale
from tables.processors import Calculation
from tables.tables import FlatTable

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, variable_cleaner, variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['macrorvi_calculation']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


scale = Scale(how='standardize')


feed_tables = {
    'agginc|geo': {},
    'agginc|geo@renter': {},
    'agginc|geo|mort@owner': {},
    'aggcost|geo@renter': {},
    'aggcost|geo|mort@owner': {},
    'aggrent|geo@renter': {},
    'aggval|geo|mort@owner': {},
    'hh|geo@renter': {},
    'hh|geo|mort@owner': {}}
    
select_tables = {
    'agginc|geo@owner@mortgage': {
        'tables': 'agginc|geo|mort@owner', 
        'parms': {'axis':{'mortgage':'Primary|Secondary|Tertiary'}}},
    'agginc|geo@owner@equity': {
        'tables': 'agginc|geo|mort@owner', 
        'parms': {'axis':{'mortgage':'Equity'}}},
    'aggcost|geo@owner@mortgage': {
        'tables': 'aggcost|geo|mort@owner', 
        'parms': {'axis':{'mortgage':'Primary|Secondary|Tertiary'}}},
    'aggcost|geo@owner@equity': {
        'tables': 'aggcost|geo|mort@owner', 
        'parms': {'axis':{'mortgage':'Equity'}}},
    'aggval|geo@owner@mortgage': {
        'tables': 'aggval|geo|mort@owner', 
        'parms': {'axis':{'mortgage':'Primary|Secondary|Tertiary'}}},
    'aggval|geo@owner@equity': {
        'tables': 'aggval|geo|mort@owner', 
        'parms': {'axis':{'mortgage':'Equity'}}},
    'hh|geo@owner@mortgage': {
        'tables': 'hh|geo|mort@owner', 
        'parms': {'axis':{'mortgage':'Primary|Secondary|Tertiary'}}},
    'hh|geo@owner@equity': {
        'tables': 'hh|geo|mort@owner', 
        'parms': {'axis':{'mortgage':'Equity'}}}}    

sum_tables = {
    'hh|geo@owner': {
        'tables': ['hh|geo@owner@mortgage', 'hh|geo@owner@equity'], 
        'parms': {'axis':'mortgage'}},
    'hh|geo': {
        'tables': ['hh|geo@owner', 'hh|geo@renter'], 
        'parms': {'axis':'tenure'}},
    'aggcost|geo@owner': {
        'tables': ['aggcost|geo@owner@mortgage', 'aggcost|geo@owner@equity'], 
        'parms': {'axis':'mortgage'}},
    'aggcost|geo': {
        'tables': ['aggcost|geo@owner', 'aggcost|geo@renter'], 
        'parms': {'axis':'tenure'}}}
    
average_tables = {
    'avginc|geo': {
        'tables': ['agginc|geo', 'hh|geo'], 
        'parms': {'aggdata':'income', 'countdata':'households', 'maxstdev':5, 'formatting':{'multiplier':'K', 'precision':0}}},        
    'avginc|geo@renter': {
        'tables': ['agginc|geo@renter', 'hh|geo@renter'], 
        'parms': {'aggdata':'income', 'countdata':'households', 'maxstdev':5, 'formatting':{'multiplier':'K', 'precision':0}}},
    'avginc|geo@owner@mortgage': {
        'tables': ['agginc|geo@owner@mortgage', 'hh|geo@owner@mortgage'], 
        'parms': {'aggdata':'income', 'countdata':'households', 'maxstdev':5, 'formatting':{'multiplier':'K', 'precision':0}}},
    'avginc|geo@owner@equity': {
        'tables': ['agginc|geo@owner@equity', 'hh|geo@owner@equity'], 
        'parms': {'aggdata':'income', 'countdata':'households', 'maxstdev':5, 'formatting':{'multiplier':'K', 'precision':0}}},
    'avgcost|geo': {
        'tables': ['aggcost|geo', 'hh|geo'], 
        'parms': {'aggdata':'cost', 'countdata':'households', 'formatting':{'multiplier':'', 'precision':0}}},               
    'avgcost|geo@renter': {
        'tables': ['aggcost|geo@renter', 'hh|geo@renter'], 
        'parms': {'aggdata':'rentercost', 'countdata':'households', 'formatting':{'multiplier':'', 'precision':0}}},
    'avgcost|geo@owner@mortgage': {
        'tables': ['aggcost|geo@owner@mortgage', 'hh|geo@owner@mortgage'], 
        'parms': {'aggdata':'ownercost', 'countdata':'households', 'formatting':{'multiplier':'', 'precision':0}}},
    'avgcost|geo@owner@equity': {
        'tables': ['aggcost|geo@owner@equity', 'hh|geo@owner@equity'], 
        'parms': {'aggdata':'ownercost', 'countdata':'households', 'formatting':{'multiplier':'', 'precision':0}}},
    'avgrent|geo@renter': {
        'tables': ['aggrent|geo@renter', 'hh|geo@renter'], 
        'parms': {'aggdata':'rent', 'countdata':'households', 'formatting':{'multiplier':'', 'precision':0}}},
    'avgval|geo@owner@mortgage': {
        'tables': ['aggval|geo@owner@mortgage', 'hh|geo@owner@mortgage'], 
        'parms': {'aggdata':'value', 'countdata':'households', 'formatting':{'multiplier':'K', 'precision':0}}},
    'avgval|geo@owner@equity': {
        'tables': ['aggval|geo@owner@equity', 'hh|geo@owner@equity'], 
        'parms': {'aggdata':'value', 'countdata':'households', 'formatting':{'multiplier':'K', 'precision':0}}}}    
    
ratio_tables = {
    'avgcost@renter/avgcost@owner@mortgage|geo': {
        'tables': ['avgcost|geo@renter', 'avgcost|geo@owner@mortgage'],
        'parms': {'formatting':{'multiplier':'', 'precision':3}, 'factor':1}},
    'avgcost@renter/avgcost@owner@equity|geo': {
        'tables': ['avgcost|geo@renter', 'avgcost|geo@owner@equity'],
        'parms': {'formatting':{'multiplier':'', 'precision':3}, 'factor':1}},
    'avgcost/avginc|geo': {
        'tables': ['avgcost|geo', 'avginc|geo'],
        'parms': {'formatting':{'multiplier':'', 'precision':3}, 'factor':12}},
    'avgcost/avginc|geo@renter': {
        'tables':['avgcost|geo@renter', 'avginc|geo@renter'],
        'parms': {'formatting':{'multiplier':'', 'precision':3}, 'factor':12}},
    'avgcost/avginc|geo@owner@mortgage': {
        'tables': ['avgcost|geo@owner@mortgage', 'avginc|geo@owner@mortgage'],
        'parms': {'formatting':{'multiplier':'', 'precision':3}, 'factor':12}},
    'avgcost/avginc|geo@owner@equity': {
        'tables': ['avgcost|geo@owner@equity', 'avginc|geo@owner@equity'],
        'parms': {'formatting':{'multiplier':'', 'precision':3}, 'factor':12}}}    


macrorvi_calculation = Calculation('macrorvi', name='Macro Rent/Value/Inccome USCensus')


@macrorvi_calculation.create(**feed_tables)
def feed_pipeline(tableID, *args, **kwargs):
    queryParms = query(tableID)
    universe, index, header, scope = queryParms['universe'], queryParms['index'], queryParms['header'], queryParms['scope']   
    dataframe = acs_webapi(*args, tableID=tableID, **queryParms, **kwargs)
    dataframe = variable_cleaner(dataframe, *args, **kwargs)
    flattable = FlatTable(dataframe, variables=variables, name=tableID)
    arraytable = flattable[[universe, index, header, 'date', *scope.keys()]].unflatten(universe)
    arraytable = arraytable.squeeze(*scope.keys()).sortall(ascending=True).fillneg(np.nan)   
    return arraytable

@macrorvi_calculation.create(**select_tables)
def select_pipeline(tableID, table, *args, axis, **kwargs): 
    table = table.sel(**axis)
    for key, value in axis.items(): table = table.squeeze(key) if isinstance(value, str) else table
    return table

@macrorvi_calculation.create(**sum_tables)
def sum_pipeline(tableID, table, other, *args, axis, **kwargs):
    return tblops.add(table, other, *args, axis=axis, **kwargs)
    
@macrorvi_calculation.create(**average_tables)
def average_pipeline(tableID, aggtable, counttable, *args, aggdata, countdata, maxstdev=None, formatting, **kwargs):
    table = tblops.divide(aggtable, counttable, *args, formatting=formatting, infinity=False, **kwargs)
    table = table.retag(**{'/'.join(['agg'+aggdata, countdata]):'avg'+aggdata})
    if maxstdev: table = table.fillextreme('stdev', threshold=maxstdev, fill=np.nan)
    return table
    
@macrorvi_calculation.create(**ratio_tables)
def ratio_pipeline(tableID, toptable, btmtable, *args, factor, formatting, **kwargs):  
    return tblops.divide(toptable, btmtable, *args, formatting=formatting, infinity=False, **kwargs) * factor 


def main():     
    print(str(macrorvi_calculation), '\n')
    

if __name__ == '__main__':  
    main()


