# -*- coding: utf-8 -*-
"""
Created on Thu Dec 5 2019
@name:   USCensus Website Real Estate Demand Side
@author: Jack Kirby Cook

"""

import numpy as np

import tables as tbls
from tables.processors import Calculation

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, variable_cleaner, variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['demand_calculations']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


demand_calculations = Calculation('demand', name='USCensus Real Estate Demand Calculations')


feed_tables = {
    'hh|geo|inc': {},
    'hh|geo|inc@renter': {},
    'hh|geo|inc@owner': {}, 
    'hh|geo|size@renter': {}, 
    'hh|geo|size@owner': {}, 
    'hh|geo|yrocc@renter': {}, 
    'hh|geo|yrocc@owner': {}, 
    'hh|geo|age@renter': {}, 
    'hh|geo|age@owner': {}, 
    'hh|geo|age@owner@mortgage': {}, 
    'hh|geo|age@owner@equity': {}, 
    'hh|geo|child@renter': {}, 
    'hh|geo|child@owner': {}, 
    'pop|geo|age@male': {}, 
    'pop|geo|age@female': {}, 
    'pop|geo|race': {},
    'hh|geo|cost@renter': {},
    'hh|geo|cost@owner': {},
    'hh|geo|cost@owner@mortgage': {},
    'hh|geo|cost@owner@equity': {},
    'hh|geo|rent@renter': {},
    'hh|geo|val@owner': {},
    'pop|geo|edu@male@age1': {},
    'pop|geo|edu@male@age2': {},
    'pop|geo|edu@male@age3': {},
    'pop|geo|edu@male@age4': {},
    'pop|geo|edu@male@age5': {},
    'pop|geo|edu@female@age1': {},
    'pop|geo|edu@female@age2': {},
    'pop|geo|edu@female@age3': {},
    'pop|geo|edu@female@age4': {},
    'pop|geo|edu@female@age5': {}}
    

sum_tables = {
    'hh|geo|size': {
        'tables': ['hh|geo|size@renter', 'hh|geo|size@owner'], 
        'parms': {'axis':'tenure'}},
    'hh|geo|yrocc': {
        'tables': ['hh|geo|yrocc@renter', 'hh|geo|yrocc@owner'], 
        'parms': {'axis':'tenure'}},
    'hh|geo|age': {
        'tables': ['hh|geo|age@renter', 'hh|geo|age@owner'], 
        'parms': {'axis':'tenure'}},
    'hh|geo|child': {
        'tables': ['hh|geo|child@renter', 'hh|geo|child@owner'], 
        'parms': {'axis':'tenure'}},
    'pop|geo|age': {
        'tables': ['pop|geo|age@male', 'pop|geo|age@female'], 
        'parms': {'axis':'sex'}},
    'pop|geo|edu@age1': {
        'tables': ['pop|geo|edu@male@age1', 'pop|geo|edu@female@age1'], 
        'parms': {'axis':'sex'}},
    'pop|geo|edu@age2': {
        'tables': ['pop|geo|edu@male@age2', 'pop|geo|edu@female@age2'], 
        'parms': {'axis':'sex'}},
    'pop|geo|edu@age3': {
        'tables': ['pop|geo|edu@male@age3', 'pop|geo|edu@female@age3'], 
        'parms': {'axis':'sex'}},
    'pop|geo|edu@age4': {
        'tables': ['pop|geo|edu@male@age4', 'pop|geo|edu@female@age4'], 
        'parms': {'axis':'sex'}},
    'pop|geo|edu@age5': {
        'tables': ['pop|geo|edu@male@age5', 'pop|geo|edu@female@age5'], 
        'parms': {'axis':'sex'}},
    'pop|geo|edu@age1|5': {
        'tables': ['pop|geo|edu@age1', 'pop|geo|edu@age2', 'pop|geo|edu@age3', 'pop|geo|edu@age4', 'pop|geo|edu@age5'], 
        'parms': {'axis':'age'}}}


@demand_calculations.create(**feed_tables)
def feed_pipeline(tableID, *args, **kwargs):
    queryParms = query(tableID)
    universe, index, header, scope = queryParms['universe'], queryParms['index'], queryParms['header'], queryParms['scope']   
    dataframe = acs_webapi(*args, tableID=tableID, **queryParms, **kwargs)
    dataframe = variable_cleaner(dataframe, *args, **kwargs)   
    flattable = tbls.FlatTable(dataframe, variables=variables, name=tableID)
    arraytable = flattable[[universe, index, header, 'date', *scope.keys()]].unflatten(universe)
    arraytable = arraytable.squeeze(*scope.keys()).sortall(ascending=True).fillneg(np.nan)   
    return arraytable

@demand_calculations.create(**sum_tables)
def sum_pipeline(tableID, table, *args, axis, **kwargs):
    others = [arg for arg in args if isinstance(arg, type(table))]
    for other in others: table = tbls.operations.add(table, other, *args, axis=axis, **kwargs)
    return table












