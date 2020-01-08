# -*- coding: utf-8 -*-
"""
Created on Sat Oct 19 2019
@name:   USCensus Macro Rent/Value/Income Calculations
@author: Jack Kirby Cook

"""

import numpy as np

import tables.operations as tblops
from tables.processors import Calculation, Meta
from tables.tables import FlatTable

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, variable_cleaner, variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['macrorvi_calculation']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


macrorvi_calculation = Calculation('macrorvi', name='Macro Rent/Value/Inccome USCensus')


feed_tables = {
    'agginc|geo@renter': dict(meta=Meta('aggincome', 'geography', tenure='Renter')),
    'agginc|geo|mort@owner': dict(meta=Meta('aggincome', 'geography', 'mortgage', tenure='Owner')),
    'aggcost|geo@renter': dict(meta=Meta('aggcost', 'geography', tenure='Renter')),
    'aggcost|geo|mort@owner': dict(meta=Meta('aggcost', 'geography', 'mortgage', tenure='Owner')),
    'aggrent|geo@renter': dict(meta=Meta('aggrent', 'geography', tenure='Renter')),
    'aggval|geo|mort@owner': dict(meta=Meta('aggvalue', 'geography', 'mortgage', tenure='Owner')),
    'hh|geo@renter': dict(meta=Meta('households', 'geography', tenure='Renter')),
    'hh|geo|mort@owner': dict(meta=Meta('households', 'geography', 'mortgage', tenure='Owner'))}
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


select_tables = {
    'agginc|geo@owner@mortgage': dict(tables='agginc|geo|mort@owner', 
                                      meta=Meta('aggincome', 'geography', tenure='Owner', mortgage='Primary|Secondary|Tertiary'), 
                                      parms={'axis':{'mortgage':'Primary|Secondary|Tertiary'}}),
    'agginc|geo@owner@equity': dict(tables='agginc|geo|mort@owner', 
                                    meta=Meta('aggincome', 'geography', tenure='Owner', mortgage='Equity'), 
                                    parms={'axis':{'mortgage':'Equity'}}),
    'aggcost|geo@owner@mortgage': dict(tables='aggcost|geo|mort@owner', 
                                       meta=Meta('aggcost', 'geography', tenure='Owner', mortgage='Primary|Secondary|Tertiary'), 
                                       parms={'axis':{'mortgage':'Primary|Secondary|Tertiary'}}),
    'aggcost|geo@owner@equity': dict(tables='aggcost|geo|mort@owner', 
                                     meta=Meta('aggcost', 'geography', tenure='Owner', mortgage='Equity'), 
                                     parms={'axis':{'mortgage':'Equity'}}),
    'aggval|geo@owner@mortgage': dict(tables='aggval|geo|mort@owner', 
                                      meta=Meta('aggvalue', 'geography', tenure='Owner', mortgage='Primary|Secondary|Tertiary'), 
                                      parms={'axis':{'mortgage':'Primary|Secondary|Tertiary'}}),
    'aggval|geo@owner@equity': dict(tables='aggval|geo|mort@owner', 
                                    meta=Meta('aggvalue', 'geography', tenure='Owner', mortgage='Equity'), 
                                    parms={'axis':{'mortgage':'Equity'}}),
    'hh|geo@owner@mortgage': dict(tables='hh|geo|mort@owner', 
                                  meta=Meta('households', 'geography', tenure='Owner', mortgage='Primary|Secondary|Tertiary'), 
                                  parms={'axis':{'mortgage':'Primary|Secondary|Tertiary'}}),
    'hh|geo@owner@equity': dict(tables='hh|geo|mort@owner', 
                               meta=Meta('households', 'geography', tenure='Owner', mortgage='Equity'), 
                               parms={'axis':{'mortgage':'Equity'}})}
@macrorvi_calculation.create(**select_tables)
def select_pipeline(tableID, table, *args, axis, **kwargs): 
    table = table.sel(**axis)
    for key, value in axis.items(): table = table.squeeze(key) if isinstance(value, str) else table
    return table


average_tables = {
    'avgincome|geo@renter': dict(tables=['agginc|geo@renter', 'hh|geo@renter'], 
                                 meta=Meta('avgincome', 'geography', tenure='Renter'), 
                                 parms={'aggdata':'income', 'countdata':'households', 'formatting':{'multiplier':'K', 'precision':0}}),
    'avgincome|geo@owner@mortgage': dict(tables=['agginc|geo@owner@mortgage', 'hh|geo@owner@mortgage'], 
                                         meta=Meta('avgincome', 'geography', tenure='Owner', mortgage='Primary|Secondary|Tertiary'), 
                                         parms={'aggdata':'income', 'countdata':'households', 'formatting':{'multiplier':'K', 'precision':0}}),
    'avgincome|geo@owner@equity': dict(tables=['agginc|geo@owner@equity', 'hh|geo@owner@equity'], 
                                       meta=Meta('avgincome', 'geography', tenure='Owner', mortgage='Equity'), 
                                       parms={'aggdata':'income', 'countdata':'households', 'formatting':{'multiplier':'K', 'precision':0}}),
    'avgcost|geo@renter': dict(tables=['aggcost|geo@renter', 'hh|geo@renter'], 
                               meta=Meta('avgcost', 'geography', tenure='Renter'), 
                               parms={'aggdata':'cost', 'countdata':'households', 'formatting':{'multiplier':'', 'precision':0}}),
    'avgcost|geo@owner@mortgage': dict(tables=['aggcost|geo@owner@mortgage', 'hh|geo@owner@mortgage'], 
                                       meta=Meta('avgcost', 'geography', tenure='Owner', mortgage='Primary|Secondary|Tertiary'), 
                                       parms={'aggdata':'cost', 'countdata':'households', 'formatting':{'multiplier':'', 'precision':0}}),
    'avgcost|geo@owner@equity': dict(tables=['aggcost|geo@owner@equity', 'hh|geo@owner@equity'], 
                                     meta=Meta('avgcost', 'geography', tenure='Owner', mortgage='Equity'), 
                                     parms={'aggdata':'cost', 'countdata':'households', 'formatting':{'multiplier':'', 'precision':0}}),
    'avgrent|geo@renter': dict(tables=['aggrent|geo@renter', 'hh|geo@renter'], 
                               meta=Meta('avgrent', 'geography', tenure='Renter'), 
                               parms={'aggdata':'rent', 'countdata':'households', 'formatting':{'multiplier':'', 'precision':0}}),
    'avgval|geo@owner@mortgage': dict(tables=['aggval|geo@owner@mortgage', 'hh|geo@owner@mortgage'], 
                                      meta=Meta('avgvalue', 'geography', tenure='Owner', mortgage='Primary|Secondary|Tertiary'), 
                                      parms={'aggdata':'value', 'countdata':'households', 'formatting':{'multiplier':'K', 'precision':0}}),
    'avgval|geo@owner@equity': dict(tables=['aggval|geo@owner@equity', 'hh|geo@owner@equity'], 
                                    meta=Meta('avgvalue', 'geography', tenure='Owner', mortgage='Equity'), 
                                    parms={'aggdata':'value', 'countdata':'households', 'formatting':{'multiplier':'K', 'precision':0}})}
@macrorvi_calculation.create(**average_tables)
def average_pipeline(tableID, aggtable, counttable, *args, aggdata, countdata, formatting, **kwargs):
    return tblops.divide(aggtable, counttable, *args, formatting=formatting, infinity=False, **kwargs).retag(**{'/'.join(['agg'+aggdata, countdata]):'avg'+aggdata})


ratio_tables = {
    'cost@renter/cost@owner|geo@mortgage': dict(tables=['aggcost|geo@renter', 'aggcost|geo@owner@mortgage'],
                                                meta=Meta('rentercost/ownercost', 'geography', tenure='Owner', mortgage='Primary|Secondary|Tertiary'),
                                                parms={'formatting':{'multiplier':'', 'precision':3}, 'factor':1}),
    'cost@renter/cost@owner|geo@equity': dict(tables=['aggcost|geo@renter', 'aggcost|geo@owner@equity'],
                                              meta=Meta('rentercost/ownercost', 'geography', tenure='Owner', mortgage='Equity'),
                                              parms={'formatting':{'multiplier':'', 'precision':3}, 'factor':1}),
    'cost/income|geo@renter': dict(tables=['aggcost|geo@renter', 'agginc|geo@renter'],
                                   meta=Meta('rentercost/renterincome', 'geography', tenure='Renter'),
                                   parms={'formatting':{'multiplier':'', 'precision':3}, 'factor':12}),
    'cost/income|geo@owner@mortgage': dict(tables=['aggcost|geo@owner@mortgage', 'aggcost|geo@owner@mortgage'],
                                           meta=Meta('ownercost/ownerincome', 'geography', tenure='Owner', mortgage='Primary|Secondary|Tertiary'),
                                           parms={'formatting':{'multiplier':'', 'precision':3}, 'factor':12}),
    'cost/income|geo@owner@equity': dict(tables=['aggcost|geo@owner@equity', 'aggcost|geo@owner@equity'],
                                         meta=Meta('ownercost/ownerincome', 'geography', tenure='Owner', mortgage='Equity'),
                                         parms={'formatting':{'multiplier':'', 'precision':3}, 'factor':12})}
@macrorvi_calculation.create(**ratio_tables)
def ratio_pipeline(tableID, toptable, btmtable, *args, factor, formatting, **kwargs):  
    return tblops.divide(toptable, btmtable, *args, formatting=formatting, infinity=False, **kwargs) * factor 


def main():     
    print(str(macrorvi_calculation), '\n')
    

if __name__ == '__main__':  
    main()


