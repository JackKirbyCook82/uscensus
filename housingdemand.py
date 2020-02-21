# -*- coding: utf-8 -*-
"""
Created on Thurs Fed 20 2020
@name:   USCensus Housing Demand Side Calculations
@author: Jack Kirby Cook

"""

import numpy as np

import tables.operations as tblops
from tables.processors import Calculation
from tables.tables import FlatTable

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, variable_cleaner, variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['housing_demand_calculation']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


housing_demand_calculation = Calculation('demand', name='Housing Demand USCensus')

feed_tables = {
    'hh|geo|inc@renter': {},
    'hh|geo|inc@owner': {},
    'hh|geo|size@renter': {}, 
    'hh|geo|size@owner': {},
    'pop|geo|age@male': {},
    'pop|geo|age@female': {},
    'hh|geo|child': {}}

sum_tables = {
    'hh|geo|inc': dict(tables=['hh|geo|inc@renter', 'hh|geo|inc@owner'], parms={'axis':'tenure'}),
    'hh|geo|size': dict(tables=['hh|geo|size@renter', 'hh|geo|size@owner'], parms={'axis':'tenure'}),
    'pop|geo|age': dict(tables=['pop|geo|age@male', 'pop|geo|age@female'], parms={'axis':'sex'})}


@housing_demand_calculation.create(**feed_tables)
def feed_pipeline(tableID, *args, **kwargs):
    queryParms = query(tableID)
    universe, index, header, scope = queryParms['universe'], queryParms['index'], queryParms['header'], queryParms['scope']   
    dataframe = acs_webapi(*args, tableID=tableID, **queryParms, **kwargs)
    dataframe = variable_cleaner(dataframe, *args, **kwargs)   
    flattable = FlatTable(dataframe, variables=variables, name=tableID)
    arraytable = flattable[[universe, index, header, 'date', *scope.keys()]].unflatten(universe)
    arraytable = arraytable.squeeze(*scope.keys()).sortall(ascending=True).fillneg(np.nan)   
    return arraytable

@housing_demand_calculation.create(**sum_tables)
def sum_pipeline(tableID, table, other, *args, axis, **kwargs):
    return tblops.add(table, other, *args, axis=axis, **kwargs)


def main():     
    print(str(housing_demand_calculation), '\n')
    

if __name__ == '__main__':  
    main()




