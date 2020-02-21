# -*- coding: utf-8 -*-
"""
Created on Thurs Fed 20 2020
@name:   USCensus Housing Supply Side Calculations
@author: Jack Kirby Cook

"""

import numpy as np

from tables.processors import Calculation
from tables.tables import FlatTable

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, variable_cleaner, variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['housing_supply_calculation']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


housing_supply_calculation = Calculation('supply', name='Housing Demand USCensus')

feed_tables = {}


@housing_supply_calculation.create(**feed_tables)
def feed_pipeline(tableID, *args, **kwargs):
    queryParms = query(tableID)
    universe, index, header, scope = queryParms['universe'], queryParms['index'], queryParms['header'], queryParms['scope']   
    dataframe = acs_webapi(*args, tableID=tableID, **queryParms, **kwargs)
    dataframe = variable_cleaner(dataframe, *args, **kwargs)
    flattable = FlatTable(dataframe, variables=variables, name=tableID)
    arraytable = flattable[[universe, index, header, 'date', *scope.keys()]].unflatten(universe)
    arraytable = arraytable.squeeze(*scope.keys()).sortall(ascending=True).fillneg(np.nan)   
    return arraytable


def main():     
    print(str(housing_supply_calculation), '\n')
    

if __name__ == '__main__':  
    main()


