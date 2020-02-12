# -*- coding: utf-8 -*-
"""
Created on Sat Oct 19 2019
@name:   USCensus Macro Rent/Value/Income Calculations
@author: Jack Kirby Cook

"""

import numpy as np

from tables.processors import Calculation
from tables.tables import FlatTable

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, variable_cleaner, variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['macrorvi_calculation']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


macrorvi_calculation = Calculation('macrorvi', name='Macro Rent/Value/Inccome USCensus')


feed_tables = {}


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


def main():     
    print(str(macrorvi_calculation), '\n')
    

if __name__ == '__main__':  
    main()


