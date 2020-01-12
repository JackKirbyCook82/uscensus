# -*- coding: utf-8 -*-
"""
Created on Sat Oct 19 2019
@name:   USCensus Micro Rent/Value/Income Calculations
@author: Jack Kirby Cook

"""

import numpy as np

from tables.processors import Calculation
from tables.tables import FlatTable

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, variable_cleaner, variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['microrvi_calculation']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


feed_tables = {
    'hh|geo|inc@renter': {},
    'hh|geo|inc@owner': {},
    'hh|geo|cost@renter': {},
    'hh|geo|cost@owner@mortgage': {},
    'hh|geo|cost@owner@equity': {},
    'hh|geo|rent@renter': {},
    'hh|geo|val@owner': {},
    'hh|geo|ci@renter': {},
    'hh|geo|ci@owner@mortgage': {},
    'hh|geo|ci@owner@equity': {}}


microrvi_calculation =  Calculation('microrvi', name='Micro Rent/Value/Inccome USCensus')


@microrvi_calculation.create(**feed_tables)
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
    print(str(microrvi_calculation), '\n')
    

if __name__ == '__main__':  
    main()




