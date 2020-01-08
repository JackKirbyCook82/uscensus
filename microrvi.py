# -*- coding: utf-8 -*-
"""
Created on Sat Oct 19 2019
@name:   USCensus Micro Rent/Value/Income Calculations
@author: Jack Kirby Cook

"""

import numpy as np

from tables.processors import Calculation, Meta
from tables.tables import FlatTable

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, variable_cleaner, variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['microrvi_calculation']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


microrvi_calculation = Calculation('microrvi', name='Micro Rent/Value/Inccome USCensus')


feed_tables = {
    'hh|geo|inc@renter': dict(meta=Meta('households', 'geography', 'income', tenure='Renter')),
    'hh|geo|inc@owner': dict(meta=Meta('households', 'geography', 'income', tenure='Owner')),
    'hh|geo|cost@renter': dict(meta=Meta('households', 'geography', 'cost', tenure='Renter')),
    'hh|geo|cost@owner@mortgage': dict(meta=Meta('households', 'geography', 'cost', tenure='Owner', mortgage='Primary|Secondary|Tertiary')),
    'hh|geo|cost@owner@equity': dict(meta=Meta('households', 'geography', 'cost', tenure='Owner', mortgage='Equity')),
    'hh|geo|rent@renter': dict(meta=Meta('households', 'geography', 'rent', tenure='Renter')),
    'hh|geo|val@owner': dict(meta=Meta('households', 'geography', 'value', tenure='Owner')),
    'hh|geo|ci@renter': dict(meta=Meta('households', 'geography', '%costincome', tenure='Renter')),
    'hh|geo|ci@owner@mortgage': dict(meta=Meta('households', 'geography', '%costincome', tenure='Owner', mortgage='Primary|Secondary|Tertiary')),
    'hh|geo|ci@owner@equity': dict(meta=Meta('households', 'geography', '%costincome', tenure='Owner', mortgage='Equity'))}
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




