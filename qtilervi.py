# -*- coding: utf-8 -*-
"""
Created on Sat Oct 19 2019
@name:   USCensus Quantile Rent/Value/Income Calculations
@author: Jack Kirby Cook

"""

import numpy as np

from tables.processors import Calculation, Meta
from tables.tables import FlatTable

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, variable_cleaner, variables

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['qtilervi_calculation']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


qtilervi_calculation = Calculation('qtilervi', name='Quantile Rent/Value/Inccome USCensus')


feedtables = {}
@qtilervi_calculation.create(**feedtables)
def feed_pipeline(tableID, *args, **kwargs):
    queryParms = query(tableID)
    universe, index, header, scope = queryParms['universe'], queryParms['index'], queryParms['header'], queryParms['scope']   
    dataframe = acs_webapi(*args, tableID=tableID, **queryParms, **kwargs)
    dataframe = variable_cleaner(dataframe, *args, **kwargs)
    flattable = FlatTable(dataframe, variables=variables, name=tableID)
    arraytable = flattable[[universe, index, header, 'date', *scope.keys()]].unflatten(universe)
    arraytable = arraytable.squeeze(*scope.keys()).sortall(ascending=True).fillneg(np.nan)   
    return arraytable


qtilervi_calculation()

