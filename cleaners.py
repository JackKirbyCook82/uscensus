# -*- coding: utf-8 -*-
"""
Created on Sat Oct 5 2019
@name:   USCensus Cleaners
@author: Jack Kirby Cook

"""

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Variable_Cleaner']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


class USCensus_Variable_Cleaner(object):
    def __init__(self, variables): self.__variables = variables
    @property
    def variables(self): return self.__variables
    
    def __call__(self, dataframe, *args, **kwargs):
        for column in dataframe.columns:
            if column in self.variables.keys(): 
                try: dataframe[column] = dataframe[column].apply(lambda x: str(self.variables[column].fromstr(str(x))))
                except: dataframe[column] = dataframe[column].apply(lambda x: str(self.variables[column](x)))
            else: pass 
        return dataframe