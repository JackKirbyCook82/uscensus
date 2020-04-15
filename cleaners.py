# -*- coding: utf-8 -*-
"""
Created on Sat Oct 5 2019
@name:   USCensus Cleaners
@author: Jack Kirby Cook

"""

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


class USCensus_Cleaner(object):
    @property
    def variables(self): return self.__variables    
    def __init__(self, variables): self.__variables = variables
  
    def __call__(self, dataframe, *args, **kwargs):
        for column in dataframe.columns:
            if column in self.variables.keys(): 
                try: dataframe[column] = dataframe[column].apply(lambda x: self.variables[column](x).value)   
                except: 
                    try: dataframe[column] = dataframe[column].apply(lambda x: self.variables[column].fromstr(x).value)
                    except: dataframe[column] = dataframe[column].apply(lambda x: self.variables[column].fromstr(str(x)).value) 
        return dataframe