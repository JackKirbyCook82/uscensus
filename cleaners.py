# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 2019
@name:   USCensus Cleaners
@author: Jack Kirby Cook

"""

from parse import parse
from functools import update_wrapper
import math

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Parser']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


_RANGEFORMATS = {'upper':'>{lowernum}', 'center':'{lowernum}-{uppernum}', 'lower':'<{uppernum}'}
_ROUNDING = {'digit':1, 'up':True, 'astype':int}


def rangestr_parser(strvalue, *args, rangeformats=_RANGEFORMATS, **kwargs):
   assert all([direction in rangeformats.keys() for direction in ('upper', 'center', 'lower')])
   rangefunction = lambda numkwargs: [numkwargs.get('lowernum', None), numkwargs.get('uppernum', None)]   
   
   for direction, rangeformat in rangeformats.items():
       unformated = parse(rangeformat, strvalue)             
       if unformated is not None: return rangefunction(unformated.named)      
   raise ValueError(strvalue)


def rangenum_parser(numsvalue, *args, rounding=_ROUNDING, **kwargs):
    astype, digit, roundfunction = rounding['astype'], rounding['digit'], lambda x: math.ceil(x) if rounding['up'] else math.floor(x)
    numfunction = lambda numvalue: astype(roundfunction(astype(numvalue)/digit)) * digit
    return [numfunction(numvalue) if numvalue is not None else None for numvalue in numsvalue]



class USCensus_Parser(object):
    def __init__(self, variables): self.__variables = variables    
    def __getitem__(self, column): return self.registry(column)       

    @property
    def variables(self): return self.__variables    

    def __call__(self, dataframe, *args, universe, index, header, scope, **kwargs):
        parser = self.registry()[header]
        variable = self.variables[header]
        function = lambda x: str(variable(parser(x, *args, **kwargs)))
        dataframe[header] = dataframe[header].apply(function)
        return dataframe
        
    # REGISTER FUNCTIONS
    __registry = {}
    @classmethod
    def registry(cls): return cls.__registry
    @classmethod
    def update(cls, **column_parser): return cls.__registry.update(column_parser)
    
    @classmethod
    def register(cls, *columns, **formatmethods):
        def decorator(function):
            def wrapper(strvalue, *args, **kwargs):    
                assert isinstance(strvalue, str)
                return function(strvalue, *args, **formatmethods, **kwargs)

            update_wrapper(wrapper, function)
            cls.update(**{column:wrapper for column in columns})            
            return wrapper
        return decorator


@USCensus_Parser.register('income', 'value', rounding={'digit':1000, 'up':True, 'astype':int},
                          rangeformats={'upper':'${lowernum}ormore', 'center':'${lowernum}to${uppernum}', 'lower':'lessthan${uppernum}'})
def income_value_parser(strvalue, *args, **kwargs):    
    strvalue = strvalue.replace(" ", "").lower()
    strvalue = strvalue.replace(",", "").lower()
    numsvalue = rangestr_parser(strvalue, *args, **kwargs)  
    numsvalue = rangenum_parser(numsvalue, *args, **kwargs)
    return numsvalue

    
@USCensus_Parser.register('rent', rounding={'digit':10, 'up':True, 'astype':int}, exactformats={'nocashrent':[0, 0]}, 
                          rangeformats={'upper':'${lowernum}ormore', 'center':'${lowernum}to${uppernum}', 'lower':'lessthan${uppernum}'})
def rent_parser(strvalue, *args, exactformats, **kwargs):
    strvalue = strvalue.replace(" ", "").lower()
    strvalue = strvalue.replace(",", "").lower()
    try: numsvalue = rangestr_parser(strvalue, *args, **kwargs)
    except ValueError:
        for rangeformat, rangevalue in exactformats.items():
            if rangeformat == strvalue: numsvalue = rangevalue
    try: numsvalue = rangenum_parser(numsvalue, *args, **kwargs)
    except NameError: raise ValueError(strvalue)
    return numsvalue


@USCensus_Parser.register('age', rounding={'digit':1, 'up':True, 'astype':int},
                          rangeformats={'upper':'{lowernum}yearsandover', 'center':'{lowernum}to{uppernum}years', 'lower':'under{uppernum}years'})
def age_parser(strvalue, *args, **kwargs):
    strvalue = strvalue.replace(" ", "").lower()
    strvalue = strvalue.replace(",", "").lower()
    numsvalue = rangestr_parser(strvalue, *args, **kwargs) 
    numsvalue = rangenum_parser(numsvalue, *args, **kwargs)
    return numsvalue

