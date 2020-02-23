# -*- coding: utf-8 -*-
"""
Created on Thu Dec 5 2019
@name:   USCensus Website Calculation Application
@author: Jack Kirby Cook

"""

import sys
import os.path
import numpy as np

import tables as tbls
import visualization as vis
from tables.processors import Calculation, Renderer
from parsers import BoolParser, ListParser, DictParser
from variables import Geography, Date
from utilities.inputparsers import InputParser

from uscensus.webquery import query
from uscensus.webtable import acs_webapi, variable_cleaner, variables
from uscensus.display import MapPlotter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['calculation']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(DIR, os.pardir))
SHAPE_DIR = os.path.join(ROOT_DIR, 'resources', 'shapefiles')
EXCEL_FILE = os.path.join(ROOT_DIR, 'USCensusTables.xlsx')

renderer = Renderer(style='double', extend=1)
mapplotter = MapPlotter(SHAPE_DIR, size=(8,8), vintage=2018, colors='YlGn', roads=True)
calculation = Calculation('uscensus', name='USCensus Calculation')


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


@calculation.create(**feed_tables)
def feed_pipeline(tableID, *args, **kwargs):
    queryParms = query(tableID)
    universe, index, header, scope = queryParms['universe'], queryParms['index'], queryParms['header'], queryParms['scope']   
    dataframe = acs_webapi(*args, tableID=tableID, **queryParms, **kwargs)
    dataframe = variable_cleaner(dataframe, *args, **kwargs)   
    flattable = tbls.FlatTable(dataframe, variables=variables, name=tableID)
    arraytable = flattable[[universe, index, header, 'date', *scope.keys()]].unflatten(universe)
    arraytable = arraytable.squeeze(*scope.keys()).sortall(ascending=True).fillneg(np.nan)   
    return arraytable

@calculation.create(**sum_tables)
def sum_pipeline(tableID, table, other, *args, axis, **kwargs):
    return tbls.operations.add(table, other, *args, axis=axis, **kwargs)


def create_spreadsheet(table, *inputArgs, **inputParms): table.flatten().toexcel(EXCEL_FILE)
def create_mapplot(table, *inputArgs, **inputParms): mapplotter(table, *inputArgs, **inputParms)
def display(table, tree, *inputArgs, **inputParms): print('\n'.join([str(tree), str(table), str(table.variables)])) 


def main(*inputArgs, tableID, mapplot=False, spreadsheet=False, **inputParms):   
    calculation()
    print(str(inputparser), '\n')  
    print(str(calculation), '\n')
    
    if tableID in calculation: 
        tree = renderer(calculation[tableID])
        table = calculation[tableID](*inputArgs, **inputParms)
        if spreadsheet: create_spreadsheet(table, *inputArgs, **inputParms)
        if mapplot: create_mapplot(table, *inputArgs, **inputParms)   
        if table: display(table, tree, *inputArgs, **inputParms)
    else: print("{} Table Not Supported: '{}'".format(calculation.name, tableID))
    
   
if __name__ == '__main__':  
    tbls.set_options(linewidth=110, maxrows=40, maxcolumns=10, threshold=100, precision=3, fixednotation=True, framechar='=')
    vis.set_options(axisfontsize=8, legendfontsize=8, titlefontsize=12)
    tbls.show_options()
    vis.show_options()
    
    boolparser, listparser, dictparser = BoolParser(), ListParser(pattern=','), DictParser(pattern=',|')
    geography_parser = lambda item: Geography(dictparser(item))
    dates_parser = lambda items: [Date.fromstr(item) for item in listparser(items)]
    date_parser = lambda item: Date.fromstr(item) if item else None
    variable_parsers = {'geography':geography_parser, 'dates':dates_parser, 'date':date_parser, 'mapplot':boolparser, 'spreadsheet':boolparser}
    inputparser = InputParser(assignproxy='=', spaceproxy='_', parsers=variable_parsers)    
    
    print(repr(inputparser))
    print(repr(renderer))
    print(repr(mapplotter))
    print(repr(calculation), '\n')  
    
    sys.argv.extend(['tableID=',
                     'mapplot=False', 
                     'spreadsheet=False',
                     'geography=state|48,county|157,tract|*,block|*', 
                     'dates=2017,2016,2015'])
    inputparser(*sys.argv[1:])
    main(*inputparser.inputArgs, **inputparser.inputParms)
    
    
    