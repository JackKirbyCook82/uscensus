# -*- coding: utf-8 -*-
"""
Created on Thu Dec 5 2019
@name:   USCensus Website Calculation Application
@author: Jack Kirby Cook

"""

import sys
import os.path

import tables as tbls
import visualization as vis
from tables.processors import Calculation, Renderer
from parsers import BoolParser, ListParser, DictParser
from variables import Geography, Date
from utilities.inputparsers import InputParser

from uscensus.housingdemand import housing_demand_calculation
from uscensus.housingsupply import housing_supply_calculation
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
calculation += housing_demand_calculation
calculation += housing_supply_calculation
calculation()


def create_spreadsheet(table, *inputArgs, **inputParms): table.flatten().toexcel(EXCEL_FILE)
def create_mapplot(table, *inputArgs, **inputParms): mapplotter(table, *inputArgs, **inputParms)
def display(table, tree, *inputArgs, **inputParms): print('\n'.join([str(tree), str(table), str(table.variables)])) 


def main(*inputArgs, tableID, mapplot=False, spreadsheet=False, **inputParms):     
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
    
    sys.argv.extend(['tableID=hh|geo|child',
                     'mapplot=False', 
                     'spreadsheet=False',
                     'geography=state|48,county|157,tract|*', 
                     'dates=2017,2016,2015'])
    inputparser(*sys.argv[1:])
    main(*inputparser.inputArgs, **inputparser.inputParms)
    
    
    