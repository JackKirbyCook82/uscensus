# -*- coding: utf-8 -*-
"""
Created on Thu Dec 5 2019
@name:   USCensus Website Calculation Application
@author: Jack Kirby Cook

"""

import sys

import tables as tbls
import visualization as vis
from tables.processors import Calculation, Renderer
from parsers import ListParser, DictParser
from variables import Geography, Date
from utilities.inputparsers import InputParser

from uscensus.qtilervi import qtilervi_calculation

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['calculation']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


renderer = Renderer(style='double', extend=1)
calculation = Calculation('uscensus', name='USCensus Calculation')
calculation.extend(qtilervi_calculation)
calculation()


def main(*inputArgs, tableID, **inputParms):     
    print(str(inputparser), '\n')  
    print(str(calculation), '\n')
    
    if tableID in calculation: 
        tree = renderer(calculation[tableID])
        table = calculation[tableID](*inputArgs, **inputParms)
        if table: 
            print(str(tree), '\n')
            print(str(table), '\n')
            print(str(table.variables))
    else: print("{} Table Not Supported: '{}'".format(calculation.name, tableID))
    
   
if __name__ == '__main__':  
    tbls.set_options(linewidth=110, maxrows=40, maxcolumns=10, threshold=100, precision=3, fixednotation=True, framechar='=')
    vis.set_options(axisfontsize=8, legendfontsize=8, titlefontsize=12)
    tbls.show_options()
    vis.show_options()
    
    input_dictparser = DictParser(pattern=',|')
    input_listparser = ListParser(pattern=',')
    geography_parser = lambda item: Geography(input_dictparser(item))
    dates_parser = lambda item: [Date.fromstr(value, dateformat='%Y') for value in input_listparser(item)] 
    variable_parsers = {'geography':geography_parser, 'dates':dates_parser}
    inputparser = InputParser(assignproxy='=', spaceproxy='_', parsers=variable_parsers)    
    
    print(repr(inputparser))
    print(repr(calculation), '\n')  
    
    sys.argv.extend(['tableID=', 'geography=', 'dates='])
    inputparser(*sys.argv[1:])
    main(*inputparser.inputArgs, **inputparser.inputParms)
    
    
    