from os import stat
import numpy as np
import time
from enum import Enum
import re
from re import match, search, findall

class Commands( object ):
    def __init__( self ) -> None:
        super().__init__()
        self.tokens = [ ( r"[^a-zA-Z]([Mm][Aa])(\d\d?\d?)", "movingAvg" ),
                        ( r"[^a-zA-Z]([Ee][Mm][Aa])(\d\d?\d?)", "expMovingAvg" ),
                        ( r"[^a-zA-Z]([Aa][Dd][Rr])(\d\d?\d?)?", "adr" ),
                        ( r"[^a-zA-Z](PrevClose)(\d\d?\d?)?", "prevClose" ),
                        ( r"[^a-zA-Z](PrevDayHigh|PrevHigh)(\d\d?\d?)?", "prevHigh" ),
                        ( r"[^a-zA-Z](PrevDayLow|PrevLow)(\d\d?\d?)?", "prevLow" ),
                        ( r"[^a-zA-Z](GapOpen)(\d\d?\d?)?", "gapOpen" ),
                        ( r"[^a-zA-Z](PrevOpenCloseRange)(\d\d?\d?)?", "prevOpenCloseRange" ),
                        ( r"[^a-zA-Z](PrevRange|PrevHighLowRange)(\d\d?\d?)?", "prevRange" ),
                        ( r"[^a-zA-Z](Range|HighLowRange)(\d\d?\d?)?", "range" ) ] 
    
    def compile( self, code, data ):
        indicators = []
        for token in self.tokens:
            regex, fname = token
            allMatches = set( findall( regex, code ) )
            for m in allMatches:
                indicators += [ ''.join( m ) ]
                func = getattr( self, fname )
                func( data, *m )
        print( f"compiled {indicators}" )

    @staticmethod
    def processLabel( label, n, default=1 ):
        if not n:
            name = label
            n = default
        elif isinstance( n, str ):
            name = f"{label}{n}"
            n = eval( n )
        else:
            name = label
            n = n
        return ( name, n )

    @staticmethod
    def movingAvg( data, label, n ):
        name, n = Commands.processLabel( label, n )
        data[ name ] = data[ 'Close' ].rolling( n ).mean()
    
    @staticmethod
    def expMovingAvg( data, label, n ):
        name, n = Commands.processLabel( label, n )
        data[ name ] = data[ 'Close' ].ewm( span=n ).mean()

    @staticmethod
    def adr( data, label, period ):
        if not period:
            period = 20
            name = f"{label}"
        else:
            name = f"{label}{period}"
            period = eval( period )
        cleanup = []
        if 'Range' not in data:
            Commands.range( data, "Range", "" )
            cleanup = [ 'Range' ]
        
        data[ name ] = round( ( data[ 'Range' ].rolling( period ).mean() ), 4 )
        data.drop( cleanup, axis=1, inplace=True )

    @staticmethod
    def trend( data, label, n ):
        name, n = Commands.processLabel( label, n )
        ma = f"MA{n}"
        data[ name ] = data[ ma ].ewm( span=5 ).mean()

    @staticmethod
    def prevClose( data, label, n ):
        name, n = Commands.processLabel( label, n, 1 )
        data[ name ] = data.shift( periods=n, axis=0 )[ "Close" ]
    
    @staticmethod
    def prevOpen( data, label, n ):
        name, n = Commands.processLabel( label, n, 1 )
        data[ name ] = data.shift( periods=n, axis=0 )[ "Open" ]

    @staticmethod    
    def prevHigh( data, label, n ):
        name, n = Commands.processLabel( label, n, 1 )
        data[ name ] = data.shift( periods=n, axis=0 )[ "High" ]

    @staticmethod    
    def prevLow( data, label, n ):
        name, n = Commands.processLabel( label, n, 1 )
        data[ name ] = data.shift( periods=n, axis=0 )[ "Low" ]

    @staticmethod
    def gapOpen( data, label, n, *kargs, **kwargs ):
        name, n = Commands.processLabel( label, n, 1 )
        cleanup = []
        if 'PrevClose' not in data:
            Commands.prevClose( data, "PrevClose", n )
            cleanup += [ 'PrevClose' ]
        
        data[ name ] = ( data[ 'Open' ] - data[ 'PrevClose' ] ) / data[ 'PrevClose' ]
        data.drop( cleanup, axis=1, inplace=True )

    @staticmethod
    def range( data, label, n, *kargs, **kwargs ):
        name, _ = Commands.processLabel( label, n, 1 )
        data[ name ] = ( data[ 'High' ] / data[ 'Low' ] ) - 1

    @staticmethod
    def prevOpenCloseRange( data, label, n, *kargs, **kwargs ):
        name, n = Commands.processLabel( label, n, 1 )
        cleanup = []
        if 'PrevClose' not in data:
            Commands.prevClose( data, "PrevClose", n )
            cleanup += [ 'PrevClose' ]
        if 'PrevOpen' not in data:
            Commands.prevOpen( data, 'PrevOpen', n )
            cleanup += [ 'PrevOpen' ]

        data[ name ] = ( data[ 'PrevClose' ] - data[ 'PrevOpen' ] ) / data[ 'PrevClose' ]
        data.drop( cleanup, axis=1, inplace=True )

    @staticmethod
    def prevRange( data, label, n, *kargs, **kwargs ):
        name, n = Commands.processLabel( label, n, 1 )
        cleanup = []
        if 'PrevHigh' not in data:
            Commands.prevHigh( data, "PrevHigh", n )
            cleanup += [ 'PrevHigh' ]
        if 'PrevLow' not in data:
            Commands.prevLow( data, 'PrevLow', n )
            cleanup += [ 'PrevLow' ]

        data[ name ] = ( data[ 'PrevHigh' ] - data[ 'PrevLow' ] ) / data[ 'PrevLow' ]
        data.drop( cleanup, axis=1, inplace=True )