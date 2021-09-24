import numpy as np
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
                        ( r"[^a-zA-Z](Range|HighLowRange)(\d\d?\d?)?", "range" ),
                        ( r"[^a-zA-Z](DayOfTheWeek|DayOfWeek)(\d\d?\d?)?", "dayOfWeek" ) ]
    
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

    def processLabel( self, label, n, default=1 ):
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

    def depends( *kargsDepends ):
        def wrapper( self, data, *kargs, **kwargs ):
            cleanup = []
            for d in kargsDepends:
                if d not in data:
                    for token in self.tokens:
                        regex, fname = token
                        matches = findall( regex, d )
                        for m in matches:
                            f = getattr( self, fname )
                            f( data, *m )
                cleanup += [ d ]
            func( self, data, *kargs, **kwargs )
            data.drop( cleanup, axis=1, inplace=True )

    ########################################################################
    # Code for indicators starts from here
    ########################################################################
    def dayOfWeek( self, data, label, *kargs, **kwargs ):
        data[ label ] = data.index.strftime( '%A' )

    def movingAvg( self, data, label, n ):
        name, n = self.processLabel( label, n )
        data[ name ] = data[ 'Close' ].rolling( n ).mean()
    
    def expMovingAvg( self, data, label, n ):
        name, n = self.processLabel( label, n )
        data[ name ] = data[ 'Close' ].ewm( span=n ).mean()

    def trend( self, data, label, n ):
        name, n = self.processLabel( label, n )
        ma = f"MA{n}"
        data[ name ] = data[ ma ].ewm( span=5 ).mean()

    def prevClose( self, data, label, n ):
        name, n = self.processLabel( label, n, 1 )
        data[ name ] = data.shift( periods=n, axis=0 )[ "Close" ]
    
    def prevOpen( self, data, label, n ):
        name, n = self.processLabel( label, n, 1 )
        data[ name ] = data.shift( periods=n, axis=0 )[ "Open" ]

    def prevHigh( self, data, label, n ):
        name, n = self.processLabel( label, n, 1 )
        data[ name ] = data.shift( periods=n, axis=0 )[ "High" ]

    def prevLow( self, data, label, n ):
        name, n = self.processLabel( label, n, 1 )
        data[ name ] = data.shift( periods=n, axis=0 )[ "Low" ]

    def range( self, data, label, n, *kargs, **kwargs ):
        name, _ = self.processLabel( label, n, 1 )
        data[ name ] = ( data[ 'High' ] / data[ 'Low' ] ) - 1

    def gapOpen( self, data, label, n, *kargs, **kwargs ):
        name, n = self.processLabel( label, n, 1 )
        cleanup = []
        if 'PrevClose' not in data:
            self.prevClose( data, "PrevClose", n )
            cleanup += [ 'PrevClose' ]
        
        data[ name ] = ( data[ 'Open' ] - data[ 'PrevClose' ] ) / data[ 'PrevClose' ]
        data.drop( cleanup, axis=1, inplace=True )

    def adr( self, data, label, period ):
        if not period:
            period = 20
            name = f"{label}"
        else:
            name = f"{label}{period}"
            period = eval( period )
        cleanup = []
        if 'Range' not in data:
            self.range( data, "Range", 0 )
            cleanup = [ 'Range' ]
        
        data[ name ] = round( ( data[ 'Range' ].rolling( period ).mean() ), 4 )
        data.drop( cleanup, axis=1, inplace=True )

    def prevOpenCloseRange( self, data, label, n, *kargs, **kwargs ):
        name, n = self.processLabel( label, n, 1 )
        cleanup = []
        if 'PrevClose' not in data:
            self.prevClose( data, "PrevClose", n )
            cleanup += [ 'PrevClose' ]
        if 'PrevOpen' not in data:
            self.prevOpen( data, 'PrevOpen', n )
            cleanup += [ 'PrevOpen' ]

        data[ name ] = ( data[ 'PrevClose' ] - data[ 'PrevOpen' ] ) / data[ 'PrevClose' ]
        data.drop( cleanup, axis=1, inplace=True )

    def prevRange( self, data, label, n, *kargs, **kwargs ):
        name, n = self.processLabel( label, n, 1 )
        cleanup = []
        if 'PrevHigh' not in data:
            self.prevHigh( data, "PrevHigh", n )
            cleanup += [ 'PrevHigh' ]
        if 'PrevLow' not in data:
            self.prevLow( data, 'PrevLow', n )
            cleanup += [ 'PrevLow' ]

        data[ name ] = ( data[ 'PrevHigh' ] - data[ 'PrevLow' ] ) / data[ 'PrevLow' ]
        data.drop( cleanup, axis=1, inplace=True )