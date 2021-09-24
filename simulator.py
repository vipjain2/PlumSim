from pathlib import Path
import pandas as pd
import numpy as np
import time
import json
import yaml
import re
from enum import Enum
import plotly.express as px

from simulator_webserver import WebApp
from ticker_data import DataLoaderUtils
from simulator_shell import Shell, ShellConfig
from utils_common import timer, timerData
from trade_engine import TradeEngine

CONFIG_FILE = "./.plumsim.config.json"
STRATEGY_FILE = "./Strategy1.simulate"

########################################################################
# Various classes to hold global settings etc. which can be accessed and 
# manupulated inside disconnected classes
########################################################################
class PlumsimConfig( object ):
    threads = []
    verbose = True
    debuglevel = 0

########################################################################
# Simulator code starts here
########################################################################
class Stats( object ):
    numWin = 0
    numLoss = 0

class Simulator( object ):
    def __init__( self, config, env={} ) -> None:
        self.env = env
        self.trades_master = pd.DataFrame( columns=[ "Date", "SellDate", "Type", "BuyPrice", "SellPrice", "Quantity", "Ticker", "Profit", "Invested", "Profits", "AggregateProfits" ] )
        self.positions_master = pd.DataFrame()
        self.tickers = []
        self.stats = Stats()
        self.params = {}
        self.config = config
        self.cache = {}
        self.strategyInfo = {}
        self._curStrategy = None

    def setTickers( self, args ):
        def processArgs( args ):
            try:
                _args = eval( args )
                is_list = isinstance( _args, list )
            except:
                is_list = False

            if is_list:
                tickers = _args
            else:
                args = args.split()
                tickers = []
                for arg in args:
                    arg = arg.strip( " ,\"[]" )
                    if arg.endswith( ".csv" ):
                        df = pd.read_csv( arg, index_col=0 ).index.values
                    else:
                        ticker = [ arg.upper() ]
                        tickers += ticker
            return set( tickers )

        self.tickers = processArgs( args )
        print( "Item count: {}".format( len( self.tickers ) ) )

    def clearTrades( self, args ):
        self.trades_master = pd.DataFrame()

    def simulate( self, args ):
        start_date = pd.to_datetime( self.params[ "START_DATE" ] )
        end_date = pd.to_datetime( self.params[ "END_DATE" ] )

        threads = []
        for t in self.tickers:
            trader = TradeEngine( t, self.strategyInfo[ self._curStrategy ], self.params, self.config )
            self.cache[ t ] = trader
            buy, sell = ( True, True )
            trader.run( buy, sell )
        
        for t in self.tickers:
            trader = self.cache[ t ] 
            trades = trader.tradeRange( start_date, end_date )

            if trades is not None and not trades.empty:
                self.trades_master = pd.concat( [ self.trades_master, trades ] )

        if self.trades_master.empty:
            print( "No Trades during this period." )
            return

        self.trades_master.sort_values( by=[ "Date" ], inplace=True )

        self.calcPnl( self. trades_master )
        self.showSummary( self.trades_master )

    def calcPnl( self, trades ):
        if trades.empty:
            return

        trades[ 'Invested' ] = self.params[ "INIT_CAP" ]

        if self.params[ "COMPOUND" ]:
            amount = self.params[ "INIT_CAP" ]
            for row in trades.itertuples():
                trades.loc[ row.Index, "Invested" ] = amount
                amount = amount * ( 1 + row.Profit )

        trades[ "Profits" ] =  trades[ "Invested" ] * trades[ "Profit" ] * trades[ 'Quantity' ]
        trades[ "AggregateProfits" ] = trades[ "Profits" ].cumsum()

    def showSummary( self, trades ):
        if trades.empty:
            return
        print( "last 10 trades..." )
        print( trades.tail( 10 ) )
        print( "---------" )

        numWin = trades[ trades[ "Profits" ] > 0 ][ "Profits" ].count()
        numLoss = trades[ trades[ "Profits" ] < 0 ][ "Profits" ].count()
        print( "winning trades: %s" % numWin )
        print( "losing trades: %s" % numLoss )
        print( "winning %: {}".format( round( numWin / ( numWin + numLoss ) * 100 ) ) )

        # Aggregate the statistics for each date
        ntrades = {}
        dailyprofit = {}
        for t in trades.itertuples():
            if t.Date not in ntrades:
                ntrades[ t.Date ] = 0
            if t.Type == "LONG":
                ntrades[ t.Date ] += 1

            if t.Date not in dailyprofit:
                dailyprofit[ t.Date ] = 0
            dailyprofit[ t.Date ] += t.Profit

        print( trades[ "Profits" ].describe().to_string() )
        print( "Total profit: %d" % trades[ "Profits" ].sum() )

    def showPnl( self, args ):
        args = args.split()
        args = [ arg.strip().upper() for arg in args ]

        if not args:
            self.showSummary( self.trades_master )
            return

        if args[ 0 : 2 ] == [ "BY", "TICKER" ]:
            trades = self.trades_master.groupby( [ 'Ticker' ] ).sum()
            print( "---------" )
            print( trades.loc[ : , 'Profits' ].to_string() )

        if args[ 0 : 2 ] == [ "BY", "DAY" ]:
            trades = self.trades_master.groupby( [ 'Date' ] ).sum()
            print( "---------" )
            print( trades.loc[ : , 'Profits' ].to_string() )
            self.custom_fig = px.histogram( trades, x="Profits" )

        if args[ 0 : 2 ] == [ "BY", "INVESTED" ]:
            trades = self.trades_master.groupby( [ 'Date' ] ).sum()
            print( "---------" )
            print( trades.loc[ : , 'Invested' ].to_string() )
            self.custom_fig = px.histogram( trades, x="Invested" )

        elif args[ 0 ] in self.cache:
            start_date = pd.to_datetime( self.params[ "START_DATE" ] )
            end_date = pd.to_datetime( self.params[ "END_DATE" ] )
            trades = self.cache[ args[ 0 ] ].tradeRange( start_date, end_date, consolidate=True )
            self.calcPnl( trades )
            self.showSummary( trades )

    def showTrades( self, args ):
        args = args.split()
        args = [ arg.strip().upper() for arg in args ]

        if not args:
            trades = self.trades_master
            print( trades )
            return

        if args[ 0 ] == "CONSOLIDATE":
            ticker = args[ 1 ]
            consolidate=True
        else:
            ticker = args[ 0 ]
            consolidate=False

        if not ticker or ticker not in self.cache:
            print( f"No data available for {ticker}." )
            return

        start_date = pd.to_datetime( self.params[ "START_DATE" ] )
        end_date = pd.to_datetime( self.params[ "END_DATE" ] )

        trades = self.cache[ ticker ].tradeRange( start_date, end_date, consolidate=consolidate )
        print( trades )

    def showOutliers( self, showBest, args ):
        args = args.split()
        args = [ arg.strip().upper() for arg in args ]

        if not args:
            return

        try:
            n = eval( args[ 0 ] )
        except:
            n = 10

        if showBest:
            print( self.trades_master.nlargest( n, "Profits" ) )
        else:
            print( self.trades_master.nsmallest( n, "Profits" ) )

    def exit( self ):
        pass
        #print( "Exiting simulator" )

    def saveConfig( self, args ):
        _config = {}
        _config[ 'tickers' ] = self.tickers
        
        jsonObject = json.dumps( _config )
        with open( CONFIG_FILE, 'w' ) as f:
            f.write( jsonObject )

    def printParams( self ):
        for k, v in self.params.items():
            print( f"{k} : {v}" )

    def initParams( self ):
        self.params = { "Amount" : 0, "MAX_POSITION_SIZE" : 1.0 }

    def initStrategyInfo( self, name ):
        self.strategyInfo[ name ] = {}
        self.strategyInfo[ name ][ "BUY" ] = {}
        self.strategyInfo[ name ][ "SELL" ] = {}

    def loadStrategy( self, args ):
        timeframe = "Day-All"
        output = None
        stopLoss = None

        def _parseTimeframe( timeframe ):
            p = r"""
            (\d?\d?[a-zA-Z]+)(\d?\d?:?\d?\d?)?          # Match things like Day1, 5min1
            [ ]?-?[ ]?                                  # " - ", optional
            (\d?\d?[a-zA-Z]+)?(\d?\d?:?\d?\d?)?         # second instance of Day2, 5min2 etc.
            """

            m = re.match( p, timeframe, re.VERBOSE )
            d1, t1, d2, t2 = m.groups()
            t1 = 0 if not t1 else int( t1 )
            t2 = 0 if not t2 else int( t2 )

            return ( d1, t1, d2, t2 )

        def _parseCondition( key, value ):
            nonlocal timeframe
            nonlocal output
            nonlocal stopLoss
            if isinstance( value, dict ):
                input = []

                for k, v in value.items():
                    input += [ _parseCondition( k, v ) ]

                # Clean up the list. Remove any empty parameters '()'
                input = [ x for x in input if x != '()' ]

                if "AND" in key:
                    linkword = " and "
                elif "OR" in key:
                    linkword = " or "
                else:
                    linkword = ""

                return "( {} )".format( linkword.join( input ) )

            elif "In" in key:
                return "( {} )".format( value )
            elif "Out" in key:
                output = value
                return "()"
            elif "Timeframe" in key:
                timeframe = value
                return "()"
            elif "SetStopLoss" in key:
                stopLoss = f"min( Open, {value} )"
                return "()"

        def _parseAction( key ):
            try:
                [ action, qty ] = key.split( ',' )
            except:
                action, qty = key, "1"

            action = action.strip().upper()

            div = 1
            if '%' in qty:
                qty = qty.strip( '% ' )
                div = 100

            try:
                qty = eval( qty ) / div
            except:
                print( "Numeric value needed" )
                return None
            return ( action, qty )

        name = args.strip()

        with open( STRATEGY_FILE, 'r') as f:
            yamlDict = yaml.load( f, Loader=yaml.FullLoader )

        if name in yamlDict:
            strategy = yamlDict[ name ]
            self._curStrategy = name
            print( "strategy : {}".format( name ) )
            print( "=====================================" )
        else:
            print( "Strategy not found." )
            return

        cacheKey = f"_{name}"
        if cacheKey in self.cache:
            oldStrategy = self.cache[ cacheKey ]
        self.cache[ cacheKey ] = strategy
        
        self.initStrategyInfo( name )
        self.strategyInfo[ name ][ "code" ] = str( strategy )
        self.initParams()
        
        for key, value in strategy.items():
            if key.upper() == "PARAMS":
                for k, v in value.items():
                    if isinstance( v, str ) and v.strip().endswith( '%' ):
                        v = eval( v.strip( '%' ) ) / 100

                    if k == "MAX_LAVERAGE":
                        k = "MAX_POSITION_SIZE"
                        v = self.params[ k ] * eval( v.strip() )

                    self.params[ k ] = v
                self.printParams()
                continue

            timeframe = "Day-All"
            output = None
            stopLoss = None

            # Pre-processing of conditions goes here, before we start parsing

            # Parsing starts from here
            action, qty = _parseAction( key )
            condition = _parseCondition( key, value )
            timeframe = _parseTimeframe( timeframe )

            # Post-processing and store the parsed conditions
            if "STOP" in action:
                condition = f"Low < ( {condition} )"
                output = f"( {output} ) * ( 1 - DISPERSION )"
            
            # Store the parsed output
            parsedStrategy = ( timeframe, qty, condition, output, stopLoss )

            if "BUY" in action:
                self.strategyInfo[ name ][ "BUY" ][ action ] = parsedStrategy

            if "STOP" in action:
                self.strategyInfo[ name ][ "SELL" ][ action ] = parsedStrategy

            if "SELL" in action:
                self.strategyInfo[ name ][ "SELL" ][ action ] = parsedStrategy
            
            print( "{} : {}".format( key, parsedStrategy ) )
            print( "---" )

    def printStrategy( self, args ):
        with open( STRATEGY_FILE, 'r') as f:
            dictionary = yaml.load( f, Loader=yaml.FullLoader )
            for key, value in dictionary.items():
                print ( "{} : {}\n".format( key, str( value ) ) )

    def printTimerInfo( self ):
        temp = {}
        for k, v in timerData.items():
            temp[ k ] = round( v, 2 )
        print( temp )

if __name__ == "__main__":
    pd.set_option( "display.max_rows", None )

    plumsimConfig = PlumsimConfig()
    simulator = Simulator( config=plumsimConfig )
    webServer = WebApp( simulator )
    webServer.startServer()

    config = ShellConfig()
    config.app = simulator
    config.config = plumsimConfig
    config.utils = DataLoaderUtils()

    shell = Shell( config )
    shell.prompt = '%s>> ' % ( "SIMULATOR" )
    shell._cmdloop( "" )