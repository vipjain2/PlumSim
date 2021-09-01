from pathlib import Path
from collections import OrderedDict
import pandas as pd
import numpy as np
import threading
import time
import json
import yaml
import re
from enum import Enum

from dash_html_components.Hr import Hr
import plotly
import plotly.express as px
import plotly.graph_objs as go
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
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
    web_thread_active = True
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
        self.trades_master = pd.DataFrame()
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
            return tickers

        self.tickers = processArgs( args )

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
        if self.trades_master.empty:
            return
        print( "last 10 trades..." )
        print( trades.tail( 10 ) )

        self.stats.numWin = self.trades_master[ self.trades_master[ "Profits" ] > 0 ][ "Profits" ].count()
        self.stats.numLoss = self.trades_master[ self.trades_master[ "Profits" ] < 0 ][ "Profits" ].count()
        print( "winning trades: %s" % self.stats.numWin )
        print( "losing trades: %s" % self.stats.numLoss )
        print( "winning %: {}".format( round( self.stats.numWin / ( self.stats.numWin + self.stats.numLoss ) * 100 ) ) )

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

        #freq, bins = np.histogram( ntrades.values(), range=[ 0, 15 ] )
        #for b, f in zip( bins[ 1 : ], freq ):
        #    print( round( b, 1 ), ' '.join( np.repeat( '*', f ) ) )

        print( trades[ "Profits" ].describe() )
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

    def showOutliers( self, args ):
        args = args.split()
        args = [ arg.strip().upper() for arg in args ]

        if not args:
            return

        try:
            n = eval( args[ 0 ] )
        except:
            n = 10

        print( self.trades_master.sort_values( by=["Profits"], ascending=False ).tail( n ) )


    def exit( self ):
        print( "Exiting simulator" )

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

    def loadStrategy( self, args ):
        timeframe = "Day-All"
        output = None
        stoploss = None

        def _parseTimeframe( timeframe ):
            p = r"""
            (\d?\d?[a-zA-Z]+)(\d?\d?:?\d?\d?)?          # Match things like Day1, 5min1
            [ ]?-?[ ]?                                  # " - ", optional
            (\d?\d?[a-zA-Z]+)?(\d?\d?:?\d?\d?)?         # second instance of Day2, 5min2 etc.
            """

            m = re.match( p, timeframe, re.VERBOSE )
            return m.groups()

        def _parseCondition( key, value ):
            nonlocal timeframe
            nonlocal output
            nonlocal stoploss
            if isinstance( value, dict ):
                input = []

                for k, v in value.items():
                    input += [ _parseCondition( k, v ) ]

                # Clean up the list. Remove any empty parameters '()'
                input = [ x for x in input if x != '()' ]

                if key == "AND":
                    linkword = " and "
                elif key == "OR":
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
                stoploss = f"min( Open, {value} )"
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

        self.strategyInfo[ name ] = {}
        self.strategyInfo[ name ][ "code" ] = str( strategy )

        cacheKey = f"_{name}"
        if cacheKey in self.cache:
            oldStrategy = self.cache[ cacheKey ]
        self.cache[ cacheKey ] = strategy
        
        self.initParams()
        buyStrategy = OrderedDict()
        sellStrategy = OrderedDict()

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

            action, qty = _parseAction( key )

            if "BUY" in key.upper():
                timeframe = "Day-All"
                output = None
                stoploss = None
                condition = _parseCondition( key, value )
                timeframe = _parseTimeframe( timeframe )
                buyStrategy[ action ] = ( timeframe, qty, condition, output, stoploss )
                self.strategyInfo[ name ][ "buy" ] = buyStrategy
                print( "{} : {}".format( key, buyStrategy[ action ] ) )

            if "SELL" in key.upper():
                timeframe = "Day-All"
                output = None
                stoploss = None
                condition = _parseCondition( key, value )
                timeframe = _parseTimeframe( timeframe )
                sellStrategy[ action ] = ( timeframe, qty, condition, output, stoploss )           
                self.strategyInfo[ name ][ "sell" ] = sellStrategy
                print( "{} : {}".format( key, sellStrategy[ action ] ) )
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

    app = dash.Dash( external_stylesheets=[dbc.themes.BOOTSTRAP] )
    app.layout = dbc.Container(
        [
            html.H1( "Strategy performance for %s" % simulator.tickers ),
            dbc.Tabs( 
                [
                    dbc.Tab( label="performance", tab_id="perf_graph" ),
                    dbc.Tab( label="chart", tab_id="data_chart" )
                ],
                id = "tabs",
                active_tab = "perf_graph",
            ),
            html.Div( id="tab-content", className="p-4" )        
        ]
    )

    @app.callback(
        Output( "tab-content", "children" ),
        Input( "tabs", "active_tab" ),
    )
    def render_tab_content( active_tab ):
        if active_tab == "perf_graph":
            fig = px.line( simulator.trades_master, x="Date", y="AggregateProfits" )
            return dcc.Graph( style={ "width": "120vh", "height": "70vh" }, figure=fig )
        elif active_tab == "data_chart":
            fig = go.Figure( data=go.Ohlc( x=trader.df[ "Date" ],
                                            open=trader.df[ "Open" ],
                                            high=trader.df[ "High" ],
                                            low=trader.df[ "Low" ],
                                            close=trader.df[ "Close" ] ) 
                            )
            #fig.update( layout_xaxis_rangeslider_visible=False )
            return dcc.Graph( style={ "width": "120vh", "height": "70vh" }, figure=fig )

    plumsimConfig.web_thread_active = True
    def start_web_server():
        app.run_server( debug=True, use_reloader=False, dev_tools_hot_reload=False )
        while plumsimConfig.web_thread_active:
            time.sleep( 5 )

    web_thread = threading.Thread( target=start_web_server )
    web_thread.start()

    config = ShellConfig()
    config.app = simulator
    config.config = plumsimConfig
    config.config.threads = [ web_thread ]
    config.utils = DataLoaderUtils()

    shell = Shell( config )
    shell.prompt = '%s>> ' % ( "SIMULATOR" )
    shell._cmdloop( "" )