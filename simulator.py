from os import name
from pathlib import Path
from collections import OrderedDict
import pandas as pd
import numpy as np
import threading
import time
import json
import yaml
import re
import sys, code, traceback
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
from ticker_data import DataLoader, DataLoaderUtils

from simulator_shell import Shell, ShellConfig
from utils_common import timer, timerData

DATA_DIR = "./data"
CONFIG_FILE = "./.plumsim.config.json"
STRATEGY_FILE = "./Strategy1.simulate"

########################################################################
# Various classes to hold global settings etc. which can be accessed and 
# manupulated inside disconnected classes
########################################################################

class PlumsimConfig( object ):
    threads = []
    web_thread_active = True
    intraday = True
    verbose = True

########################################################################
# 
########################################################################
class TradeType( Enum ):
    BUY = 1,
    SELL = 2,
    SHORT = 3,
    COVER = 4

class TradeEngine( object ):
    def __init__( self, ticker, buyStrategy, sellStrategy, params={} ):
        self._ticker = ticker
        self.buyStrategy = buyStrategy
        self.sellStrategy = sellStrategy
        self.params = params
        self.stdin = sys.stdin
        self.stdout = sys.stdout

        self.loader = DataLoader( DATA_DIR )

        self.data = self.loader.data( ticker, period="daily" )
        self.intradayData = self.loader.data( ticker, period="intraday" )

        self.setup()
        self.trades = pd.DataFrame( columns=[ 'Date', 'Ticker', 'Type', 'Strategy', 'Price', 'Quantity' ] )
        self.positions = pd.DataFrame( columns=[ 'Date', 'Ticker', 'Type', 'Strategy', 'Price', 'Quantity' ] )

    def setup( self ):
        self.data.rename( columns={ 'close': 'Close',
                                    'open': 'Open',
                                    'low': 'Low', 
                                    'high': 'High' }, inplace=True )
        self.data.index.rename( 'Date', inplace=True )
        self.data.sort_index( inplace=True )

        self.data[ "PrevClose" ] = self.data.shift( axis=0 )[ "Close" ]
        self.data[ "PrevDayHigh" ] = self.data.shift( axis=0 )[ "High" ]
        self.data[ "GapOpen" ] = self.data.apply( lambda row : ( row[ "Open" ] - row[ "PrevClose" ] ) / row[ "PrevClose" ], axis=1 )

        # Add indicators
        self.calc_ma( [ 10, 15, 20, 50, 65, 100, 200 ] )
        #self.calc_trend( [ 10, 20, 50, 100, 200 ] )
        self.data[ "Range" ] = self.data[ "High" ] / self.data[ "Low" ]
        self.data[ "ADR" ] = round( ( self.data[ "Range" ].rolling( 20 ).mean() - 1 ) * 100, 2 )

    def ticker( self ):
        return self._ticker

    def calc_ma( self, mas ):
        for i in mas:
            ma_name = "MA%d" % i
            self.data[ ma_name ] = self.data[ "Close" ].ewm( span=i ).mean()

    def calc_trend( self, args ):
        for t in args:
            name = "Trend%d" % t
            ma = "MA%d" % t
            self.data[ name ] = self.data[ ma ].ewm( span=5 ).mean()


    def processTimeframe( self, timeframe, date ):
        ( d1, t1, d2, t2 ) = timeframe
        # The following stetement block should be converted to structural pattern matching
        # once we upgrade to Python 3.10
        if ( d1, t1, d2 ) == ( "Day", "1", None ):
            data = self.data.loc[ date : date ]
        elif ( d1, d2 ) == ( "Day", "Day" ):
            pass
        elif ( d1, d2 ) == ( "Day", "All" ):
            data = self.data.loc[ date : ]
        elif ( d1, d2 ) == ( "1Min", "1Min" ):
            data = self.intradayData.loc[ date ]
        else:
            print( "Syntax error in timeframe" )
            data = None
        return data
    
    @timer
    def executeCondition( self, condition, globalVars, localVars ):
        globals = globalVars
        locals = localVars
        condition = f"returnVal={condition}"
        try:
            code = compile( condition + "\n", "<stdin>", "single" )
            saved_stdin = sys.stdin
            saved_stdout = sys.stdout
            sys.stdin = self.stdin
            sys.stdout = self.stdout

            try:
                exec( code, globals, locals )
            finally:
                sys.stdin = saved_stdin
                sys.stdout = saved_stdout
        except:
            exec_info = sys.exc_info()[ :2 ]
            print( traceback.format_exception_only( *exec_info )[ -1 ].strip() )
        return locals[ "returnVal" ]

    @timer
    def findTrade( self, type, data, condition, qty, priceCondition, newStopLoss, env ):
        """Takes a table of daily or intraday price data and finds the first row entry that meets the trading condition

        Inputs - 
            Type : Trade type as in sell, buy, short, cover
            Data : intraday or daily data
        """
        #data = data.to_dict( "index" )
        price = 0
        globals = env
        date = None
        for d in data.itertuples():
            date = d.Index
            locals = d._asdict()

            # Some special processing for sell or cover trades. 
            # Drawdown and stop loss need to be calculcated only once we are in a trade.
            if type == TradeType.SELL or type == TradeType.COVER:                
                drawdown = self.executeCondition( "( Price - Low ) / Price", globals, locals )
                locals.update ( { 'Drawdown' : drawdown } )                

                # Test the stoploss if one exists
                if globals[ 'StopLoss' ] > 0:
                    ret = self.executeCondition( "Low < StopLoss", globals, locals )
                    if ret:
                        price = self.executeCondition( "StopLoss * ( 1 - DISPERSION )", globals, locals )
                        qty = 1             # better Qty is to use the remaining buy qty
                        break

            ret = self.executeCondition( condition, globals, locals )
            if ret:
                price = self.executeCondition( priceCondition, globals, locals )
                if newStopLoss:
                    stoploss = self.executeCondition( f"{newStopLoss}", globals, locals )
                    env[ 'StopLoss' ] = stoploss
                qty = qty
                break
        return ( price, qty, date )

    @timer
    def getBuys( self, strategy ):
        if self.data.empty:
            return
        
        print( "{}: calculating buy trades.".format( self.ticker() ) )

        type = TradeType.BUY
        tradeId = 1
        env = self.params
        for p in self.data.itertuples( index=True ):
            date = p.Index
            
            for name in strategy:
                ( timeframe, qty, condition, priceCondition, stopLoss ) = strategy[ name ]

                data = self.processTimeframe( timeframe, date )

                # Now we walk through data from startTime till endTime and find out if we meet the trade condition
                ( price, qty, _ ) = self.findTrade( TradeType.BUY, data, condition, qty, priceCondition, stopLoss, env )

                if price > 0:
                    self.positions.loc[ tradeId ] = { 'Date': date, 'Type': type, 'Strategy' : name, 'Price': price, 'Quantity': qty, 'Ticker': self.ticker() }
                    tradeId += 1

        self.positions[ 'Date' ] = pd.to_datetime( self.positions[ 'Date' ] )

    @timer
    def getSales( self, strategy ):
        if self.positions.empty:
            return

        print( "{}: calculating sell trades.".format( self.ticker() ) )

        type = TradeType.SELL
        tradeId = 1
        env = self.params
        for p in self.positions.itertuples( index=False ):
            date = p.Date
            totalQty = p.Quantity
            env.update( { 'Price' : p.Price, 'StopLoss' : 0 } )
            
            self.trades.loc[ tradeId ] = p
            tradeId += 1
            
            # Now we apply the sell conditions one by one until the entire position has been sold
            # For each confition, we first fetch the date range that this condition needs to be applied to
            for name in strategy:
                ( timeframe, qty, condition, priceCondition, stopLoss ) = strategy[ name ]

                if totalQty == 0:
                    continue

                data = self.processTimeframe( timeframe, date )
                
                # If the last trade was a partial sell, we need the next day to start executing 
                # a sell strategy on the remaining quantity.
                if totalQty < 1:
                    data = data[ 1 : ]
                
                # Now we walk through data from startTime till endTime 
                # and find out if we meet the sell condition
                ( price, qty, date ) = self.findTrade( TradeType.SELL, data, condition, qty, priceCondition, stopLoss, env )

                if price > 0 and qty > 0:
                    qty = totalQty if totalQty < qty else qty
                    self.trades.loc[ tradeId ] = { 'Date': date, 'Type': type, 'Strategy' : name, 'Price': price, 'Quantity': qty, 'Ticker': self.ticker() }
                    totalQty -= qty
                    tradeId += 1

        self.trades[ 'Date' ] = pd.to_datetime( self.trades[ 'Date' ] )


    def consolidateTrades( self, trades ):
        consolidatedTrades = pd.DataFrame( columns=[ 'Date', 'Ticker', 'Type', 'BuyPrice', 'SellPrice', 'Profit' ] )

        curTradeId = 0
        avgSellPrice = 0
        curQty = 0

        for t in trades.itertuples( index=True ):
            if t.Type == TradeType.BUY:
                consolidatedTrades.loc[ t.Index ] = { 'Date': t.Date, 'BuyPrice': t.Price, 'Type' : 'LONG' }
                if curTradeId == 0:
                    # Initial condition when this BUY trade is the very first trade
                    curTradeId = t.Index
                else:
                    # Every time we hit a BUY, we need to restart
                    avgSellPrice = 0
                    curQty = 0
                    curTradeId = t.Index
            elif t.Type == TradeType.SELL:
                avgSellPrice = ( avgSellPrice * curQty + t.Price * t.Quantity ) / ( curQty + t.Quantity )
                curQty += t.Quantity
                consolidatedTrades.at[ curTradeId, 'SellPrice' ] = avgSellPrice

        consolidatedTrades[ 'Ticker' ] = self.ticker()        
        consolidatedTrades[ 'Profit'] = ( consolidatedTrades[ 'SellPrice' ] - consolidatedTrades[ 'BuyPrice' ] ) / consolidatedTrades[ 'BuyPrice' ]
        return consolidatedTrades


    def tradeRange( self, startDate=None, endDate=None, consolidate=True ):
        if not startDate:
            startDate = self.trades.index[ 0 ]
        if not endDate:
            endDate = self.trades.index[ -1 ]
        trades = self.trades.query( 'Date > @startDate and Date < @endDate', inplace=False )

        #print( trades )
        if consolidate:
            return self.consolidateTrades( trades )
        else:
            return trades

    def trade( self, date, consolidate=True ):
        return self.tradeRange( date, date )


    def run( self, buy=True, sell=True ):
        if buy:
            self.getBuys( self.buyStrategy )
        if sell:
            self.getSales( self.sellStrategy )
        self.cleanup()

    def cleanup( self ):
        pass

class Stats( object ):
    numWin = 0
    numLoss = 0

class Simulator( object ):
    def __init__( self, config, env={} ) -> None:
        self.env = env
        self.trades_master = pd.DataFrame()
        self.buyStrategy = OrderedDict()
        self.sellStrategy = OrderedDict()
        self.tickers = []
        self.stats = Stats()
        self.params = {}
        self.config = config
        self.cache = {}

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
        
    def print_summary( self, trades ):
        print( trades )
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

    def simulate( self, args ):
        start_date = pd.to_datetime( self.params[ "START_DATE" ] )
        end_date = pd.to_datetime( self.params[ "END_DATE" ] )

        threads = []
        for t in self.tickers:
            trader = TradeEngine( t, self.buyStrategy, self.sellStrategy, self.params )
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

        self.calcPnl()
        self.stats.numWin = self.trades_master[ self.trades_master[ "Profits" ] > 0 ][ "Profits" ].count()
        self.stats.numLoss = self.trades_master[ self.trades_master[ "Profits" ] < 0 ][ "Profits" ].count()
        self.print_summary( simulator.trades_master )

    def calcPnl( self, args=None ):
        self.trades_master[ 'Invested' ] = self.params[ "INIT_CAP" ]

        if self.params[ "COMPOUND" ]:
            amount = self.params[ "INIT_CAP" ]
            for row in self.trades_master.itertuples():
                self.trades_master.loc[ row.Index, "Invested" ] = amount
                amount = amount * ( 1 + row.Profit )

        self.trades_master[ "Profits" ] =  self.trades_master[ "Invested" ] * self.trades_master[ "Profit" ]
        self.trades_master[ "AggregateProfits" ] = self.trades_master[ "Profits" ].cumsum()

    def showTrades( self, args ):
        start_date = pd.to_datetime( self.params[ "START_DATE" ] )
        end_date = pd.to_datetime( self.params[ "END_DATE" ] )

        ticker = args.strip().upper()

        if not ticker or ticker not in self.cache:
            return

        trades = self.cache[ ticker ].tradeRange( start_date, end_date, consolidate=False )
        print( trades )

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
        self.params = { 'Amount' : 0 }

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
            elif "StopLoss" in key:
                stoploss = value
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
            strategyDict = yaml.load( f, Loader=yaml.FullLoader )

        if name in strategyDict:
            strategy = strategyDict[ name ]
        else:
            print( "Strategy not found." )
            return

        print( "strategy : {}".format( name ) )
        print( "=====================================" )
        cacheKey = f"_{name}"
        if cacheKey in self.cache:
            oldStrategy = self.cache[ cacheKey ]

        self.cache[ cacheKey ] = strategy
        
        self.initParams()
        for key, value in strategy.items():
            if key.upper() == "PARAMS":
                for k, v in value.items():
                    if isinstance( v, str ) and v.strip().endswith( '%' ):
                        v = eval( v.strip( '%' ) ) / 100
                    self.params[ k ] = v
                self.printParams()

            action, qty = _parseAction( key )

            if "BUY" in key.upper():
                timeframe = "Day-All"
                output = None
                condition = _parseCondition( key, value )
                timeframe = _parseTimeframe( timeframe )
                self.buyStrategy[ action ] = ( timeframe, qty, condition, output, stoploss )
                print( "{} : {}".format( key, self.buyStrategy[ action ] ) )

            if "SELL" in key.upper():
                timeframe = "Day-All"
                output = None
                condition = _parseCondition( key, value )
                timeframe = _parseTimeframe( timeframe )
                self.sellStrategy[ action ] = ( timeframe, qty, condition, output, stoploss )           
                print( "{} : {}".format( key, self.sellStrategy[ action ] ) )
            print( "---" )

    def printStrategy( self, args ):
        with open( STRATEGY_FILE, 'r') as f:
            dictionary = yaml.load( f, Loader=yaml.FullLoader )
            for key, value in dictionary.items():
                print ( "{} : {}\n".format( key, str( value ) ) )

    def printTimerInfo( self ):
        print( timerData )

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