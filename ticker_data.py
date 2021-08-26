import pandas as pd
import pathlib
from pathlib import Path
import datetime

from pyEX.client import _INCLUDE_POINTS_RATES


class DataLoader( object ):
    def __init__( self, data_dir ):
        self.ticker = None
        self.path_prefix = None
        self.data_dir = pathlib.Path( data_dir )
        self.minimizeDownload = True
        
        if not self.data_dir.exists():
            return None

    def data( self, ticker, period="daily" ):
        if ticker is None:
            return None

        self.ticker = ticker.strip().upper()

        self.path_prefix = self.data_dir.joinpath( self.ticker )

        if not self.path_prefix.exists():
            self.path_prefix.mkdir( parents=True, exist_ok=True )

        if period == "daily":
            daily_data = self.loader( "daily" )
            if not daily_data.empty:
                return self.formatDailyData( daily_data )
        elif period == "intraday":
            intradayData = self.loader( "intraday" )
            if not intradayData.empty:
                return self.formatIntradayData( intradayData )
        else:
            return None

    def loader( self, period ):
        """
        Reads the .csv file and downloads additional data to keep it up to date if needed
        If no .csv file exists, downloads entire historical data till today
        Does not deal with a corrupted .csv file yet.
        """

        file_name_suffix = "-daily.csv" if period == "daily" else "-intraday-1m.csv"
        file_path = self.path_prefix.joinpath( self.ticker + file_name_suffix )
        
        data = pd.DataFrame()
        last_stored_date = None
        download = False

        today = pd.to_datetime( "today", utc=False ).normalize()
        
        if file_path.exists():
            data = pd.read_csv ( file_path, index_col=0 )

            # Check if csv has up to date data, otherwise we may need to download more        
            sort_key = [ "date" ] if period == "daily" else [ "date", "minute" ]
            data[ "date" ] = pd.to_datetime( data[ "date" ], utc=False )
            data.sort_values( by=sort_key, ascending=True, inplace=True )
            last_row = data.tail( 1 )
            last_stored_date = last_row.iloc[ 0 ][ "date" ]
    
            # For intraday, check if we have a full day worth of data
            # Otherwise, we will delete parial days data and redownload it
            if period != "daily":
                last_stored_min = ( last_row.iloc[ 0 ][ "minute" ] ).strip()
                if last_stored_min != "15:59":
                    print( "Removing incomplete intraday data for date %s" % last_stored_date )
                    i = data[ data [ "date" ] == last_stored_date ].index
                    data.drop( i, inplace=True )
                    last_stored_date -= datetime.timedelta( days=1 )

        if data.empty:
            # Either a csv file didn't exist or there was no content in it
            # In this case, we will download the entire dataset until today.
            start_date = None
            download = True
        elif today > last_stored_date:
                start_date = last_stored_date + datetime.timedelta( days=1 )
                download = True
        else:
            print( "%s data is up to date." % period )

        if download:
            downloaded_data = self.download( start_date=start_date, period=period )
            data = pd.concat( [ data, downloaded_data ] )
            data.to_csv( file_path )

        return data

    def download( self, start_date=None, period="daily" ):
        if period == "daily":
            return self.daily( start_date=start_date )
        elif period == "intraday":
            if self.minimizeDownload and start_date is not None and ( datetime.datetime.now() - start_date ).days < 3:
                return pd.DataFrame()
            else:
                return self.intraday( start_date=start_date )    

    ######################################################################
    # All the data provider specific functions start from here
    ######################################################################
    def formatDailyData( self, df ):
        df = df[ [ 'date', 'close',	'high', 'low',	'open', 'symbol', 'volume' ] ].set_index( 'date' )
        return df

    def formatIntradayData( self, df ):
        df.rename( columns={ 'marketHigh': 'high',
                             'marketLow': 'low',
                             'marketOpen': 'open',
                             'marketClose': 'close',
                             'marketVolume': 'volume' }, inplace=True )
        
        df = df[ [ 'date', 'minute', 'high', 'low', 'open', 'close', 'volume' ] ].set_index( [ 'date', 'minute' ] )
        return df

    def daily( self, start_date=None ):
        timeframe = self.getTimeframe( start_date )

        import pyEX as p
        c = p.Client( api_token="pk_20bcce55e4f041c4b8bdc8bf1c856a08", version="stable" )
        print( "downloading latest daily data." )

        try:
            df = c.chartDF( symbol=self.ticker, timeframe=timeframe, sort="asc" )
        except:
            print( "Failed to download." )
            return pd.DataFrame()

        if start_date:
            end_date = pd.to_datetime( "today" )
            df = df.loc[ start_date : end_date, : ]

        df.reset_index( inplace=True )
        return df

    def intraday( self, start_date=None ):
        import pyEX as p
        c = p.Client( api_token="pk_20bcce55e4f041c4b8bdc8bf1c856a08", version="stable" )

        # iexcloud supports a intraday download of max 30 calendar days 
        MAX_DAYS = 30
        if start_date is None or ( datetime.datetime.now() - start_date ).days > MAX_DAYS:
            start_date = datetime.date.today() - datetime.timedelta( days=MAX_DAYS )

        df=pd.DataFrame()
        for d in pd.date_range( start=start_date, end=datetime.date.today() ):
            print( "downloading intraday data for %s:" % d )
            try:
                downloaded_data = c.intradayDF( symbol=self.ticker, date=d )
            except:
                print( "Failed to download." )
                return pd.DataFrame()

            df = pd.concat( [ df, downloaded_data ] )

        df.reset_index( inplace=True )
        return df

    def getTimeframe( self, start_date ):
        if start_date is None:
            timeframe="max"
        else:
            delta = ( datetime.datetime.now() - start_date ).days
            if 0 <= delta < 6:
                timeframe = "5d"
            elif 6 <= delta < 28:
                timeframe = "1m"
            elif 28 <= delta < 84:
                timeframe = "3m"
            elif 84 <= delta < 168:
                timeframe = "6m"
            elif 168 <= delta < 365:
                timeframe = "1y"
            elif 365 <= delta < 730:
                timeframe = "2y"
            elif 730 <= delta < 1826:
                timeframe = "5y"
            elif 1826 <= delta:
                timeframe = "max"

        return timeframe


class DataLoaderUtils( object ):
    def __init__( self ) -> None:
        super().__init__()
        self.data_dir = "./data"

    def data_update_cache( self, args ):
        loader = DataLoader( self.data_dir )
        saved_minimizeDownload = loader.minimizeDownload
        loader.minimizeDownload = False
        for p in Path( self.data_dir ).iterdir():
            if p.is_dir():
                print( p.name )
                ticker = str( p.name )
                loader.data( ticker, period="daily" )
                loader.data( ticker, period="intraday" )
        loader.minimizeDownload = saved_minimizeDownload

    def download_data( self, args ):
        """Takes a filename as argument and downloads historical data for all symbols in the file
        """
        wl_path = Path( args.strip() )
        if not wl_path.is_file():
            print( "Did not find file." )
            return

        wl = pd.read_csv( wl_path )
        loader = DataLoader( self.data_dir )
        for t in wl[ "Symbols" ]:
            print( t )
            ticker = t.strip()
            try:
                loader.data( ticker, period="daily" )
                loader.data( ticker, period="intraday" )
            except:
                pass