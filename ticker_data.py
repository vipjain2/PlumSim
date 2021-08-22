import pandas as pd
import pathlib
import datetime

from pyEX.client import _INCLUDE_POINTS_RATES


class Data_loader( object ):
    def __init__( self, data_dir ):
        self.ticker = None
        self.path_prefix = None
        self.data_dir = pathlib.Path( data_dir )
        self.intraday_off = False
        
        if not self.data_dir.exists():
            return None

    def loader( self, period ):
        """
        Reads the .csv file and downloads additional data to keep it up to date if needed
        If no .csv file exists, downloads entire historical data till today
        Does not deal with a corrupted .csv file yet.
        """
        if period == "intraday" and self.intraday_off:
            return None

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

    def data( self, ticker, period="daily" ):
        if ticker is None:
            return None
        self.ticker = ticker.strip().upper()

        self.path_prefix = self.data_dir.joinpath( self.ticker )

        if not self.path_prefix.exists():
            self.path_prefix.mkdir( parents=True, exist_ok=True )

        daily_data = self.loader( "daily" )
        intraday_data = self.loader( "intraday" )

        if period == "daily" and not daily_data.empty:
            return self.formatDailyData( daily_data )
        elif not intraday_data.empty:
            return self.formatIntradayData( intraday_data )

    def download( self, start_date=None, period="daily" ):
        if period == "daily":
            return self.daily( start_date=start_date )
        elif period == "intraday":
            return self.intraday( start_date=start_date )    


    ######################################################################
    # All the data provider specific functions start from here
    ######################################################################
    def formatDailyData( self, df ):
        df = df[ [ 'date', 'close',	'high', 'low',	'open', 'symbol', 'volume' ] ].set_index( 'date' )
        return df

    def formatIntradayData( self, df ):
            df = df[ [ 'date', 'minute', 'marketHigh', 'marketLow', 'marketOpen', 'marketClose', 'marketVolume' ] ]
            return df.rename( columns={ 'marketHigh': 'high',
                                        'marketLow': 'low',
                                        'marketOpen': 'open',
                                        'marketClose': 'close',
                                        'marketVolume': 'volume' }, inplace=True )

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


class Data_loader_utils( Data_loader ):
    def __init__( self ):
        super.__init__()
