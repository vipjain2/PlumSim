from shell_common import ShellCommon, ShellConfig
import os, signal

class Shell( ShellCommon ):
    def __init__(self, config ):
        super().__init__( config )

    ####################################
    # All do_* command functions go here
    ####################################
    def do_quit( self, args ):
        """Exits the shell
        """
        self.config.app.exit()
        self.message( "Exiting shell" )
        os.kill( os.getpid(), signal.SIGTERM )
        raise SystemExit

    def do_update_cache( self, args ):
        self.config.utils.data_update_cache( args )

    def do_download_data( self, args ):
        self.config.utils.download_data( args )
    
    def do_load_strategy( self, args ):
        self.config.app.read_strategy_file( args )

    def do_set_tickers( self, args ):
        self.config.app.setTickers( args )
    
    do_set_ticker = do_set_tickers

    def do_load_strategy( self, args ):
        self.config.app.loadStrategy( args )

    def do_save_config( self, args ):
        self.config.app.saveConfig( args )

    def do_clear_trades( self, args ):
        self.config.app.clearTrades( args )

    def do_show_trades( self, args ):
        self.config.app.showTrades( args )
    
    def do_show_best( self, args ):
        self.config.app.showOutliers( True, args )

    def do_show_worst( self, args ):
        self.config.app.showOutliers( False, args )

    def do_simulate( self, args ):
        self.config.app.simulate( args )
        
    def do_show_pnl( self, args ):
        self.config.app.showPnl( args )