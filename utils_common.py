import os, sys, code, traceback
import cmd
from functools import wraps
import time

class tcolors:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


timerData = {}

def timer( func ):
    @wraps( func )
    def wrapper( *args, **kwargs ):
        global timerData
        start = time.perf_counter()
        ret = func( *args, **kwargs )
        end = time.perf_counter()
        elapsedTime = end - start
        if func.__name__ not in timerData:
            timerData[ func.__name__ ] = 0
        timerData[ func.__name__ ] += elapsedTime
        return ret
    return wrapper