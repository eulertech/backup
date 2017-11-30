'''
Created on Jul 18, 2017

@author: Varun Muriyanat
@license: IHS - not to be used outside the company
'''

import datetime
from dateutil.parser import parse

class DatetimeUtilities(object):
    '''
    Datetime Utilities
    '''

    @staticmethod
    def ConvertToUTC(dt):
        '''
        Converts date time to UTC date time
        '''
        if dt is not None:
            timedelta = datetime.datetime.now() - datetime.datetime.utcnow()
            dt = str(dt).split(".")[0]
            dt = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            dt = dt - timedelta #convert to UTC
        return dt

    @staticmethod
    def ConvertToDT(dt):
        '''
        Converts date time to formatted date time
        '''
        if dt is not None:
            dt = str(dt).split(".")[0]
            dt = datetime.datetime.strptime(str(dt), "%Y-%m-%d %H:%M:%S")
        return dt

    @staticmethod
    def ConvertToSTR(dt):
        '''
        Converts date to string
        '''
        if dt is not None:
            dt = str(dt).split(".")[0]
        return dt

    @staticmethod
    def IsDate(string):
        '''
        just checks to see if the input string is a date or not
        '''
        try:
            parse(string)
            return True
        except ValueError:
            return False
