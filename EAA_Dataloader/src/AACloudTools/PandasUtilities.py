'''
Created on May 11, 2017

@author: Varun Muriyanat
'''

import re
import numpy as np

class PandasUtilities(object):
    '''
    Utilities class for Pandas handling
    '''

    def __init__(self, logger):
        '''
        Constructor
        '''
        self.logger = logger

    def RemoveNonAsciiCharacters(self, df):
        '''
        Replace non ascii characters with empty string
        '''
        for col in list(df.columns):
            try:
                if df[col].dtype is np.dtype('O'):
                    df[col] = df[col].fillna("")
                    df[col] = df[col].apply(
                        lambda x: re.sub(r'[^\x00-\x7F]+', '', x))
            except Exception:
                self.logger.exception(
                    "Exception in PandasUtilities.RemoveNonAsciiCharacters")
                self.logger.exception("Problem column: {}".format(col))
                raise
        return df

    def ReplaceCharacters(self, df, dictArray):
        '''
        #Iteratively replaces the mappings given in a list from each line in an input dataframe.
        #This is used when the replacement has to happen in a particular order.
        #Eg, when the newline character has to be replaced before another set of mappings
        #dict_array = [{"\n":""}, {"[~*}": "|", "{~*]": "\n"}]
        '''
        for col in list(df.columns):
            try:
                if df[col].dtype is np.dtype('O'):
                    df[col] = df[col].fillna("")
                    for src, target in dictArray.iteritems():
                        df[col] = df[col].apply(
                            lambda x: re.sub(re.escape(src), target, x)) # pylint: disable=cell-var-from-loop
            except Exception:
                self.logger.exception(
                    "Exception in PandasUtilities.ReplaceCharacters")
                self.logger.exception("Problem column: {}".format(col))
                raise
        return df

    @staticmethod
    def ConvertDateTimeToObject(df):
        '''
        Athena does not support date in Parquet format.  For now convert to text
        '''
        dfCopy = df.copy()
        for columnName, column in dfCopy.iteritems():
            # https://docs.scipy.org/doc/numpy-1.13.0/reference/arrays.dtypes.html
            if column.dtype.kind == 'M':
                fmt = '%Y-%m-%d'
                dfCopy[columnName] = dfCopy[columnName].apply(lambda x: x.strftime(fmt))
        return dfCopy
