'''
Created on Mar 25, 2017

@author: Hector Hernandez
@summary: Supports the conversion and interaction with .DBF file types.
@attention: requires PERL go to https://www.perl.org/get.html then after
            PERL has been installed go to the cmd window for windows and run command
            cpanm CAM::DBF
'''

import os
import platform
from AACloudTools.OSUtilities import OSUtilities
from AACloudTools.FileUtilities import FileUtilities

class DBFUtilities(object):
    '''
    Supports the conversion and interaction with .DBF file types.
    '''
    def __init__(self, logger):
        '''
        Constructor
        '''
        self.logger = logger
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    @staticmethod
    def GetDBF2CSVPath():
        '''
        Get path to perl script that does the DBF to CSV conversion
        '''
        return FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)) + "/dbf2csv.pl ")

    def ConvertToCSV2(self, fileInName):
        '''
        Convert file to CSV using the perl utility
        ConvertToCSV does not seem to work for some cases to need to resort to perl
        '''
        try:
            cmd = "perl " + DBFUtilities.GetDBF2CSVPath() + fileInName
            if platform.system().lower() == "linux":
                cmd = cmd + " 1>/dev/null"
            elif platform.system().lower() == "windows":
                cmd = cmd + " 1>NUL"
                cmd = cmd.replace("/", "\\")
            else:
                self.logger.exception("OS other than Windows/Linux")
            OSUtilities.RunCommandAndLogStdOutStdErr(cmd, self.logger)
        except Exception as exception:
            self.logger.exception(str(exception))
            raise

    def ConvertToCSV(self, fileInName, fileOutName, delimiter=",", includeHeader=False):
        '''
        Converts a .DBF file into a CSV file.
            - The column names can be included using the includeHeader argument.
            - If not fileOutName is specified, will use the input file name and location to deliver the result.
            - Delimiter is used to separate the field values by default is ","
        '''
        self.logger.error("Starting conversion to csv for file " + fileInName)

        headerOpt = ""

        if includeHeader is True:
            headerOpt = " --Header"

        if delimiter is None or delimiter == "":
            delimiter = ","

        if fileOutName is None:
            fileOutName = ""

        cmd = "dbf2csv " + fileInName + " " + \
            fileOutName + " --Field=" + delimiter + headerOpt

        try:
            OSUtilities.RunCommandAndLogStdOutStdErr(cmd, self.logger)
        except Exception as err:
            self.logger.error("Error while trying to convert to CSV...")
            raise Exception(err.message)
