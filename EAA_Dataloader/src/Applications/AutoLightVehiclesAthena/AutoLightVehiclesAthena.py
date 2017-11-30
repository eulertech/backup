'''
Main script to process the EIA data
Author - Christopher Lewis
License: IHS - not to be used outside the company
'''

import os
import ntpath
import re
import pandas as pd

from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.PandasUtilities import PandasUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class AutoLightVehiclesAthena(ApplicationBase):
    '''
    Code to process the Auto Light Vehicles data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(AutoLightVehiclesAthena, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    def ProcessCatalogs(self, dbCommon, catalog):
        '''
        Process each file
        '''
        # Load the data from the S3 data lake into Redshift using Athena/Redshift Spectrum
        s3Key = dbCommon["s3SrcDirectory"] + "/" + catalog["s3Filename"]
        self.logger.info(self.moduleName + " - Processing file: " + s3Key)
        
        FileUtilities.EmptyFolderContents(self.fileUtilities.gzipFolder) # Clear the folder from the previous run
        FileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)   # Clear the folder from the previous run
        fileName = ntpath.basename(s3Key)
        localGzipFilepath = self.fileUtilities.gzipFolder + "/" + fileName
        S3Utilities.S3Copy(s3Key, localGzipFilepath)

        localExcelFilepath = self.fileUtilities.csvFolder + "/" + fileName
        # Remove the gz extension
        localExcelFilepath = re.sub(r'\.gz$', '', localExcelFilepath)
        self.fileUtilities.GunzipFile(localGzipFilepath, localExcelFilepath)

        # Don't have a raw excel reader for Spark so use Pandas
        self.logger.info(self.moduleName +
                         " - Processing Excel file: " + localExcelFilepath)
        pandasDf = pd.read_excel(localExcelFilepath, catalog["excelSheetName"], index_col=None,
                           na_values=['NaN'], skiprows=catalog["skipRows"])
        pandasDf = PandasUtilities.ConvertDateTimeToObject(pandasDf)

        spark = SparkUtilities.GetCreateSparkSession(self.logger)
        table = catalog["tables"][0] # There is only table in a catalog
        schema = SparkUtilities.BuildSparkSchema(table)
        df = spark .createDataFrame(pandasDf, schema)
        df = SparkUtilities.ConvertNanToNull(df)
        SparkUtilities.SaveParquet(df, self.fileUtilities)
        self.UploadFilesCreateAthenaTablesAndSqlScripts(table, self.fileUtilities.parquet)
        self.LoadDataFromAthenaIntoRedShiftS3Scripts(table)
        self.logger.debug(self.moduleName + " -- " + "ProcessS3File for file: " + s3Key + " finished.\n\n")

    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
