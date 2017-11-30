'''
Created on Oct 5, 2017

@author: VIU53188
@summary: same as the CFTC application except it uses the Athena storage concept
'''

import os
import ntpath

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class CFTCAthena(ApplicationBase):
    '''
    Code to process the CFTC data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(CFTCAthena, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    def ProcessDataSource(self, srcCategory, tables):
        '''
        Process each category
        '''
        self.logger.debug(self.moduleName + " -- " + "ProcessCategory " + " starting ")
        try:
            for year in srcCategory["years"]:
                url = srcCategory["urlPrefix"] + year + "." + srcCategory["urlExt"]
                self.logger.info(self.moduleName + " - Processing url: " + url)

                localFilePath = self.localTempDirectory + "/raw/" +\
                                ntpath.basename(srcCategory["urlPrefix"]) +\
                                year + "." + srcCategory["urlExt"]
                scrubbedFilepath = self.localTempDirectory + "/scrub/" +\
                                ntpath.basename(srcCategory["urlPrefix"]) +\
                                year + "." + srcCategory["urlExt"]

                FileUtilities.DownloadFromURL(url, localFilePath)

                if srcCategory["urlExt"] == "zip":  # Unzip the file if we receive a zip format
                    unzipFilelocation = self.localTempDirectory + "/raw/"
                    self.fileUtilities.UnzipFile(localFilePath, unzipFilelocation)
                    localFilePath = unzipFilelocation + srcCategory["unzipFilename"]
                    scrubbedFilepath = self.localTempDirectory + "/scrub/" + year + "_" + srcCategory["unzipFilename"]

                # Need to clean up the file and add the tags
                tag = srcCategory["srcCategory"] + "," + srcCategory["srcDescription"] + ","
                replacements = {'^': tag, '" ': '"', '#VALUE!': '', r'\.,': ' ,'}
                self.fileUtilities.ReplaceStringInFile(localFilePath, scrubbedFilepath, replacements)

            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            df = SparkUtilities.ReadCSVFile(spark, tables, self.job["delimiter"], True,
                                            self.localTempDirectory + "/scrub/", self.logger)
            if "adjustFormat" in srcCategory:
                for fld in srcCategory["adjustFormat"]:
                    df = SparkUtilities.FormatColumn(df, fld["name"], fld["inputFormat"])
            df.write.parquet(self.fileUtilities.parquet, mode="append")
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/scrub/")
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/raw/")

            self.logger.debug(self.moduleName + " -- " + "ProcessCategory for " + srcCategory["srcCategory"] + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessCategory")
            raise

    def ProcessTables(self, dbCommon, tables):
        '''
        For CFTC there is only one final table.  However, they are multiple source that feed into the table
        '''
        tables = self.job["tables"][0]
        for dataSource in dbCommon:
            self.ProcessDataSource(dataSource, tables)
        self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
        self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)

    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
