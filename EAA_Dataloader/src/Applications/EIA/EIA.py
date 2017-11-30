'''
Main script to process the EIA data
Author - Christopher Lewis
License: IHS - not to be used outside the company
'''

import os
import ntpath
import pandas as pd

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.OSUtilities import OSUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class EIA(ApplicationBase):
    '''
    Code to process the PGCR Air Markets data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(EIA, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))
        self.localDataFilename = None

    @staticmethod
    def PreProcessInputFile(localFilepath, localJsonFilename):
        '''
        Pre process the input file
        '''
        fOut = open(localJsonFilename, 'wb')
        # To make this a proper JSON file add the open close brackets
        fOut.write(b"[")
        previousGoodLine = ""
        with open(localFilepath, 'r') as fIn:
            for currentLine in fIn:
                currentLine = currentLine.replace(
                    b"\0", b"")  # Remove NULL characters
                if b'series_id' in currentLine:  # May need to process other types of records
                    if previousGoodLine:
                        # To make this a proper JSON file each item needs to
                        # end with a comma
                        fOut.write(previousGoodLine.replace(b"]}", b"]},"))
                    previousGoodLine = currentLine

        fOut.write(previousGoodLine)  # No comma for the last line
        # To make this a proper JSON file add the open close brackets
        fOut.write(b"]")
        fOut.close()

    def ComposeFileName(self, fileNameNoExt, suffix):
        '''
        Compose file name in one place
        '''
        return self.localTempDirectory + "/" + fileNameNoExt + "_" + suffix + ".txt"

    def LoadSeriesIntoRedshift(self, fileNameNoExt, rsConnect, suffix):
        '''
        Load series data into redshift
        '''
        localFilename = self.ComposeFileName(fileNameNoExt, suffix)
        fullRedshiftDestTable = self.job["tableName"] + \
            fileNameNoExt + "_" + suffix
        RedshiftUtilities.LoadFileIntoRedshift(rsConnect, self.awsParams.s3, self.logger, self.fileUtilities, localFilename,
                                               self.job["destinationSchema"], fullRedshiftDestTable,
                                               self.job["fileFormat"], self.job["dateFormat"], self.job["delimiter"])

    def AppendRowToSeriesDataFile(self, row):
        '''
        Append row
        '''
        dfDataRow = pd.DataFrame.from_dict(row.data)
        dfDataRow['series_id'] = row.series_id
        with open(self.localDataFilename, 'a') as fileToAppend:
            dfDataRow.to_csv(fileToAppend, sep='|', encoding='utf-8', index=False, header=False)

    def ProcessS3File(self, srcFileNameNoExt):
        '''
        Process the files on S3
        '''
        try:

            url = self.job["urlPrefix"] + \
                srcFileNameNoExt["name"] + "." + self.job["urlExt"]
            self.logger.info(self.moduleName + " - Processing url: " + url)

            localZipFilepath = self.localTempDirectory + "/" + \
                ntpath.basename(self.job["urlPrefix"]) + \
                srcFileNameNoExt["name"] + "." + self.job["urlExt"]

            import urllib
            fileDownload = urllib.URLopener()
            fileDownload.retrieve(url, localZipFilepath)

            self.fileUtilities.UnzipFile(
                localZipFilepath, self.localTempDirectory)
            localFilepath = self.localTempDirectory + "/" + srcFileNameNoExt["name"] + ".txt"

            # -----------------------------------------------------------------------------
            self.logger.info(self.moduleName +
                             " - Pre-Processing file: " + localFilepath)
            localJsonFilepath = self.localTempDirectory + "/" + srcFileNameNoExt["name"] + ".json"
            EIA.PreProcessInputFile(localFilepath, localJsonFilepath)

            # Create tables for each specific file
            sqlTemplateFilename = self.job["sqlScriptPrefix"] + \
                srcFileNameNoExt["name"] + ".sql"
            self.CreateTables(sqlTemplateFilename)

            # -----------------------------------------------------------------------------
            # Run the R program and create the attributes and series data file
            command = self.job["rProgram"] + " --vanilla " + \
                self.location + "/" + "ProcessEIA.R " + localJsonFilepath + ' "' + ','.join(srcFileNameNoExt["attrs"]) + '"'
            OSUtilities.RunCommandAndLogStdOutStdErr(command, self.logger)

            # -----------------------------------------------------------------------------
            self.logger.info(self.moduleName +
                             " - Loading files into Redshift...")
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            self.LoadSeriesIntoRedshift(
                srcFileNameNoExt["name"], rsConnect, "series_attributes")
            self.LoadSeriesIntoRedshift(
                srcFileNameNoExt["name"], rsConnect, "series_data")

            # Cleanup
            rsConnect.close()
        except:
            self.logger.exception("we had an error in EIA on ProcessS3File")
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            for srcFileNameNoExt in self.job["srcFileNamesNoExt"]:
                self.ProcessS3File(srcFileNameNoExt)
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)
            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
