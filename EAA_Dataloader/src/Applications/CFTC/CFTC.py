'''
Created on Jan 25, 2017

@author: VIU53188
@summary: Loads files pulled from CFTC web sites based on year
        in the initial load these values are passed in the form of a json config object
        if the latest pulled file if older then the current years file then we search the web site for
        the current years version of the file

    Process:
        1)  read parameters from config file
        2)  loop thru the list of web links to pull data
        3)  copy the files to S3
        4)  use the script table create into redshift using the names in the config file
        5)  load data from s3 into redshift

    Example of web sites:
         Commitment of traders report - COT
        https://www.theice.com/publicdocs/futures/COTHist2016.csv
        https://www.theice.com/publicdocs/futures/COTHist2015.csv
        https://www.theice.com/publicdocs/futures/COTHist2014.csv
        https://www.theice.com/publicdocs/futures/COTHist2013.csv
        https://www.theice.com/publicdocs/futures/COTHist2012.csv
        https://www.theice.com/publicdocs/futures/COTHist2011.csv

        Futures disaggregated - FUTD
        http://www.cftc.gov/files/dea/history/fut_disagg_txt_2016.zip
        http://www.cftc.gov/files/dea/history/fut_disagg_txt_2015.zip
        http://www.cftc.gov/files/dea/history/fut_disagg_txt_2014.zip
        http://www.cftc.gov/files/dea/history/fut_disagg_txt_2013.zip
        http://www.cftc.gov/files/dea/history/fut_disagg_txt_2012.zip
        http://www.cftc.gov/files/dea/history/fut_disagg_txt_2011.zip

        Commitment of traders report - disaggregated - COTD
        http://www.cftc.gov/files/dea/history/com_disagg_txt_2016.zip
        http://www.cftc.gov/files/dea/history/com_disagg_txt_2015.zip
        http://www.cftc.gov/files/dea/history/com_disagg_txt_2014.zip
        http://www.cftc.gov/files/dea/history/com_disagg_txt_2013.zip
        http://www.cftc.gov/files/dea/history/com_disagg_txt_2012.zip
        http://www.cftc.gov/files/dea/history/com_disagg_txt_2011.zip
'''

import os
import ntpath
import sys
import urllib

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from Applications.Common.ApplicationBase import ApplicationBase
from AACloudTools.S3Utilities import S3Utilities

class CFTC(ApplicationBase):
    '''
    Code to process the CFTC data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(CFTC, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    def BulkUploadToS3(self, srcCategory):
        '''
        Uploads all GZIP files created into S3 to be uploaded later...
        '''
        self.logger.info(self.moduleName + " - Uploading GZIP files to s3 folder...")
        s3Location = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] +\
                     "/" + srcCategory["srcCategory"]
        S3Utilities.CopyItemsAWSCli(self.fileUtilities.gzipFolder,
                                    s3Location,
                                    "--recursive --quiet")


    def ProcessCategory(self, srcCategory, tables):
        '''
        Process each category
        '''
        self.logger.debug(self.moduleName + " -- " + "ProcessCategory " + " starting ")
        try:
            for year in srcCategory["years"]:
                url = srcCategory["urlPrefix"] + year + "." + srcCategory["urlExt"]
                self.logger.info(self.moduleName + " - Processing url: " + url)
    
#                if srcCategory["urlExt"] == "csv":
#                    localFilepath = self.fileUtilities.csvFolder
#                else:
                localFilePath = self.localTempDirectory + "/raw/" +\
                                ntpath.basename(srcCategory["urlPrefix"]) +\
                                year + "." + srcCategory["urlExt"]
                scrubbedFilepath = self.localTempDirectory + "/scrub/" +\
                                ntpath.basename(srcCategory["urlPrefix"]) +\
                                year + "." + srcCategory["urlExt"]
                outputGZ = self.fileUtilities.gzipFolder +\
                           ntpath.basename(srcCategory["urlPrefix"]) +\
                           year + "." + srcCategory["urlExt"] + ".gz"
    
                if sys.version[0] == '2':
                    fileDownload = urllib.URLopener()
                    fileDownload.retrieve(url, localFilePath)
                elif sys.version[0] == '3':
                    fileDownload = urllib.request.urlretrieve(url, localFilePath)
    
                if srcCategory["urlExt"] == "zip":  # Unzip the file if we receive a zip format
                    unzipFilelocation = self.localTempDirectory + "/raw/"
                    self.fileUtilities.UnzipFile(localFilePath, unzipFilelocation)
                    localFilePath = unzipFilelocation + srcCategory["unzipFilename"]
                    scrubbedFilepath = self.localTempDirectory + "/scrub/" + year + "_" + srcCategory["unzipFilename"]
                    outputGZ = self.fileUtilities.gzipFolder + year + "_" + srcCategory["unzipFilename"] + ".gz"
    
                tag = srcCategory["srcCategory"] + "," + \
                    srcCategory["srcDescription"] + ","
                # Need to clean up the file and add the tags
                replacements = {'^': tag, '" ': '"',
                                '#VALUE!': 'NULL', r'\.,': ' ,'}
                self.fileUtilities.ReplaceStringInFile(
                    localFilePath, scrubbedFilepath, replacements)
                self.fileUtilities.GzipFile(scrubbedFilepath, outputGZ)
                self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/raw/")
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/scrub/")
            self.BulkUploadToS3(srcCategory)            
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/gzips/" )
            self.logger.debug(self.moduleName + " -- " + "ProcessCategory for " + srcCategory["srcCategory"] + " finished ")
        except Exception as ex:
            self.logger.exception(self.moduleName + " - we had an error in ProcessCategory")
            raise

    def LoadData(self, srcCategory, tblJson):
        '''
        load the data from s3 into RedShift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " starting ")
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            s3folder = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] + \
                        "/" + srcCategory["srcCategory"]
            fldDelimiter = self.job["delimiter"]
            if "delimiter" in tblJson:
                fldDelimiter = tblJson["delimiter"]
            ignorheader = 0
            if "header" in srcCategory:
                ignorheader = srcCategory["header"]
                

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": tblJson["schemaName"],
                                                 "tableName": tblJson["table"],
                                                 "s3Filename": s3folder,
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": fldDelimiter,
                                                 "ignoreheader": ignorheader
                                             },
                                             self.logger, "N")
            self.logger.info(self.moduleName + " - Finished loading s3 data to " +\
                             tblJson["schemaName"] + '.' +  tblJson["table"])
            rsConnect.close()
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in LoadData")
            raise

    def ProcessCategories(self):
        '''
        Process all the categories
        '''
        for tables in self.job["tables"]:
            fname = self.fileUtilities.CreateTableSql(tables, self.fileUtilities.sqlFolder)
            RedshiftUtilities.PSqlExecute(fname, self.logger)

        for srcCategory in self.job["srcCategories"]:
            self.ProcessCategory(srcCategory, tables)
            self.LoadData(srcCategory, tables)

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
###
#  set up to run create folder
###
#            self.fileUtilities.moduleName = self.moduleName
#            self.fileUtilities.localBaseDirectory = self.localTempDirectory
#            self.fileUtilities.CreateFolders(self.job["folders"])
###            
            self.ProcessCategories()
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)

            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
