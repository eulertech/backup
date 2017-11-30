'''
Created on Mar 30, 2017

@author: Hector Hernandez
@summary: Loads Excel files coming from IHS PetroData RIGPOINT into Redshift.
'''
import os
import pandas

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase

class RIGPOINT(ApplicationBase):
    '''
    RIGPOINT utilization report loader
    '''

    def __init__(self):
        '''
        Initial settings
        '''
        super(RIGPOINT, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def LoadXLSToRedshift(self, fileInStage, reportConfig):
        '''
        Load a rigpoint data excel file into redshift
        '''
        rsConnect = None

        try:
            self.logger.debug(self.moduleName + " -- " + "LoadXLSToRedshift" + fileInStage + " starting ")

            fileNameCSV = os.path.splitext(fileInStage)[0] + ".csv"
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)
            dataFrame = pandas.read_excel(fileInStage,
                                          sheetname=reportConfig["excelSheetName"],
                                          index_col=None,
                                          na_values=['NaN'],
                                          skiprows=reportConfig["skipRows"],
                                          skip_footer=reportConfig["skipFooter"])

            dataFrame.to_csv(fileNameCSV, header=False, sep=str(reportConfig["delimiter"]), encoding='utf-8', index=False)

            RedshiftUtilities.LoadFileIntoRedshift(rsConnect,
                                                   self.awsParams.s3,
                                                   self.logger,
                                                   self.fileUtilities,
                                                   fileNameCSV,
                                                   self.job["destinationSchema"],
                                                   self.job["tableName"] + "_" + reportConfig["name"],
                                                   reportConfig["fileFormat"],
                                                   reportConfig["dateFormat"],
                                                   reportConfig["delimiter"])
            self.logger.debug(self.moduleName + " -- " + "LoadXLSToRedshift" + fileInStage + " finished ")

        except Exception as err:
            self.logger.error(self.moduleName + " Error while trying to load file into Redshift: " + err.message)
            raise
        finally:
            if rsConnect is not None:
                rsConnect.close()

    def DownloadFilesFromS3(self):
        '''
        Download all files from the s3 data folder.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "DownloadFilesFromS3" + " starting ")

            S3Utilities.CopyItemsAWSCli("s3://" + self.job["bucketName"] + self.job["s3DataFolder"],
                                        self.localTempDirectory,
                                        "--recursive --quiet")
            self.logger.debug(self.moduleName + " -- " + "DownloadFilesFromS3" + " finished ")
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to download files from s3. Error: " + err.message)
            raise

    def ProcessReports(self):
        '''
        Process all reports
        '''
        self.logger.debug(self.moduleName + " -- " + "ProcessReports" + " starting ")
        for report in self.job["reports"]:
            for fileName in self.fileUtilities.ScanFolder(self.localTempDirectory, None, report["fileInputExt"]):
                if report["fileInputPrefix"] in fileName:
                    self.LoadXLSToRedshift(self.localTempDirectory + "/" + fileName, report)
        self.logger.debug(self.moduleName + " -- " + "ProcessReports" + " finished ")

    def Start(self, logger, moduleName, filelocs):
        '''
        Main controller
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            self.DownloadFilesFromS3()
            self.ProcessReports()
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)
            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
