'''
Created on Mar 22, 2017

@author: Hector Hernandez
@summary: Loads the OPEC Calendar data excel file pulled from the OPEC web site.

'''

import os
import shutil
import pandas

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class OPEC(ApplicationBase):
    '''
    This class is used to control the data load process from different OPEC file sources.
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(OPEC, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
    def DownloadFile(self, srcCategory):
        '''
        Download the file into the local folder.
        '''
        self.logger.debug(self.moduleName + " -- " + "DownloadFile for file " + srcCategory["fileName"] + " starting ")
        fileNameDest = None
        try:
            scanLen = len(self.fileUtilities.ScanFolder(srcCategory["srcFolder"], srcCategory["fileName"]))
            if scanLen > 0:
                fileNameDest = os.path.join(self.localTempDirectory, srcCategory["fileName"])
                self.logger.info(self.moduleName + " - Copying file from source folder...")
                shutil.copyfile(os.path.join(srcCategory["srcFolder"], srcCategory["fileName"]), fileNameDest)
        except:
            self.logger.exception(self.moduleName + "- we had an error in DownloadFile ")
            raise Exception(self.moduleName + " - " + srcCategory["unzipFilename"] + " file not found in source directory.")
        finally:
            self.logger.debug(self.moduleName + " -- " + "DownloadFile for file " + srcCategory["fileName"] + " starting ")
        return fileNameDest

    def GetCSVFile(self, processingFile, srcCategory):
        '''
        Converts the actual excel file into csv for the worksheet configured.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "GetCSVFile" + " starting ")

            fileNameTxt = processingFile.replace(os.path.splitext(processingFile)[1], ".txt")

            dataFrame = pandas.read_excel(processingFile,
                                          sheetname=srcCategory["worksheetName"],
                                          index_col=None,
                                          na_values=["nd"],
                                          skiprows=srcCategory["skipRows"],
                                          skip_footer=srcCategory["skipFooter"],
                                          converters={"IndexNumber": int})

            dataFrame.to_csv(fileNameTxt, sep=str(srcCategory["delimiter"]), encoding='utf-8', index=False)

            fileNameCsv = fileNameTxt.replace(".txt", ".csv")
            self.fileUtilities.ReplaceIterativelyInFile(fileNameTxt, fileNameCsv, [{"\x96":"-"}])
            self.logger.debug(self.moduleName + " -- " + "GetCSVFile" + " finished ")

            return fileNameCsv
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to convert source file into csv..." + err.message)
            raise Exception(err.message)

    def LoadCategory(self, srcCategory):
        '''
        Process a single category configured in the categories dictionary in the jobConfig.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadCategory" + " starting ")
            processingFile = self.DownloadFile(srcCategory)
            processingCSV = self.GetCSVFile(processingFile, srcCategory)
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            self.logger.debug(self.moduleName + " - Loading file " + processingCSV + "...")

            RedshiftUtilities.LoadFileIntoRedshift(rsConnect,
                                                   self.awsParams.s3,
                                                   self.logger,
                                                   self.fileUtilities,
                                                   processingCSV,
                                                   self.job["destinationSchema"],
                                                   self.job["tableName"] + "_" + srcCategory["srcCategory"],
                                                   srcCategory["fileFormat"],
                                                   srcCategory["dateFormat"],
                                                   srcCategory["delimiter"])
            self.logger.debug(self.moduleName + " -- " + "LoadCategory" + " finished ")
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to load category...")
            raise Exception(err.message)

    def ProcessCategories(self):
        '''
        Controls the flow execution of all categories configured.
        '''
        self.logger.debug(self.moduleName + " -- " + "ProcessCategories" + " starting ")
        for srcCategory in self.job["srcCategories"]:
            if srcCategory["execute"] == "Y":
                self.LoadCategory(srcCategory)
        self.logger.debug(self.moduleName + " -- " + "ProcessCategories" + " finished ")

    def Start(self, logger, moduleName, filelocs):
        '''
        main routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
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
