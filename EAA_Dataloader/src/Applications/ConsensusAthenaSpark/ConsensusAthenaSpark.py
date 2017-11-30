'''
Created on Mar 22, 2017

@author: viu53188
@summary: Loads the OPEC Calendar data excel file pulled from the OPEC web site.
        Modified from Consensus to use the AthenaSpark process
'''

import os
import shutil

from datetime import datetime
import calendar
import pandas

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase
from xlrd import XLRDError

class ConsensusAthenaSpark(ApplicationBase):
    '''
    This class is used to control the data load process from different OPEC file sources.
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(ConsensusAthenaSpark, self).__init__()
        self.rawFolder = None
        self.fileUtilities = FileUtilities(self.logger)
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def BulkDownload(self):
        '''
        Download all files.
        '''
        sharedFiles = self.fileUtilities.ScanFolder(self.job["srcSharedFolder"])

        self.logger.info(self.moduleName + " - Downloading files from shared folder...")

        for fileName in sharedFiles:
            if (fileName[:2] == self.job["fileNamePrefix"]) and os.path.splitext(fileName)[1] in self.job["validExts"]:
                shutil.copyfile(os.path.join(self.job["srcSharedFolder"], fileName), self.rawFolder + "/" + fileName)

    def DfCleanUp(self, df, surveyDateVal):
        '''
        Converts the actual excel file into csv for the worksheet configured.
        '''
        bankNameColumnIn = "Unnamed: 0"
        surveyDateColName = "surveyDate"

        for colName in self.job["columnsToDrop"]:
            df = df.drop(colName, 1)

        df = df.drop(self.job["dropAfterHeader"], 0)

        for colName in df.head(0):
            dtTest = colName

            if not isinstance(dtTest, datetime) and colName != bankNameColumnIn:
                df = df.drop(colName, 1)

        df = df.assign(surveyDate=surveyDateVal)

        newOrder = [surveyDateColName]

        for colName in df.head(0):
            if colName != surveyDateColName:
                newOrder.append(colName)

        df = df[newOrder]
        df = df.melt(id_vars=[surveyDateColName, bankNameColumnIn])

        return df

    def GetData(self, rawFileName, mode=None):
        '''
        Returns the data frame or survey date
        '''

        if mode == "getSurveyDate":
            skipRows = 0
        else:
            skipRows = self.job["skipRows"]

        df = pandas.read_excel(rawFileName,
                               sheetname=self.job["worksheetName"],
                               index_col=None,
                               na_values=["na"],
                               skiprows=skipRows,
                               skip_footer=self.job["skipFooter"])

        if mode == "getSurveyDate":
            valRerturn = df.iloc[self.job["surveyDateRow"]-2][0]
        else:
            valRerturn = df

        return valRerturn

    @staticmethod
    def FormatSurveyDate(emFile):
        '''
        Returns the date based on the file's name
        '''
        surveyDateColValue = os.path.splitext(emFile)[0]
        surveyDateColValue = surveyDateColValue[2:len(surveyDateColValue)]
        surveyDateColValue = surveyDateColValue.replace("CF", "")
        surveyDateColValue = str(surveyDateColValue[3:]) + "-" + str(list(calendar.month_abbr).index(surveyDateColValue[:3])) + "-01"
        return surveyDateColValue

    def ProcessFiles(self):
        '''
        Controls the workflow for the conversion, clean up and pack of the input files.
        '''
        filesToProcess = self.fileUtilities.ScanFolder(self.rawFolder)

        for emFile in filesToProcess:
            self.logger.info(self.moduleName + " - Processing file: " + emFile)

            rawFileName = self.rawFolder + "/" + emFile
            csvFilename = self.fileUtilities.csvFolder + os.path.splitext(emFile)[0] + ".csv"

            try:
                surveyDatedt = self.GetData(rawFileName, "getSurveyDate")

                if isinstance(surveyDatedt, float):
                    surveyDatedt = self.FormatSurveyDate(emFile)
                elif isinstance(surveyDatedt, basestring):
                    if "," in surveyDatedt:
                        tmpDatedt = datetime.strptime(surveyDatedt,'%B %d, %Y')
                        surveyDatedt = datetime.strftime(tmpDatedt, "%Y-%m-%d")

                df = self.GetData(rawFileName)
                df = self.DfCleanUp(df, surveyDatedt)

                df.to_csv(csvFilename,
                          header=False,
                          sep=str(self.job["delimiter"]),
                          encoding='utf-8',
                          index=False)

            except XLRDError:
                self.logger.info(self.moduleName + " - No tab named '" + self.job["worksheetName"] + "' in " + emFile)
            except Exception:
                self.logger.error(self.moduleName + " - Error while trying to process " +  emFile)
                raise
            finally:
                FileUtilities.RemoveFileIfItExists(rawFileName)

    def ProcessTables(self, dbCommon, tables):
        '''
        process steps:
        pulls file from share and place in raw folder
        '''
        try:
            self.rawFolder = self.localTempDirectory + "/" + "Raw"
            self.BulkDownload()
            self.ProcessFiles()
            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            df = SparkUtilities.ReadCSVFile(spark, tables, self.job["delimiter"], False,
                                            self.fileUtilities.csvFolder, self.logger)
            SparkUtilities.SaveParquet(df, self.fileUtilities)
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)
            if "loadToRedshift" in tables and tables["loadToRedshift"] == "Y":
                self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessRequest")
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)