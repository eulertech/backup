'''
Created on Nov 14, 2017

@author: Hector Hernandez
@summary: This application will pull the LiquidBalance data from Excel Binary File and load it into Athena
'''
import os
import shutil

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.OSUtilities import OSUtilities
from AACloudTools.ExcelUtilities import ExcelUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class LiquidsBalanceAthenaSpark(ApplicationBase): #pylint:disable=abstract-method
    '''
    This application will pull LiquidBalance data from Excel Spreadsheet and load it into RedShift
    '''
    def __init__(self):
        '''
        constructor
        '''
        super(LiquidsBalanceAthenaSpark, self).__init__()

        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        self.fileName = None
        self.xlFileName = None

    def GetLastLiquidsBalanceFileInfo(self, dbCommon):
        '''
        Looks for the last file that needs to be processed.
        '''
        try:
            if self.fileName is None:
                wowFolders = self.fileUtilities.GetListOfFiles(dbCommon["sharedSrcFolder"], "*Q")
                lenwowFolders = len(wowFolders)

                if lenwowFolders > 0:
                    wowFolder = ""
                    wowFolders.sort(reverse=True)

                    for wowItem in wowFolders:
                        self.fileName = dbCommon["inputFilePrefix"] + wowItem + dbCommon["inputFileExt"]

                        if os.path.exists(dbCommon["sharedSrcFolder"] + wowItem + "\\" + self.fileName) is True:
                            wowFolder = wowItem
                            break

                    shutil.copyfile(dbCommon["sharedSrcFolder"] + wowFolder + "\\" + self.fileName,
                                    self.localTempDirectory + "/raw/"  + self.fileName)
                else:
                    raise Exception("No files were found in the source folder.")
        except Exception as err:
            self.logger.error(self.moduleName + " Error while trying to look for the balances file. Message: " + err.message)
            raise

    @staticmethod
    def GetSparkTableDDL(subSetNameFileName, tableConfig, dbCommon):
        '''
        Defines the table schema for the sub data set.
        '''
        columnNames = []
        columnNames = columnNames # Silly

        with open(subSetNameFileName, "r") as csvFile:
            columnNames = csvFile.readlines()[0].replace('"', "").split(",")

        for colName in columnNames:
            found = False

            for field in tableConfig["fields"]:
                if field["name"].lower() == colName.lower():
                    found = True

            if found is False:
                newField = {"name": "y" + colName.replace("\n", ""), "type": dbCommon["yearValueDataType"]}
                tableConfig["fields"].append(newField)

        return tableConfig

    def GetSheetDataToCsv(self, dbCommon, tableConfig):
        '''
        Extracts the data from the liquids balance file, based on the sheet config.
        '''
        try:
            sheetConfig = tableConfig["sheetSrc"]
            subSetName = sheetConfig["name"] + "_" + sheetConfig["subSetName"]

            self.logger.info(self.moduleName + " - Extracting data from sheet: " + sheetConfig["name"] + "-" + subSetName)

            subSetNameFileName = self.fileUtilities.csvFolder + ("/%s.csv" % subSetName)

            if self.xlFileName is None:
                self.xlFileName = self.fileName.replace(".xlsb", ".xlsx")

                cmd = dbCommon["cmd"]
                cmd = cmd.replace("#WORKINGFOLDER#", self.fileUtilities.PathToBackwardSlash(self.localTempDirectory) + "\\raw")
                cmd = cmd.replace("#SOURCEFILE#", self.fileName)
                cmd = cmd.replace("#SHEETS#", dbCommon["sheetsToExtract"])
                cmd = cmd.replace("#DESTINATIONFILE#", self.xlFileName)

                OSUtilities.RunCommandAndLogStdOutStdErr(cmd, self.logger)

            xl = ExcelUtilities(self.logger)
            dataSheet = xl.GetSheet(self.localTempDirectory + "/raw/" + self.xlFileName, xl, sheetConfig["name"])
            startLoc = 0
            endLoc = dataSheet.ncols

            if sheetConfig["subSetNameBwd"] != "":
                startLoc = xl.ExcelFindString(dataSheet, sheetConfig["subSetNameBwd"], startrow=0, startcol=0, endrow=2).col+1

            if sheetConfig["subSetNameFwd"] != "":
                endLoc = xl.ExcelFindString(dataSheet, sheetConfig["subSetNameFwd"], startrow=0, startcol=0, endrow=2).col

            cols = sheetConfig["cols"]

            for i in range(startLoc, endLoc):
                if i not in cols:
                    cols.append(i)

            xl.Excel2CSV(self.localTempDirectory + "/raw/" + self.xlFileName,
                         sheetConfig["name"],
                         subSetNameFileName,
                         self.fileUtilities.csvFolder,
                         skiprows=sheetConfig["skipTopRows"],
                         omitBottomRows=sheetConfig["skipBottomRows"],
                         cols=cols)

            response = {
                "csvFileName": subSetNameFileName,
                "tableDDL": self.GetSparkTableDDL(subSetNameFileName, tableConfig, dbCommon)
            }

            return response
        except Exception as err:
            self.logger.error(self.moduleName + " Error while trying to extract XLSB data. Message:" + err.message)
            raise

    @staticmethod
    def MeltDataFrame(applyCategoryCol, tableDDL, df):
        '''
        Will return a transposed dataframe
        '''
        dfMelted = None
        stackSentence = "stack(" + str(len(df.columns)-2) + ","

        for col in df.columns:
            if col not in ["region", "country"]:
                stackSentence = stackSentence + "'" + col.replace("y", "") + "'," + col + ","

        stackSentence = stackSentence[:len(stackSentence)-1] + ")"

        if applyCategoryCol == "Y":
            dfMelted = df.selectExpr("'" + tableDDL["sheetSrc"]["subSetName"] + "' AS category", "region", "country",
                                     stackSentence).where("region is not null")
        else:
            dfMelted = df.selectExpr("region", "country", stackSentence).where("region is not null")

        dfMelted = SparkUtilities.RenameColumns(dfMelted, ["col0", "col1"], ["year", "value"])

        return dfMelted

    def ProcessCatalogs(self, dbCommon, catalog):
        '''
        Process the liquids balance catalog.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "Processing data for catalog: " + catalog["name"])

            self.GetLastLiquidsBalanceFileInfo(dbCommon)
            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            dfConsolidated = None

            for tableDDL in catalog["tables"]:
                if tableDDL["type"] == "raw":
                    csvInfo = self.GetSheetDataToCsv(dbCommon, tableDDL)
                    df = SparkUtilities.ReadCSVFile(spark, csvInfo["tableDDL"], self.job["delimiter"], True,
                                                    csvInfo["csvFileName"], self.logger)

                    if dfConsolidated is None:
                        dfConsolidated = self.MeltDataFrame(catalog["applyCategoryCol"], tableDDL, df)
                    else:
                        dfConsolidated.unionAll(self.MeltDataFrame(catalog["applyCategoryCol"], tableDDL, df))

            for tableDDL in catalog["tables"]:
                if tableDDL["type"] == "destination":
                    SparkUtilities.SaveParquet(dfConsolidated, self.fileUtilities)
                    self.UploadFilesCreateAthenaTablesAndSqlScripts(tableDDL, self.fileUtilities.parquet)
                    self.LoadDataFromAthenaIntoRedShiftLocalScripts(tableDDL)
                    break

        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to load table. Error: " + err.message)
            raise

    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)