'''
Main script to process the PGCR Air Markets data using the Spark-Parquet Method
Author - Aswin Narayanan, using the existing PGCRAirMarkets application as a starting point.
License: IHS - not to be used outside the company
'''

import os
import ntpath
import re
from Applications.Common.ApplicationBase import ApplicationBase
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.SparkUtilities import SparkUtilities

class PGCRAirMarketsAthenaSpark(ApplicationBase):
    '''
    Code to process the PGCR Air Markets data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(PGCRAirMarketsAthenaSpark, self).__init__()

        self.awsParams = ""
        self.tempFolder = None
        self.packedFolder = None
        self.rawDataFolder = None
        self.toPackFiles = []

        self.fileUtilities = FileUtilities(self.logger)
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def SynchronizeSourceFolder(self):
        '''
        Synchronize the source folder from the AirMarkets bucket in s3
        '''
        self.logger.info("Synchronizing ZIP files from s3 folder...")

        S3Utilities.SyncFolderAWSCli("s3://" + self.job["bucketName"] + self.job["s3SrcDirectory"],
                                     self.rawDataFolder,
                                     True)

    def CleanUpAndPack(self):
        '''
        Main control to iterate thru the folders cleaning the files and packing them to be uploaded to s3.
        '''
        rawFolders = self.fileUtilities.ScanFolder(self.rawDataFolder)

        for rawFolderName in rawFolders:
            self.toPackFiles = []

            self.DecompressFromRaw(rawFolderName)
            self.CleanUpRawCSV(rawFolderName)
            self.PackFiles(rawFolderName)

    def PackFiles(self, rawFolderName):
        '''
        Compress the files for a given folder, right now is only the emissions file being packed.
        '''
        self.logger.info("Packing files for folder " + rawFolderName + "...")

        for csvFile in self.toPackFiles:
            airMarketGzFile = self.packedFolder + "/" + ntpath.basename(csvFile) + ".gz"

            self.fileUtilities.GzipFile(csvFile, airMarketGzFile)
            self.fileUtilities.DeleteFile(csvFile)

    def CleanUpRawCSV(self, rawFolderName):
        '''
        Performs the clean-up for the emissions files replacing bd characters.
        '''
        allFiles = self.fileUtilities.ScanFolder(self.tempFolder, None, "csv")
        fileList = [fileName for fileName in allFiles if self.job["srcFileNamePrefix"] in fileName]
        fileListToDel = [fileName for fileName in allFiles if self.job["srcFileNamePrefix"] not in fileName]

        self.logger.info("Cleaning up files for folder " + rawFolderName + "...")

        for airMarketFile in fileList:
            fullFileName = self.tempFolder + "/" + airMarketFile
            toPackFileName = self.tempFolder + "/" + self.job["srcFileNamePrefix"] + "_" + rawFolderName + ".csv"

            self.fileUtilities.ReplaceIterativelyInFile(fullFileName,
                                                        toPackFileName,
                                                        [{r"[^\x00-\x76]+":""}, {"'":"`"}])

            self.fileUtilities.RemoveLines(toPackFileName, self.job["removeLines"])
            self.toPackFiles.append(toPackFileName)
            self.fileUtilities.DeleteFile(fullFileName)

        for airMarketFile in fileListToDel:
            self.fileUtilities.DeleteFile(self.tempFolder + "/" + airMarketFile)

    def DecompressFromRaw(self, rawFolderName):
        '''
        Extracts the files from the EPADownload.zip file...
        '''
        try:
            filePath = self.rawDataFolder + "/" + rawFolderName + "/" + self.job["inputZipFileName"]

            self.logger.info("Unpacking file: " + filePath)
            self.fileUtilities.UnzipUsing7z(filePath, self.tempFolder)
        except StandardError as err:
            self.logger.info("Unable to decompress file: " + filePath + " Error: " + err.message)

    def UploadPackedToS3(self):
        '''
        Uploads all files packed to s3.
        '''
        self.logger.info("Uploading GZIP files to s3 folder...")

        S3Utilities.CopyItemsAWSCli(self.packedFolder,
                                    "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"],
                                    "--recursive --quiet")

    def LoadAirMarketsTables(self):
        '''
        Performs the final step to insert multiple files located in s3 into the final table in Redshift.
        '''
        try:
            s3DataFolder = "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"]

            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": self.job["destinationSchema"],
                                                 "tableName": self.job["tableName"] + self.job["srcFileNamePrefix"],
                                                 "s3Filename": s3DataFolder,
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": self.job["delimiter"]
                                             },
                                             self.logger, "N")

            self.logger.info("Cleaning s3 data folder...")

            S3Utilities.DeleteFileFromS3TempUsingAWSCLi(s3DataFolder, "--recursive --quiet")
        except Exception:
            self.logger.error("Error while trying to save into Redshift from s3 folder.")
            raise

    def CleanWorkingFolders(self):
        '''
        Ensures the folders are cleaned and ready before the process execution.
        '''
        self.logger.info("Cleaning local working folders...")

        FileUtilities.RemoveFolder(self.tempFolder)
        FileUtilities.RemoveFolder(self.packedFolder)

        FileUtilities.CreateFolder(self.tempFolder)
        FileUtilities.CreateFolder(self.packedFolder)

    def ProcessTable(self,table):
        '''
        Process data for the table
        :param table:
        :return:
        '''

        s3Key = self.job["s3Filename"]
        self.logger.info(self.moduleName + " - Processing file: " + s3Key)

        self.fileUtilities.moduleName = self.moduleName
        self.fileUtilities.localBaseDirectory = self.localTempDirectory + "/" + table["table"]
        self.fileUtilities.CreateFolders(self.job["folders"])

        fileName = ntpath.basename(s3Key)

        local7zipFilePath = self.fileUtilities.gzipFolder+ "/" +fileName

        S3Utilities.DownloadFileFromS3(self.awsParams.s3,self.job["bucketName"],
                                       s3Key,local7zipFilePath)

        localCsvFilepath = self.fileUtilities.csvFolder + "/" + fileName
        localCsvFilepath = re.sub(r'\.zip$', '', localCsvFilepath)


        self.fileUtilities.UnzipUsing7z(local7zipFilePath,localCsvFilepath)
        fileToBeloaded = localCsvFilepath+'/'+'emission_05-11-2017.csv'

        spark = SparkUtilities.GetCreateSparkSession(self.logger)
        schema = SparkUtilities.BuildSparkSchema(table)

        df = (spark.read
              .format("com.databricks.spark.csv")
              .options(header='true', delimiter=self.job["delimiter"],ignoreTrailingWhiteSpace='true')
              .schema(schema)
              .load(fileToBeloaded)
              )

        #df.show()
        self.logger.info(
            self.moduleName + " -- " + "Done reading " + str(df.count()) + " rows.  Now saving as parquet file...")
        SparkUtilities.SaveParquet(df, self.fileUtilities)
        self.UploadFilesCreateAthenaTablesAndSqlScripts(table,self.fileUtilities.parquet)
        self.logger.info(self.moduleName + " -- " + "UploadFilesCreateAthenaTablesAndSqlScripts " + " finished ")

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)

            for table in self.job["tables"]:
                self.ProcessTable(table)

            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)

            self.logger.debug(self.moduleName + " -- " + " finished ")

        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"], \
                                                  currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)


'''
self.tempFolder = self.localTempDirectory + "/Temp"
            self.packedFolder = self.localTempDirectory + "/Packed"
            self.rawDataFolder = self.localTempDirectory + "/RawData"

            #self.CleanWorkingFolders()
            self.SynchronizeSourceFolder()
            self.CleanUpAndPack()
            self.UploadPackedToS3()
            self.LoadAirMarketsTables()
        except:
            logger.exception(moduleName + " - Exception!")
            raise'''


