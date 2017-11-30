'''
Main script to process the PGCR ERCOT data
Author - Varun
License: IHS - not to be used outside the company
'''

import json
import os
import re

from AACloudTools.DatetimeUtilities import DatetimeUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase
import pandas as pd


class PGCRERCOT(ApplicationBase):
    '''
    Code to process the PGCR ERCOT data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(PGCRERCOT, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))
        self.awsParams = ""
        self.packedFolder = None
        self.currProcId = None

    def DownloadFile(self, s3Key, outputLocation):
        '''
        Worker function to download the file
        '''
        self.logger.info(" Downloading file: " + s3Key)
        try:
            s3Key = "/" + s3Key
            unzippedFile = s3Key.split("/")[-1]
            localGzipFilepath = outputLocation + unzippedFile
            S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"], s3Key, localGzipFilepath)
        except Exception:
            self.logger.exception("Error while downloading file: {}".format(s3Key))
            raise

    def CreateFolders(self):
        '''
        Delete and recreate the folders
        '''
        try:
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/raw/")  # delete and recreate the folder
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/output/")  # delete and recreate the folder
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/csvs/")  # delete and recreate the folder
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/packed/")  # delete and recreate the folder
        except:
            self.logger.exception("Error while creating folders")
            raise

    def EmptyFolders(self):
        '''
        Delete and recreate the folders
        '''
        try:
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/raw/")  # delete and recreate the folder
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/output/")  # delete and recreate the folder
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/csvs/")  # delete and recreate the folder
        except:
            self.logger.exception("Error while creating folders")
            raise

    def GetLastModifiedDatetime(self, filelocs):
        '''
        Handles the incremental load of ERCOT data
        Pulls the json {"lastModifiedDatetime": "2017-06-07 19:51:06"} from eaa_dev.etl_process_logs
        Returns datetime in UTC
        '''
        self.currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
        lastRunRecJson = self.etlUtilities.GetLastGoodRun(filelocs["tblEtl"]["table"], self.moduleName)
        paramsList = {}
        lastModifiedDatetime = None
        if (lastRunRecJson is not None) and (lastRunRecJson.get("params") is not None):
            paramsList = json.loads(lastRunRecJson["params"])
        if paramsList.get("lastModifiedDatetime") is not None:
            lastModifiedDatetime = paramsList["lastModifiedDatetime"]
        return DatetimeUtilities.ConvertToUTC(lastModifiedDatetime)

    def SetLastModifiedDatetime(self, filelocs, lastModifiedDatetime):
        '''
        sets the params section in ETL Logging table
        '''
        if self.etlUtilities.SetInstanceParameters(filelocs["tblEtl"]["table"],\
                                                  self.currProcId,\
                                                  json.dumps({"lastModifiedDatetime": lastModifiedDatetime})) is not True:
            self.logger.info(self.moduleName + " - we could not set the instance.")

    def GetNewFiles(self, lastModifiedDatetime):
        '''
        Get the list of new files on S3
        '''
        maxModifiedDatetime = None
        files = []
        if lastModifiedDatetime is not None:
            maxModifiedDatetime = lastModifiedDatetime
            newFiles = S3Utilities.GetFilesSinceGivenDatetime(self.job["bucketName"], self.job["s3SrcDirectory"], lastModifiedDatetime)
            for newFile in newFiles:
                dtStr = newFile["datetime"]
                dt = DatetimeUtilities.ConvertToDT(dtStr)
                if dt > maxModifiedDatetime:
                    maxModifiedDatetime = dt
                    files.append(newFile["fileName"])
        else:
            newFiles = S3Utilities.GetFilesNModifiedDatetimeFromS3(self.job["bucketName"], self.job["s3SrcDirectory"])
            for newFile in newFiles:
                dtStr = newFile["datetime"]
                dt = DatetimeUtilities.ConvertToDT(dtStr)
                if maxModifiedDatetime is None:
                    maxModifiedDatetime = dt
                if dt > maxModifiedDatetime:
                    maxModifiedDatetime = dt
                files.append(newFile["fileName"])
        return (files, maxModifiedDatetime)

#     def GetListOfFiles(self):
#         '''
#         Get the list of files to be downloaded
#         '''
#         try:
#             filesOnS3 = S3Utilities.GetListOfFiles(self.awsParams.s3, self.job["bucketName"], self.job["s3SrcDirectory"][1:])
#             filesOnS3 = [fl.replace(self.job["s3SrcDirectory"][1:], "") for fl in filesOnS3]  # remove the S3 path from the filename
#             return filesOnS3
#         except:
#             self.logger.exception("Error while getting list of files on S3")
#             raise


    def SkipHeader(self, inputFolderPath):
        '''
        Skips the Header
        '''
        csvFiles = self.fileUtilities.ScanFolder(inputFolderPath)
        for csvFile in csvFiles:
            FileUtilities.RemoveLines(inputFolderPath + csvFile, [1])

    def AddColumnSkipHeader(self, inputFolderPath):
        '''
        Adds the column DSTFlag if it doesn't exist
        '''
        csvFiles = self.fileUtilities.ScanFolder(inputFolderPath)
        for csvFile in csvFiles:
            df = pd.read_csv(inputFolderPath + csvFile)
            if not 'DSTFlag' in df.columns:
                df["DSTFlag"] = 'N'
            df.to_csv(inputFolderPath + csvFile, header=False, index=False)

    def PackFiles(self, inputFolderPath, outputFolderPath):
        '''
        Converts the cleaned CSV files into gz.
        Deletes the original CSV file
        '''
        csvFiles = self.fileUtilities.ScanFolder(inputFolderPath)
        for csvFile in csvFiles:
            self.fileUtilities.GzipFile(inputFolderPath + csvFile, outputFolderPath + csvFile + ".gz")
            self.fileUtilities.DeleteFile(inputFolderPath + csvFile)

    def UploadPackedToS3(self):
        '''
        Uploads all files packed to s3.
        '''
        self.logger.info("Uploading GZIP files to s3 folder...")
        S3Utilities.CopyItemsAWSCli(self.packedFolder,
                                    "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"],
                                    "--recursive --quiet")

    def ProcessFiles(self, lastModifiedDatetime):
        '''
        Start processing the ERCOT files
        '''
        maxModifiedDatetime = None
        try:
            filesOnS3, maxModifiedDatetime = self.GetNewFiles(lastModifiedDatetime)
            filesOnS3 = [fl.replace(self.job["s3SrcDirectory"][1:],"") for fl in filesOnS3]
            for fileConfig in self.job["files"]:
                zipFiles = list(filter(re.compile(fileConfig["FileRegex"]).match, filesOnS3))
                for zipFileName in zipFiles:  # eg DAM_Hr_LMP_2011.zip
                    self.DownloadFile(self.job["s3SrcDirectory"][1:] + zipFileName, self.localTempDirectory + "/raw/")
                    self.fileUtilities.UnzipUsing7z(self.localTempDirectory + "/raw/" + zipFileName, self.localTempDirectory + "/output/")
                    level2Files = self.fileUtilities.ScanFolder(self.localTempDirectory + "/output/")
                    level2Files = [l2File for l2File in level2Files if l2File.lower().endswith("_csv.zip")]  # exclude all non-csv files
                    for l2File in level2Files:  # eg cdr.00012328.0000000000000000.20110101.131852.DAMHRLMPNP4183_csv.zip
                        l2zip = self.localTempDirectory + "/output/" + l2File
                        self.fileUtilities.UnzipUsing7z(l2zip, self.localTempDirectory + "/csvs/")
                        FileUtilities.RemoveFileIfItExists(l2zip)  # delete the file after unzipping
                    FileUtilities.RemoveFileIfItExists(self.localTempDirectory + "/raw/" + zipFileName)  # delete the parent file
                    self.AddColumnSkipHeader(self.localTempDirectory + "/csvs/")  # Add column DSTFlag if it doesn't exist and skip header
                    self.PackFiles(self.localTempDirectory + "/csvs/", self.localTempDirectory + "/packed/")
        except:
            self.logger.exception("Error while processing ERCOT files")
            raise
        return maxModifiedDatetime

    def EmptyPackedFolder(self):
        '''
        Empties the packed folder
        '''
        FileUtilities.RemoveFolder(self.packedFolder)

    def LoadErcotTables(self):
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
                                                 "tableName": self.job["tableName"] + "DAM",
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
        finally:
            if rsConnect is not None:
                rsConnect.close()

    def PostLoadETL(self):
        '''
        Create the L2 tables post data load
        '''
        postLoadScriptTemplate = self.job.get("PostLoadScript")
        if postLoadScriptTemplate is not None:
            sqlTableCreationScript = super(PGCRERCOT, self).BuildTableCreationScript(postLoadScriptTemplate)
            RedshiftUtilities.PSqlExecute(sqlTableCreationScript, self.logger)
            self.logger.info(self.moduleName + " - SQL tables created.")

    def GetLastModifiedDateTime(self, filelocs):
        '''
        Handles the incremental load of ERCOT data
        Pulls the json {"lastModifiedDateTime": "2017-06-07 19:51:06"} from eaa_dev.etl_process_logs
        '''
        lastRunRecJson = self.etlUtilities.GetLastGoodRun(filelocs["tblEtl"]["table"], self.moduleName)
        paramsList = []
        try:
            if lastRunRecJson is not None:
                paramsList = json.loads(lastRunRecJson["params"])
        except Exception as ex:
            self.logger.exception(ex.message)
            raise
        print(paramsList)

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.packedFolder = self.localTempDirectory + "/packed/"
            self.CreateFolders()
            lastModifiedDatetime = self.GetLastModifiedDatetime(filelocs)
            maxModifiedDatetime = self.ProcessFiles(lastModifiedDatetime)
            self.UploadPackedToS3()
            self.LoadErcotTables()
            self.SetLastModifiedDatetime(filelocs, DatetimeUtilities.ConvertToSTR(maxModifiedDatetime))
            self.EmptyPackedFolder()
            self.PostLoadETL()
        except Exception:
            logger.exception(moduleName + " - Exception!")
            raise
