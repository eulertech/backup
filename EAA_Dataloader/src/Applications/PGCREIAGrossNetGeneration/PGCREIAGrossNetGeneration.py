'''
Main script to process the PGCR EIA 923 data
Author - Chinmay
License: IHS - not to be used outside the company
'''

import os
import pandas as pd

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.ExcelUtilities import ExcelUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class PGCREIAGrossNetGeneration(ApplicationBase):
    '''
    Code to process the PGCR EIA Form - 923 data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(PGCREIAGrossNetGeneration, self).__init__()
        self.packedFolder = None
        self.folderList = []
        self.toPackFiles = []
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))
        self.awsParams = ""

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            for sheet in self.job["ExcelSheets"]:
                self.DownloadAllFiles(sheet)
                self.ConvertExcel2Csv(sheet)
            self.SkipPackAndLoad(self.job["ExcelSheets"])
            if "postETLQueries" in self.job:
                ApplicationBase.CreateTables(self, self.job["postETLQueries"])
        except Exception:
            logger.exception(moduleName + " - Exception!")
            raise

    def DownloadAllFiles(self, sheet):
        '''
        Worker function to download the file
        '''
        s3Key = "/" + self.job["s3SrcDirectory"] + "/" + sheet["Name"]
        self.logger.info(" Downloading file: " + s3Key)
        localFilePath = self.localTempDirectory + "/" + sheet["Name"]
        S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"], s3Key, localFilePath)

    def UnzipExcel(self, srcFileParameter):
        '''
        Convert zip files to Excel worksheets using FileUtilities
        '''
        localFilePath = self.localTempDirectory + "/" + srcFileParameter["s3Filename"] + self.job["srcfileFormat"]
        outFilePath = self.localTempDirectory + "/" + srcFileParameter["SubFolder"]
        self.fileUtilities.UnzipFile(localFilePath, outFilePath)

    def ConvertExcel2Csv(self, sheet):
        '''
        Convert Excel worksheets to csvs using Excel Utilities
        '''
        localFilePath = self.localTempDirectory + "/" + sheet["Name"]
        csvName = self.localTempDirectory + "/" + sheet["Name"].split(".")[0] + ".csv"
        eu = ExcelUtilities(self.logger)
        eu.Excel2CSV(localFilePath, sheet["Sheet"], csvName, localFilePath, ',')
        df = pd.read_csv(csvName)
        df.to_csv(csvName, index=False)

    def SkipPackAndLoad(self, fileLocations):
        '''
        Pick all CSVs in a folder and append them after skipping some rows
        '''
        for filenames in fileLocations:
            inputFileFullPath = self.localTempDirectory + "/" + filenames["Name"].split(".")[0] + ".csv" 

        self.toPackFiles.append(inputFileFullPath)
        self.PackFiles()
        self.UploadPackedToS3(self.localTempDirectory + "/")
        self.LoadEIANetGen(self.job["tableSuffix"])
        self.fileUtilities.RemoveFolder(self.localTempDirectory + "/")


    def PackFiles(self):
        '''
        Compress the files for a given folder, right now is only the emissions file being packed.
        '''
        self.logger.info("Packing files for the folders")

        for csvFile in self.toPackFiles:
            eia923GzFile = csvFile + ".gz"

            self.fileUtilities.GzipFile(csvFile, eia923GzFile)
            self.fileUtilities.DeleteFile(csvFile)

    def UploadPackedToS3(self, folder):
        '''
        Uploads all files packed to s3.
        '''
        self.logger.info("Uploading GZIP files to s3 from folder..." + folder)
        S3Utilities.CopyItemsAWSCli(self.localTempDirectory,
                                    "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"],
                                    "--recursive --quiet")

    def LoadEIANetGen(self, tableSuffix):
        '''
        Performs the final step to insert multiple files located in s3 into the final table in Redshift.
        '''
        try:
            file_name = self.job["ExcelSheets"][0]["Name"].split('.')[0]+'.csv.gz'
            s3DataFolder = "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"]+file_name

            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": self.job["destinationSchema"],
                                                 "tableName": self.job["tableName"] + tableSuffix,
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
