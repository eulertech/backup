'''
Main script to process the PGCR EIA 923 data
Author - Chinmay
License: IHS - not to be used outside the company
'''

import os

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.FileUtilities import FileUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class PGCREIAOpD(ApplicationBase):
    '''
    Code to process the PGCR EIA Form - 923 data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(PGCREIAOpD, self).__init__()
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
            for srcFileParameter in self.job["srcFileParameters"]:
                self.DownloadAllFiles(srcFileParameter)
                self.UnzipExcel(srcFileParameter)
            self.SkipPackAndLoad(self.job["srcFileParameters"])
            if "postETLQueries" in self.job:
                ApplicationBase.CreateTables(self, self.job["postETLQueries"])

        except Exception:
            logger.exception(moduleName + " - Exception!")
            raise

    def DownloadAllFiles(self, srcFileParameter):
        '''
        Worker function to download the file
        '''
        s3Key = "/" + self.job["s3SrcDirectory"] + "/" +\
                srcFileParameter["s3Filename"] + self.job["srcfileFormat"]
        self.logger.info(" Downloading file: " + s3Key)
        localFilePath = self.localTempDirectory + "/" +\
                        srcFileParameter["s3Filename"] + self.job["srcfileFormat"]
        S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"], s3Key, localFilePath)

    def UnzipExcel(self, srcFileParameter):
        '''
        Convert zip files to Excel worksheets using FileUtilities
        '''
        localFilePath = self.localTempDirectory + "/" +\
                        srcFileParameter["s3Filename"] + self.job["srcfileFormat"]
        outFilePath = self.localTempDirectory + "/" + srcFileParameter["redshiftTableSuffix"]
        self.fileUtilities.UnzipFile(localFilePath, outFilePath)

    def SkipPackAndLoad(self, fileLocations):
        '''
        Pick all CSVs in a folder and append them after skipping some rows
        '''
        folderList = []
        for filenames in fileLocations:
            inputFileFullPath = self.localTempDirectory + "/" + filenames["redshiftTableSuffix"] + "/" + filenames["s3Filename"] + ".csv"
            self.fileUtilities.RemoveLines(inputFileFullPath, filenames["Skip"])
            folderList.append(filenames["redshiftTableSuffix"])
            self.toPackFiles.append(inputFileFullPath)
        self.PackFiles()
        folderList = list(set(folderList))
        for folder in folderList:
            self.UploadPackedToS3(folder)
            self.LoadEIAOpDTables(folder)
            self.fileUtilities.RemoveFolder(folder)

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
        S3Utilities.CopyItemsAWSCli(self.localTempDirectory + "/" + folder,
                                    "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"] + folder,
                                    "--recursive --quiet")

    def LoadEIAOpDTables(self, folder):
        '''
        Performs the final step to insert multiple files located in s3 into the final table in Redshift.
        '''
        try:
            s3DataFolder = "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"] + folder

            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": self.job["destinationSchema"],
                                                 "tableName": self.job["tableName"] + folder,
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
