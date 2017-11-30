'''
Main script to process the PGCR ERCOT data
Author - Varun
License: IHS - not to be used outside the company
'''

import os

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase


class PGCRERCOTAthena(ApplicationBase):
    '''
    Code to process the PGCR ERCOT data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(PGCRERCOTAthena, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))
        self.awsParams = ""
        self.packedFolder = None
        self.currProcId = None
        
    def ProcessTables(self, dbCommon, tables):
        '''
        Process each file
        '''
        print("Inside ProcessTables")
        
    def DownDataFiles(self, dbCommon):
        '''
        Download all the files and unzip them
        '''
        s3SrcDirectory = dbCommon["s3SrcDirectory"]
        bucketName = s3SrcDirectory.replace("s3://","").split("/")[0].strip()
        directory = s3SrcDirectory.replace("s3://"+bucketName, "")
        fileList = S3Utilities.GetListOfFiles(self.awsParams.s3, bucketName, directory[1:])
        for fileName in fileList:
            try:
                inputFileFullPath = self.localTempDirectory + "/" + fileName.split("/")[-1]
                S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"], fileName, inputFileFullPath)
                unzipFolder = self.fileUtilities.gzipFolder + inputFileFullPath.split('.')[0] + "/"
                self.fileUtilities.UnzipFile(inputFileFullPath, unzipFolder)
            except Exception:
                self.logger.exception("Download Error for file " + fileName)
                raise
            
    def ProcessCatalogs(self, dbCommon, catalog):
        '''
        Pull the data first and then process each table
        '''
        self.DownDataFiles(dbCommon)
        for tables in catalog["tables"]:
            self.ProcessTables(dbCommon, tables)
    
    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
