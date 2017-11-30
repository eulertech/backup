'''
Main script to process the PGCR IHSMarkit data
Author - Varun
License: IHS - not to be used outside the company
'''

import os
import re
import csv
import pandas as pd
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.FileUtilities import FileUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class PGCRIHSMarkitData(ApplicationBase):
    '''
    Code to process the PGCR IHSMarkit data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(PGCRIHSMarkitData, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))
        self.awsParams = ""
        self.excelUtilities = None


    def DownloadFiles(self):
        '''
        Download the entire bucket of IHSMarkitData
        '''
        fileList = S3Utilities.GetListOfFiles(self.awsParams.s3, self.job["bucketName"], self.job["s3SrcDirectory"][1:])
        for fileName in fileList:
            try:
                inputFileFullPath = self.localTempDirectory + "/" + fileName.split("/")[-1]
                S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"], fileName, inputFileFullPath)
            except Exception:
                self.logger.exception("Download Error for file " + fileName)
                raise

    def ProcessFiles(self):
        '''
        Process the downloaded files and upload into Redshift
        '''
        fileNames = self.fileUtilities.ScanFolder(self.localTempDirectory)
        for fileConfig in self.job["files"]:
            files = list(filter(re.compile(fileConfig["RegexPattern"]).match, fileNames))
            delimiter = self.job["delimiter"]
            if fileConfig.get("Delimiter") is not None:
                delimiter = fileConfig.get("Delimiter")
            skipRows = []
            if fileConfig.get("SkipRows") is not None:
                skipRows = [x-1 for x in fileConfig.get("SkipRows")] #convert the line numbers to a zero based index
            for fileName in files:
                inputFileFullPath = self.localTempDirectory + "/" + fileName
                df = pd.read_csv(inputFileFullPath, sep=delimiter, header=None, skiprows=skipRows)
                #===============================================================
                # If the filename starts (first 8 characters) with bilinear, add the extra columns
                #===============================================================
                if fileName[:8] == "bilinear":
                    headerInfo = self.GetHeaderInfo(inputFileFullPath)
                    df["dataProvider"] = headerInfo["dataProvider"]
                    df["extractedOn"] = headerInfo["extractedOn"]
                    df["model"] = headerInfo["model"]
                    df["verticalLevel"] = headerInfo["verticalLevel"]
                    df["horizontalInterpolation"] = headerInfo["horizontalInterpolation"]
                    df["iValue"] = headerInfo["iValue"]
                    df["jValue"] = headerInfo["jValue"]
                    df["latitude"] = headerInfo["latitude"]
                    df["longitude"] = headerInfo["longitude"]
                outputFileFullPath = self.localTempDirectory + "/cleaned/" + fileName
                df = df.applymap(lambda x: x.replace("\n", "") if type(x) is str else x) #replace all newline characters in the dataframe
                df = df.applymap(lambda x: x.replace("'", "`") if type(x) is str else x) #replace all single quote with back tick
                df.to_csv(outputFileFullPath, header=False, index=False, quoting=csv.QUOTE_NONNUMERIC)
                self.LoadCSVFile(outputFileFullPath, fileConfig)

    def GetHeaderInfo(self, filePath):
        '''
        Extract the header info from the bilinear file
        '''
        fp = open(filePath, "r")
        lines = fp.readlines()[:10]
        fp.close()
        headerInfo = {}
        headerInfo["dataProvider"] = lines[0].strip().split(":")[-1].strip()
        headerInfo["extractedOn"] = lines[1].strip().replace("Extracted on:","").strip()
        headerInfo["model"] = lines[3].strip().split(":")[-1].strip()
        headerInfo["verticalLevel"] = lines[4].strip().split(":")[-1].strip()
        headerInfo["horizontalInterpolation"] = lines[5].strip().split(":")[-1].strip()
        headerInfo["iValue"] = lines[6].strip().split(":")[-1].strip()
        headerInfo["jValue"] = lines[7].strip().split(":")[-1].strip()
        headerInfo["latitude"] = lines[8].strip().split(":")[-1].strip()
        headerInfo["longitude"] = lines[9].strip().split(":")[-1].strip()
        return headerInfo


    def LoadCSVFile(self, localFilePath, loadName):
        '''
        For each file we need to process, provide the data loader the s3 key
        and destination table name
        '''
        self.logger.info("Loading data into Redshift")
        try:
            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])

            RedshiftUtilities.LoadFileIntoRedshift(rsConnect,
                                                   self.awsParams.s3,
                                                   self.logger,
                                                   self.fileUtilities,
                                                   localFilePath,
                                                   self.job["destinationSchema"],
                                                   self.job["tableName"] + loadName["redshiftTableSuffix"],
                                                   self.job["fileFormat"],
                                                   self.job["dateFormat"],
                                                   self.job["delimiter"])
        except Exception:
            self.logger.exception("Exception in PGCRIHSMarkit.LoadCSVFile")
            self.logger.exception("Error while uploading to table:{}, filePath:{}".format(self.job["tableName"] + loadName["redshiftTableSuffix"], localFilePath))
            raise
        finally:
            if rsConnect is not None:
                rsConnect.close()
            self.logger.info(self.moduleName + " - Finished Processing S3 file: " + loadName["redshiftTableSuffix"])



    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory) #delete and recreate the folder
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/cleaned/") #delete and recreate the folder
            self.DownloadFiles()
            self.ProcessFiles()
        except Exception:
            logger.exception(moduleName + " - Exception!")
            raise


