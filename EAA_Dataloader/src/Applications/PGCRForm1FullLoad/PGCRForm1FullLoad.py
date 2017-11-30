'''
Created on Jan 25, 2017
'''
import re
import os
import csv
import pandas as pd
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.DBFUtilities import DBFUtilities
from AACloudTools.OSUtilities import OSUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class PGCRForm1FullLoad(ApplicationBase):
    '''
    Handles Form1 files in the PGCR project
    '''
    def __init__(self):
        '''
        Constructor for this class
        '''
        super(PGCRForm1FullLoad, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        self.awsParams = ""
        self.dbfUtilities = None

    def DownloadFile(self, s3Key, localGzipFilepath):
        '''
        Wrapper to download file
        '''
        self.logger.info(self.moduleName + " Downloading file: " + s3Key)
        try:
            S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"], s3Key, localGzipFilepath)
        except:
            self.logger.exception("Error while downloading file: {}".format(s3Key))
            raise

    def CreateFolders(self):
        '''
        Creates folder if it doesn't exist
        If it already exists, empties the folder contents
        '''
        FileUtilities.EmptyFolderContents(self.localTempDirectory + self.job["downloadPath"])
        FileUtilities.EmptyFolderContents(self.localTempDirectory + "/cleaned/")
        FileUtilities.EmptyFolderContents(self.localTempDirectory + "/packed/")
        for fp in self.job["foxpro_files"]:
            FileUtilities.EmptyFolderContents(self.localTempDirectory + "/packed/" + fp["Name"] + "/")

    def DownloadFiles(self, listOfFiles):
        '''
        Downloads the file from the S3 bucket to the data folder
        '''
        path = self.localTempDirectory + self.job["downloadPath"]
        for fp in listOfFiles:
            try:
                year = re.findall(r"\d{4}", fp)[-1] #gets the year from the file path
                fileName = fp.split("/")[-1]
                FileUtilities.CreateFolder(path + year + "/")
                self.DownloadFile("/" + fp, path + year + "/" + fileName)
                #unzip to the folder path as filename without the file extension
                self.fileUtilities.UnzipUsing7z(path + year + "/" + fileName, path + year + "/" + fileName[:-4])
                self.ProcessFiles(path + year + "/" + fileName[:-4], year)
            except:
                self.logger.exception("Error while downloading file: {}".format(fp))
                raise

    def ProcessFiles(self, folderPath, year):
        '''
        Processes the files that have been unzipped
        '''
        files = [f for f in self.fileUtilities.ScanFolder(folderPath) if f.lower() != "working"] #exclude the folder named "working"
        dbfFiles = [f for f in files if f.upper().endswith(".DBF")] #get the list of DBF files
        for fp in self.job["foxpro_files"]:
            if fp["Name"] + ".DBF" in dbfFiles:
                if fp.get("GetFPTFile") is None: #only if this key is present in the json file, keep the FPT file
                    if os.path.isfile(folderPath + "/" + fp["Name"] + ".FPT") is True: #check if the file exists
                        self.fileUtilities.DeleteFile(folderPath + "/" + fp["Name"] + ".FPT") #delete the FPT file
                self.dbfUtilities.ConvertToCSV2(folderPath + "/" + fp["Name"] + ".DBF")
                self.fileUtilities.DeleteFile(folderPath + "/" + fp["Name"] + ".DBF") #delete the DBF file
                if fp.get("InsertYear") is not None and fp.get("InsertYear") == "Y":
                    df = pd.read_csv(folderPath + "/" + fp["Name"] + ".CSV")
                    if "report_yea" not in df.columns:
                        df["report_yea"] = year #add the year column
                        df.to_csv(folderPath + "/" + fp["Name"] + ".CSV", index=False) #save without the index
                self.SkipHeader(folderPath + "/" + fp["Name"] + ".CSV")
                cleanFile = self.CleanFile(folderPath + "/" + fp["Name"] + ".CSV")
                newFile = folderPath + "/" + year + "_" + fp["Name"] + ".CSV"
                os.rename(cleanFile, newFile) #rename the file to prepend year to the name
                self.PackFiles(fp["Name"], newFile)
                self.fileUtilities.DeleteFile(newFile) #delete the CSV file

    def PackFiles(self, fpName, newFile):
        '''
        Converts the cleaned CSV files into gz.
        Deletes the original CSV file
        '''
        outputFile = self.localTempDirectory + "/packed/" + fpName + "/" + newFile.split("/")[-1] + ".gz"
        self.fileUtilities.GzipFile(newFile, outputFile)

    def LoadFilesIntoRedshift(self):
        '''
        Upload to S3 and load into Redshift
        '''
        for fp in self.job["foxpro_files"]:
            self.LoadIntoRedshift(fp["Name"])

    def UploadPackedToS3(self):
        '''
        Uploads all files packed to s3.
        '''
        for fp in self.job["foxpro_files"]:
            self.logger.info("Uploading GZIP files to s3 folder...")
            inputFolderPath = self.localTempDirectory + "/packed/" + fp["Name"] + "/"
            S3Utilities.CopyItemsAWSCli(inputFolderPath,
                                        "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"] + fp["Name"] + "/",
                                        "--recursive --quiet")

    def EmptyPackedFolder(self):
        '''
        Empties the packed folder
        '''
        FileUtilities.EmptyFolderContents(self.localTempDirectory + "/packed/")

    def LoadIntoRedshift(self, fpName):
        '''
        Performs the final step to insert multiple files located in s3 into the final table in Redshift.
        '''
        rsConnect = None
        s3DataFolder = "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"] + fpName + "/"
        try:
            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])
            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": self.job["destinationSchema"],
                                                 "tableName": self.job["tableName"] + fpName,
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
        Rename the tables post data load (to meaningful names)
        '''
        postLoadScriptTemplate = self.job.get("PostLoadScript")
        if postLoadScriptTemplate is not None:
            sqlTableCreationScript = super(PGCRForm1FullLoad, self).BuildTableCreationScript(postLoadScriptTemplate)
            RedshiftUtilities.PSqlExecute(sqlTableCreationScript, self.logger)
            self.logger.info(self.moduleName + " - SQL tables created.")

    def CleanFile(self, inputFile):
        '''
        Cleans the file
        '''
        try:
            outputFile = self.localTempDirectory + "/cleaned/" + inputFile.split("/")[-1]
            self.fileUtilities.ReplaceIterativelyInFile(inputFile, outputFile, self.job["charsToBeReplaced"])
        except:
            self.logger.exception("Error while cleaning the file: {}".format(inputFile))
            raise
        return outputFile

    def SkipHeader(self, filePath):
        '''
        Converts comma separated files to pipe separated files
        '''
        try:
            df = pd.read_csv(filePath)
            #drop the junk columns
            for column in self.job["csvColumnsExclusionList"]:
                if column in df.columns: #check if the column is present in the dataframe
                    df = df.drop(str(column), 1)
            df.to_csv(filePath,
                      sep=str(self.job["delimiter"]),
                      na_rep="",
                      header=False,
                      index=False,
                      quoting=csv.QUOTE_NONNUMERIC)
        except:
            self.logger.exception("Error removing header from file {}".format(filePath))
            raise

    def ConvertToCSV(self, fileInName):
        '''
        Convert file to CSV using the perl utility
        ConvertToCSV does not seem to work for some cases to need to resort to perl
        '''
        cmd = "perl " + DBFUtilities.GetDBF2CSVPath() + fileInName
        try:
            OSUtilities.RunCommandAndLogStdOutStdErr(cmd, self.logger)
        except Exception:
            raise

    def GetListOfFilesOnS3(self):
        '''
        Get the list of files on S3 under the given bucket & source directory and download the files
        '''
        try:
            return S3Utilities.GetListOfFiles(self.awsParams.s3, self.job["bucketName"], self.job["s3SrcDirectory"][1:])
        except Exception:
            self.logger.exception("Exception in PGCRFERCFilings.GetListOfFilesOnS3")
            self.logger.exception("Exception while fetching the list of files from S3 bucket: {}, path:{}".format(self.job["bucketName"],
                                                                                                                  self.job["s3SrcDirectory"][1:]))
            raise

    def Process(self):
        '''
        Process Form1 Files
        '''
        listOfFiles = self.GetListOfFilesOnS3()
        #=======================================================================
        # get the zip files in the reverse chronological order. Load the latest ones first
        #=======================================================================
        zipFiles = sorted([f for f in listOfFiles if f.lower().endswith(".zip")], reverse=True)
        self.DownloadFiles(zipFiles)

    def Start(self, logger, moduleName, filelocs):
        '''
        Starting point of this Project
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.dbfUtilities = DBFUtilities(logger)
            self.CreateFolders()
            self.Process()
            self.UploadPackedToS3()
            self.LoadFilesIntoRedshift()
            self.EmptyPackedFolder()
            self.PostLoadETL()
        except:
            self.logger.exception(moduleName + " - Exception!")
            raise
