'''
Created on Jan 25, 2017

@author: Varun Muriyanat
'''
import os
import csv
import numpy as np
import pandas as pd
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.PandasUtilities import PandasUtilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase

class PGCRFERCQuarterlyFilings(ApplicationBase):
    '''
    Handles the FERC Quarterly Filings
    '''
    def __init__(self):
        '''
        Class Constructor
        '''
        super(PGCRFERCQuarterlyFilings, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        self.pandasUtilities = None
        self.packedFolder = None

    def RecursivelyUnzipFiles(self, srcDirectory):
        '''
        Recursively unzips the files
        '''
        srcDirectory = srcDirectory.strip() #trim trailing spaces if any
        if srcDirectory[-1] != "/": #if the path doesn't end with forward slash, append one
            srcDirectory = srcDirectory + "/"
        print(srcDirectory)
        #=======================================================================
        # get the list of files in the given path and unzip them
        #=======================================================================
        files = self.fileUtilities.ScanFolder(srcDirectory)
        for unzippedFile in files:
            try:
                if unzippedFile.lower().endswith(".zip"): #we are looking for only zip files
                    inputFilename = srcDirectory + "/" + unzippedFile #build the full path to the zip file
                    outputFolder = unzippedFile.split(".")[0] #get the filename without the zip part
                    outputDirectory = srcDirectory + "/" + unzippedFile.split(".")[0] #build the output directory to which the file is to be unzipped
                    FileUtilities.CreateFolder(outputDirectory) #Create the folder to be unzipped into
                    self.fileUtilities.UnzipUsing7z(inputFilename, outputDirectory) #unzip using the 7z utility
                    FileUtilities.RemoveFileIfItExists(inputFilename) #deletes the zip file after unzipping it
                    self.RecursivelyUnzipFiles(srcDirectory + "/" + outputFolder) #recursive call to this method
            except:
                self.logger.exception("Exception in PGCRFERCFilings.RecursivelyUnzipFiles while unzipping file: {}".format(unzippedFile))
                raise


    def SaveTransactions(self, fileType, fileName, outputFileName):
        '''
        It was suggested during the code review to skip loading into pandas step and use native file handling to save transactions files
        skip header
        remove special characters
        remove self.job["charsToBeReplaced"]
        save it to self.localTempDirectory + "/" + fileType + "/" + outputFileName
        '''
        with open(fileName, "rU") as ifile:
            with open(self.localTempDirectory + "/" + fileType + "/" + outputFileName, "wb") as ofile:
                filtered = (line.replace("\n", '') for line in ifile)
                reader = csv.reader(filtered, delimiter=',', quotechar='"')
                writer = csv.writer(ofile, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                lineCounter = 1
                for row in reader:
                    if lineCounter != 1: #skip the header line
                        writer.writerow(row)
                    lineCounter = lineCounter + 1


    def SaveAsCSV(self, fileType, fileName, outputFileName):
        '''
        Save the pandas dataframe as csv
        '''
        try:
            if fileType == "contracts":
                df = pd.read_csv(fileName, header=0, dtype={"contract_unique_id":object,
                                                            "seller_company_name":object,
                                                            "seller_history_name":object,
                                                            "customer_company_name":object,
                                                            "contract_affiliate":object,
                                                            "ferc_tariff_reference":object,
                                                            "contract_service_agreement_id":object,
                                                            "contract_execution_date":object,
                                                            "commencement_date_of_contract_term":object,
                                                            "contract_termination_date":object,
                                                            "actual_termination_date":object,
                                                            "extension_provision_description":object,
                                                            "class_name":object,
                                                            "term_name":object,
                                                            "increment_name":object,
                                                            "increment_peaking_name":object,
                                                            "product_type_name":object,
                                                            "product_name":object,
                                                            "quantity":object,
                                                            "units":object,
                                                            "rate":object,
                                                            "rate_minimum":object,
                                                            "rate_maximum":object,
                                                            "rate_description":object,
                                                            "rate_units":object,
                                                            "point_of_receipt_balancing_authority":object,
                                                            "point_of_receipt_specific_location":object,
                                                            "point_of_delivery_balancing_authority":object,
                                                            "point_of_delivery_specific_location":object,
                                                            "begin_date":object,
                                                            "end_date":object})
            elif fileType == "indexPub":
                df = pd.read_csv(fileName, header=0, dtype={"filer_unique_id":object,
                                                            "seller_company_name":object,
                                                            "index_price_publishers_to_which_sales_transactions_have_been_reported":object,
                                                            "transactions_reported":object})
            elif fileType == "ident":
                df = pd.read_csv(fileName, header=0, dtype={"filer_unique_id":object,
                                                            "company_name":object,
                                                            "company_identifier":object,
                                                            "contact_name":object,
                                                            "contact_title":object,
                                                            "contact_address":object,
                                                            "contact_city":object,
                                                            "contact_state":object,
                                                            "contact_zip":object,
                                                            "contact_country_name":object,
                                                            "contact_phone":object,
                                                            "contact_email":object,
                                                            "transactions_reported_to_index_price_publishers":object,
                                                            "filing_quarter":object})

            #sometimes, the contract files has this column. Add this column to the ones that do not have it
            if (fileType == "contracts") and ("seller_history_name" not in df.columns):
                df["seller_history_name"] = np.nan
                df = df[self.job["columns"]["contracts"]] #rearrange the column order to suit the tables in Redshift

            #replace all newline characters in the dataframe
            #df = df.applymap(lambda x: x.replace("\n", "") if type(x) is str else x)
            #Pylint recommended isinstance() instead of type()
            df = df.applymap(lambda x: x.replace("\n", "") if isinstance(x, str) else x)

            #===================================================================
            # run only if the returned object is a dataframe.
            # In some instances, there would be no data just lines of empty strings with quotes in it
            # and pandas applymap for some reason returns a Series object rather than a dataframe
            #===================================================================
            if isinstance(df, pd.DataFrame):
                dfLength = len(df)
                if dfLength > 0: #save as csv only if the file has non-zero number of rows except the header
                    df = self.pandasUtilities.RemoveNonAsciiCharacters(df)
                    df = self.pandasUtilities.ReplaceCharacters(df, self.job["charsToBeReplaced"]) #Replace characters as specified in the config file
                    df.to_csv(self.localTempDirectory + "/" + fileType + "/" + outputFileName, header=False, index=False)
        except Exception:
            self.logger.exception("Exception in PGCRFERCFilings.SaveAsCSV")
            self.logger.exception("Exception while handling the file: {}".format(fileName))
            raise


    def GetYear(self, filePart):
        '''
        Return the year part from the filename
        eg from a sample CSV_2013_Q3/CSV_2013_Q3_101673_487938/201309_Wolverine_Power_Supply_Cooperative-_Inc._indexPub.CSV
        returns 2013
        '''
        return filePart.replace(self.localTempDirectory + self.job["folderPath"]["raw"], "").split("_")[1]

    def GetQuarter(self, filePart):
        '''
        Return the month part from the filename
        eg from a sample CSV_2013_Q3/CSV_2013_Q3_101673_487938/201309_Wolverine_Power_Supply_Cooperative-_Inc._indexPub.CSV
        Possible values Q1, Q2, Q3, Q4
        '''
        return filePart.replace(self.localTempDirectory + self.job["folderPath"]["raw"], "").split("_")[2]

    @staticmethod
    def GetFileType(fileName):
        '''
        Return the fileType part from the filename
        eg from a sample CSV_2013_Q3/CSV_2013_Q3_101673_487938/201309_Wolverine_Power_Supply_Cooperative-_Inc._indexPub.CSV
        Possible values are: indexPub, transactions, contracts, ident
        '''
        return fileName.split("_")[-1].split(".")[0]

    def ClassifyFiles(self):
        '''
        classifies the files as Transactions, ident, contracts, indexPub
        '''
        self.logger.info("Inside PGCRFERCFilings.ClassifyFiles")
        #=======================================================================
        # get the list of all csv files
        # FileUtilities.ScanFolder doesn't do a recursive listing, hence wrote a new method
        #=======================================================================
        searchPath = self.localTempDirectory + self.job["folderPath"]["raw"]
        fileNames = self.fileUtilities.GetListOfFilesRecursively(searchPath, filetype="*.CSV")
        self.logger.info("{} files found".format(len(fileNames)))
        for fileName in fileNames:
            try:
                fileType = PGCRFERCQuarterlyFilings.GetFileType(os.path.basename(fileName)) #transactions, contracts, ident, indexPub
                outputFileName = os.path.basename(fileName) #returns the filename
                folderPath = os.path.dirname(fileName) #returns the directory name
                #===============================================================
                # Special handling for transactions to speed up the file processing
                # Use native file processing for transactions files
                #===============================================================
                if fileType == "transactions":
                    self.SaveTransactions(fileType, fileName, outputFileName)
                    FileUtilities.RemoveFileIfItExists(fileName) #deletes the input file after processing
                else:
                    self.SaveAsCSV(fileType, fileName, outputFileName)
                    FileUtilities.RemoveFileIfItExists(fileName) #deletes the input file after processing
            except Exception:
                self.logger.exception("Exception in PGCRFERCFilings.ClassifyFiles while handling a file in the path: {}".format(folderPath))
                raise


    def AppendFilesIntoCSV(self, listOfFiles, fileType):
        '''
        Prepare to call the AppendFiles method in FileUtilities
        Builds the list of dictionary [{"Name":"file1FullPath", "IgnoreLines":[]}, {"Name":"file2FullPath", "IgnoreLines":[]}]
        '''
        self.logger.info("Inside PGCRFERCFilings.AppendFilesIntoCSV")
        sourceFiles = []
        for csvFile in listOfFiles:
            sourceFiles.append({"Name":self.localTempDirectory + self.job["folderPath"][fileType] + csvFile, "IgnoreLines":[]})
        try:
            filePath = self.localTempDirectory + "/" + fileType + ".CSV"
            self.fileUtilities.AppendFiles(filePath, sourceFiles)
        except Exception:
            self.logger.exception("Exception in PGCRFERCFilings.AppendFilesIntoCSV")
            self.logger.exception("Exception while appending the file: {}".format(self.localTempDirectory + "/" + fileType + ".CSV"))
            raise
        return filePath

    def CombineFiles(self):
        '''
        Get the list of all CSV files in the path self.localTempDirectory + "/" + fileType
        '''
        self.logger.info("Inside PGCRFERCFilings.CombineFiles")
        try:
            appendedFiles = []
            fileTypes = [fileType for fileType in list(self.job["folderPath"].keys()) if fileType != "raw"]
            for fileType in fileTypes:
                listOfFiles = self.fileUtilities.GetListOfFiles(self.localTempDirectory + self.job["folderPath"][fileType], "*.CSV")
                filePath = self.AppendFilesIntoCSV(listOfFiles, fileType)
                appendedFiles.append({"filePath":filePath, "fileType":fileType})
        except Exception:
            self.logger.exception("Exception in PGCRFERCFilings.CombineFiles")
            raise
        return appendedFiles


    def CleanAndPack(self):
        '''
        Removes blank lines & empty strings ("") from files
        '''
        self.logger.info("Inside PGCRFERCFilings.RemoveBlankLines")
        for folder in list(self.job["folderPath"].keys()):
            files = self.fileUtilities.ScanFolder(self.localTempDirectory + "/" + folder + "/")
            for iFile in files:
                inputFile = self.localTempDirectory + "/" + folder + "/" + iFile
                cleanedFile = self.localTempDirectory + "/" + folder + "/cleaned_" + iFile
                self.fileUtilities.RemoveBlankLines(inputFile, cleanedFile)
                FileUtilities.RemoveFileIfItExists(inputFile)
                self.fileUtilities.GzipFile(cleanedFile, cleanedFile) #gzip the file
                self.fileUtilities.DeleteFile(cleanedFile) #delete the CSV file

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

    def DownloadFile(self, s3Key):
        '''
        Worker function to download the file
        '''
        self.logger.info(" Downloading file: " + s3Key)
        try:
            s3Key = "/" + s3Key
            unzippedFile = s3Key.split("/")[-1]
            localGzipFilepath = self.localTempDirectory + self.job["folderPath"]["raw"] + unzippedFile
            S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"], s3Key, localGzipFilepath)
        except Exception:
            self.logger.exception("Error while downloading file: {}".format(s3Key))
            raise

    def ProcessQuarterlyFilings(self):
        '''
        Process Quarterly Filings
        '''
        try:
            files = self.GetListOfFilesOnS3()
            for s3Key in files:
                self.DownloadFile(s3Key)
                self.logger.info("Starting to unzip files")
                self.RecursivelyUnzipFiles(self.localTempDirectory + self.job["folderPath"]["raw"])
                self.ClassifyFiles()
                self.PackFiles()
                self.UploadPackedToS3()
                self.CreateFolders()
        except Exception:
            self.logger.exception("Exception in PGCRFERCFilings.ProcessQuarterlyFilings")
            raise


    def PackFiles(self):
        '''
        Converts the cleaned CSV files into gz.
        Deletes the original CSV file
        '''
        self.logger.info("Packing files")
        for folder in [flder for flder in list(self.job["folderPath"].keys()) if flder != "raw"]:
            path = self.localTempDirectory + self.job["folderPath"][folder]
            csvFiles = self.fileUtilities.ScanFolder(path, ext=".CSV")
            for csvFile in csvFiles:
                self.fileUtilities.GzipFile(path + csvFile, path + csvFile + ".gz")
                self.fileUtilities.DeleteFile(path + csvFile)


    def UploadPackedToS3(self):
        '''
        Uploads all files packed to s3.
        '''
        self.logger.info("Uploading GZIP files to s3 folder...")
        for folder in [flder for flder in list(self.job["folderPath"].keys()) if flder != "raw"]:
            S3Utilities.CopyItemsAWSCli(self.localTempDirectory + self.job["folderPath"][folder],
                                        "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"][folder],
                                        "--recursive --quiet")

    def DeleteFilesFromAWS(self, table):
        '''
        Deletes files from AWS
        '''
        s3DataFolder = "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"][table]
        S3Utilities.DeleteFileFromS3TempUsingAWSCLi(s3DataFolder, "--recursive --quiet")

    def LoadQFTables(self, table):
        '''
        Performs the final step to insert multiple files located in s3 into the final table in Redshift.
        '''
        rsConnect = None
        try:
            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])
            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": self.job["destinationSchema"],
                                                 "tableName": self.job["tableName"] + table,
                                                 "s3Filename": "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"][table],
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": self.job["delimiter"]
                                             },
                                             self.logger, "N")

            self.logger.info("Cleaning s3 data folder...")
            self.DeleteFilesFromAWS(table)
        except Exception as ex:
            self.logger.exception("Error while trying to save into Redshift from s3 folder: " + ex.message)
            raise
        finally:
            if rsConnect is not None:
                rsConnect.close()

    def CreateFolders(self):
        '''
        Creates folders
        '''
        FileUtilities.EmptyFolderContents(self.localTempDirectory + self.job["folderPath"]["raw"])
        FileUtilities.EmptyFolderContents(self.localTempDirectory + self.job["folderPath"]["ident"])
        FileUtilities.EmptyFolderContents(self.localTempDirectory + self.job["folderPath"]["transactions"])
        FileUtilities.EmptyFolderContents(self.localTempDirectory + self.job["folderPath"]["contracts"])
        FileUtilities.EmptyFolderContents(self.localTempDirectory + self.job["folderPath"]["indexPub"])

    def PostLoadETL(self):
        '''
        Create the L2 tables post data load
        '''
        postLoadScriptTemplate = self.job.get("PostLoadScript")
        if postLoadScriptTemplate is not None:
            sqlTableCreationScript = super(PGCRFERCQuarterlyFilings, self).BuildTableCreationScript(postLoadScriptTemplate)
            RedshiftUtilities.PSqlExecute(sqlTableCreationScript, self.logger)
            self.logger.info(self.moduleName + " - SQL tables created.")


    def Start(self, logger, moduleName, filelocs):
        '''
        Application starting point
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.pandasUtilities = PandasUtilities(self.logger)
            self.CreateFolders()
            #===================================================================
            # delete files from S3 bucket
            #===================================================================
            for folder in [flder for flder in list(self.job["folderPath"].keys()) if flder != "raw"]:
                self.DeleteFilesFromAWS(self.job["folderPath"][folder].replace("/", ""))
            #===================================================================
            # Process the files
            #===================================================================
            self.ProcessQuarterlyFilings()
            #===================================================================
            # Load into Redshift
            #===================================================================
            for folder in [flder for flder in list(self.job["folderPath"].keys()) if flder != "raw"]:
                self.LoadQFTables(self.job["folderPath"][folder].replace("/", ""))
            #===================================================================
            # do post load ETL & view creation
            #===================================================================
            self.PostLoadETL()
        except Exception as ex:
            self.logger.exception(moduleName + " - Exception!")
            self.logger.exception(str(ex))
            raise
