'''
Created on Apr 21, 2017
@author: Varun Muriyanat
'''
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.DBFUtilities import DBFUtilities
import pandas as pd

class Form1:
    '''
    The instance of this class loops through the foxpro items in the config file and calls the FoxProUtilities
    '''
    def __init__(self, logger, fileUtilities, localTempDirectory, config, awsParams):
        '''
        Constructor of this class
        '''
        self.logger = logger
        self.awsParams = awsParams
        self.fileUtilities = fileUtilities
        self.localTempDirectory = localTempDirectory
        self.job = config
        self.ProcessFiles()

    def ProcessFiles(self):
        '''
        Processes the files
        '''
        self.logger.info("Processing Form1 files")
        self.CreateFolders()
        self.Convert2CSV()
        self.ConvertToPipeDelimitedFile()
        self.CleanFiles() #remove the characters as specified in "charsToBeReplaced"
        self.LoadCSVIntoRedshift()


    def Convert2CSV(self):
        '''
        Leverages the Perl script to convert DBF (foxpro) files into CSV
        '''
        dbfu = DBFUtilities(self.logger)
        for job in self.job["foxpro_files"]:
            fileInName = self.localTempDirectory + "/" + job["Name"] + ".DBF"
            self.logger.info("Converting file: {} into CSV".format(fileInName))
#             fileOutName = self.localTempDirectory + "/" + job["Name"] + ".csv"
#             delimiter = "|"
#             includeHeader=True
#             dbfu.ConvertToCSV(fileInName, fileOutName, delimiter, includeHeader)
            dbfu.ConvertToCSV2(fileInName)

    def CreateFolders(self):
        '''
        Creates the folders if they do not exist already
        '''
        FileUtilities.CreateFolder(self.localTempDirectory + "/processed/")
        FileUtilities.CreateFolder(self.localTempDirectory + "/cleaned/")

    def ConvertToPipeDelimitedFile(self):
        '''
        Converts comma separated files to pipe separated files
        '''
        for job in self.job["foxpro_files"]:
            try:
                df = pd.read_csv(self.localTempDirectory + "/" + job["Name"] + ".CSV")
                #drop the junk columns
                for column in self.job["csvColumnsExclusionList"]:
                    if column in df.columns: #check if the column is present in the dataframe
                        df = df.drop(str(column), 1)
                df.to_csv(self.localTempDirectory + "/processed/" + job["Name"] + ".CSV",
                          sep=str(self.job["outputDelimiter"]),
                          na_rep="",
                          header=False,
                          index=False)
            except:
                self.logger.exception("Error converting {} to PipeDelimited File!".format(self.localTempDirectory + "/cleaned/" + job["Name"] + ".CSV"))
                raise

    def CleanFiles(self):
        '''
        Cleans the files
        '''
        for job in self.job["foxpro_files"]:
            try:
                inputFile = self.localTempDirectory + "/processed/" + job["Name"] + ".CSV"
                outputFile = self.localTempDirectory + "/cleaned/" + job["Name"] + ".CSV"
                self.fileUtilities.ReplaceIterativelyInFile(inputFile, outputFile, self.job["charsToBeReplaced"])
            except Exception as e:
                self.logger.exception("Error while cleaning the file: {}".format(inputFile))
                self.logger.exception("{}".format(str(e)))
                raise

    def LoadCSVIntoRedshift(self):
        '''
        Loops through the files to load them into redshift
        '''
        for job in self.job["foxpro_files"]:
            self.LoadIntoRedshift(job)

    def LoadIntoRedshift(self, job):
        '''
        Does the actual loading of data into Redshift
        '''
        self.logger.info("Loading {} into Redshift".format(job["Name"]))
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
                                                   self.localTempDirectory + "/cleaned/" + job["Name"] + ".CSV",
                                                   self.job["destinationSchema"],
                                                   self.job["tableName"].lower().replace("f1_", "") + job["Name"],
                                                   self.job["fileFormat"],
                                                   self.job["dateFormat"],
                                                   self.job["outputDelimiter"])
            rsConnect.close()
        except Exception as e:
            self.logger.exception("we had an error in FoxPro.LoadIntoRedshift() while loading data into Redshift")
            self.logger.exception("{}".format(str(e)))
            
