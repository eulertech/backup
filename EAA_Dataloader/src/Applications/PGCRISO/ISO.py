'''
Created on Apr 21, 2017
@author: Varun Muriyanat
'''
from Applications.Common.ApplicationBase import ApplicationBase
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities

class ISO(ApplicationBase):
    '''
    ISO handler
    '''
    def __init__(self, logger, fileUtilities, localTempDirectory, config, awsParams):
        '''
        Constructor for this class
        '''
        super(ISO, self).__init__()
        self.awsParams = awsParams
        self.logger = logger
        self.job = config
        self.fileUtilities = fileUtilities
        self.localTempDirectory = localTempDirectory
        self.ProcessISOs()

    def ProcessISOs(self):
        '''
        loop through all the iso types in the config file and clean, remove header and load into Redshift
        '''
        for iso in self.job["iso_files"]:
            self.logger.info("Processing iso: " + iso["Name"])
            self.CleanFiles(iso) #removes junk, lines to be ignored
            self.RemoveHeader(iso)
            self.LoadFiles(iso)


    def CleanFiles(self, iso):
        '''
        Generic cleaning wrapper
        '''
        self.logger.info("Cleaning data")
        ignoreLines = None
        columnCount = None
        inputPath = self.localTempDirectory + "/" + iso["Name"] + "/"
        outputPath = self.localTempDirectory + "/" + iso["Name"] + "/cleaned/"
        FileUtilities.CreateFolder(outputPath) #creates the cleaned folder if doesn't already exist
        if iso.get("IgnoreLines") is not None:
            ignoreLines = iso.get("IgnoreLines")
        if iso.get("column_count") is not None:
            columnCount = iso.get("column_count")
        listOfFiles = self.fileUtilities.GetListOfFiles(inputPath, self.job["input_file_type"]) #get all the CSV files
        self.logger.info("Files found: {}".format(str(len(listOfFiles))))
        for fp in listOfFiles:
            try:
                self.fileUtilities.CleanFile(inputPath + fp,
                                             outputPath + fp,
                                             IgnoreLines=ignoreLines,
                                             ColumnCount=columnCount,
                                             Delimiter=self.job["delimiter"])
            except Exception as ex:
                self.logger.exception("Error while cleaning the MISO file: {}".format(fp))
                self.logger.exception("{}".format(str(ex)))
                raise

    def RemoveHeader(self, iso):
        '''
        Removes the header (1st row) of each file under iso
        '''
        self.logger.info("Removing header from the ISO datasets")
        inputPath = self.localTempDirectory + "/" + iso["Name"] + "/cleaned/"
        outputPath = self.localTempDirectory + "/" + iso["Name"] + "/processed/"
        listOfFiles = self.fileUtilities.GetListOfFiles(inputPath, self.job["input_file_type"]) #get all the CSV files
        for fp in listOfFiles:
            try:
                FileUtilities.RemoveHeader(inputPath, outputPath, fp) #remove the header from the csv file before loading into Redshift
            except Exception as ex:
                self.logger.exception("Exception while removing header of file {}".format(fp))
                self.logger.excpetion("{}".format(str(ex)))
                raise

    def LoadFiles(self, iso):
        '''
        Loop through the files for the give ISO and call the LoadISOData
        '''
        self.logger.info("Loading ISO files into Redshift")
        localFilePath = self.localTempDirectory + "/" + iso["Name"] + "/processed/"
        listOfFiles = self.fileUtilities.GetListOfFiles(localFilePath, self.job["input_file_type"])
        for fp in listOfFiles:
            self.LoadData(iso, localFilePath, fp)

    def LoadData(self, iso, localFilePath, fp):
        '''
        Method to load ISO data into Redshift
        '''
        self.logger.info("Loading ISO data into Redshift")
        rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                              host=self.awsParams.redshift['Hostname'],
                                              port=self.awsParams.redshift['Port'],
                                              user=self.awsParams.redshiftCredential['Username'],
                                              password=self.awsParams.redshiftCredential['Password'])
        RedshiftUtilities.LoadFileIntoRedshift(rsConnect,
                                               self.awsParams.s3,
                                               self.logger,
                                               self.fileUtilities,
                                               localFilePath + fp,
                                               self.job["destinationSchema"],
                                               self.job["tableName"] + iso["Name"],
                                               self.job["fileFormat"],
                                               self.job["dateFormat"],
                                               self.job["delimiter"])
