import os
from Applications.Common.ApplicationBase import ApplicationBase
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities
import psycopg2 # library to support connection to Redshift (Postgres)
from HindsightQC import HindsightQC

class Hindsight(ApplicationBase):
    def __init__(self):
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        self.awsParams = ""

    def ProcessSubJob(self, subJob):
        self.logger.info("Start the bcpUtilities.RunBCPJob for table " + subJob.get("destination"))   
        #TODO: set time capture to log file
        self.bcpUtilities.RunBCPJob(self.job["bcpParameters"]["sqlServerloginInfo"],                                            
                                    self.job["bcpParameters"]["bcpUtilityDirOnLinux"], 
                                    self.fileUtilities.LoadSQLQuery(self.fileUtilities.GetApplicationDirectory("Hindsight") + subJob.get("inputQuery")),                                             
                                    self.localTempDirectory + "/" + subJob.get("destination"), 
                                    subJob.get("fieldTerminator"), 
                                    subJob.get("rowTerminator"))        

        self.logger.info("Start of the cleaning process for table " + subJob.get("destination"))
        if (subJob.get("charsToBeReplaced") is not None) and (len(subJob.get("charsToBeReplaced")) != 0):
            self.fileUtilities.ReplaceIterativelyInFile(self.bcpUtilities.GetFullFilePath(subJob.get("destination")), self.bcpUtilities.GetFullFilePath(self.bcpUtilities.GetFileToBeUploaded(subJob.get("destination"), subJob.get("charsToBeReplaced"))), subJob.get("charsToBeReplaced"))

        self.logger.info("Start of the uploading process to Redshift process for table " + subJob.get("destination"))
        rsConnect = psycopg2.connect(dbname = self.awsParams.redshift['Database'], 
                                     host = self.awsParams.redshift['Hostname'], 
                                     port = self.awsParams.redshift['Port'],
                                     user = self.awsParams.redshiftCredential['Username'], 
                                     password = self.awsParams.redshiftCredential['Password'])

        RedshiftUtilities.LoadFileIntoRedshift(rsConnect, 
                                               self.awsParams.s3, 
                                               self.logger, 
                                               self.fileUtilities, 
                                               self.bcpUtilities.GetFullFilePath(self.bcpUtilities.GetFileToBeUploaded(subJob.get("destination"), subJob.get("charsToBeReplaced"))),
                                               subJob["destinationSchema"], 
                                               subJob.get("destination"),
                                               self.job["bcpParameters"]["fileFormat"], 
                                               self.job["bcpParameters"]["dateFormat"], 
                                               self.job["bcpParameters"]["delimiter"])

        rsConnect.close()

    def Start(self, logger, moduleName, filelocs):
        try:            
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/QC/") #delete and recreate the folder                        
            hqc = HindsightQC(self.logger, self.fileUtilities, self.bcpUtilities, self.localTempDirectory)
            hqc.Get_sql_server_rowcounts("pre") #Get pre-ETL rowcounts

            #Execute the pre-etl queries
            for sqlFile in self.job["bcpParameters"].get("preETLQueries"):  
                RedshiftUtilities.PSqlExecute(self.fileUtilities.GetApplicationDirectory("Hindsight") + sqlFile, logger)                 

            for subJob in self.job["bcpParameters"]["subJobs"]: 
                if subJob.get("destinationSchema") is None:                     
                    subJob["destinationSchema"] = self.job["bcpParameters"]["destinationSchema"]                           
                    self.ProcessSubJob(subJob)

            #Get SQL Server rowcounts
            hqc.Get_sql_server_rowcounts("post")

            #Execute the post-etl queries to prepare the data post-ETL prior to loading into the production tables
            for sqlFile in self.job["bcpParameters"].get("postETLQueries"):  
                RedshiftUtilities.PSqlExecute(self.fileUtilities.GetApplicationDirectory("Hindsight") + sqlFile, logger)

            #Get Redshift rowcounts                        
            hqc.Get_redshift_rowcounts("post")

            #Execute the post-etl qc queries
            status = hqc.ValidateETL()

            #Check whether the ETL passed the QC
            #Check 1: inter-version counts. Are the difference beyond a particular threshold
            #Check 2: pre-sql v/s post-redshift. Are the differences beyond a particular threshold            
            #If the ETL doesn't pass the QC, do not update/insert the prod tables
            #If the ETL passed the QC, insert into production tables (data, attributes, history)
            if status == True:
                self.logger.info("ETL good to go")
                for sqlFile in self.job["bcpParameters"].get("FinalLoadQueries"):  
                    #===========================================================
                    # add a process to backup data/attributes history tables
                    # Download to S3
                    #===========================================================
                    RedshiftUtilities.PSqlExecute(self.fileUtilities.GetApplicationDirectory("Hindsight") + sqlFile, logger)                
            else:
                self.logger.warning("Bad ETL. No go!")

            print hqc.TimeElaspsed()
        except:
            logger.exception(moduleName + " - Exception!")
            raise        
