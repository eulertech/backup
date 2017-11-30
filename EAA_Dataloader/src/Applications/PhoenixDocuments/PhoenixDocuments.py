'''
Created on Mar 13, 2017

@author: VIU53188
@summary: Application will load the documents data from Phoenix
        1) take in the instance of ApplicationBase that sets up all the standard configurations
        2) call windows web service to that will pull data from Phoenix
        3) create csv from web service
        4) load csv into eaa web postgres database
'''
import os
import ntpath
import urllib2
import json
import csv
from Applications.Common.ApplicationBase import ApplicationBase
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities
import psycopg2         ###  supports connectivity to RedShift

class PhoenixDocuments(ApplicationBase):
    '''
    load the documents data from Phoenix
    '''
    def __init__(self):
        '''
        Constructor
        '''
        super(PhoenixDocuments, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
    def PullDataFromPhoenix(self):
        '''
        pull date from Phoenix using web service
        '''
        url = self.job["PhoenixDocumentService"]
        ##
        # make the call
        ##
        try:
            req = urllib2.Request(url)
            response = urllib2.urlopen(req)
            jo = json.load(response)
            return jo
##            response_text = response.read().decode("utf-8")
        except:
            self.logger.exception(self.moduleName + " - we had an error in PullDataFromPhoenix")
            raise

    def GetPSConnection(self):
        '''
        get connection to local PS database
        '''
        try:
            psConnect = psycopg2.connect(dbname=self.job["psInfo"]['dataBase'],
                                         host=self.job["psInfo"]['hostName'],
                                         port=self.job["psInfo"]['Port'],
                                         user=self.job["psInfo"]['Username'],
                                         password=self.job["psInfo"]['Password'])
            return psConnect
        except:
            self.logger.exception("we had an error in GetPSConnection")
            raise
    def ExportToCSV(self, outfile, jsonDocuments):
        '''
        Create CSV file from json objectg
        '''
        try:
            csvfile = open(outfile, 'wb')
            csvWriter = csv.writer(csvfile)
            for row in jsonDocuments:
                outrow = []
                outrow.append(unicode(row["attr_id"]).encode("utf-8"))
                outrow.append(unicode(row["titlehtml"]).encode("utf-8"))
                outrow.append(unicode(row["bodyhtml"]).encode("utf-8"))
                outrow.append(unicode(row["publishdate"]).encode("utf-8"))
                outrow.append(unicode(row["taxonomyname"]).encode("utf-8"))
                outrow.append(unicode(row["taxonomyvalueid"]).encode("utf-8"))
                csvWriter.writerow(outrow)
        except:
            self.logger.exception(self.moduleName + " - Exception in ExportToCSV!")
            raise
    def CreatePostgresTables(self, psConnect):
        '''
        Create table in local PS database  Data will be removed
        '''
        sqlTableCreationScript = self.BuildTableCreationScript(self.job['psSqlScript'])

        # The following code will recreate all the tables.  EXISTING DATA WILL BE DELETED
        RedshiftUtilities.ExecuteSQLScript(psConnect, sqlTableCreationScript, self.logger)
        self.logger.info(self.moduleName + " - SQL tables created.")

    def DownloadFromS3ToPSTempDir(self, psConnect, bucketName, s3TempKey):
        '''
        Pull data from S3
        '''
        try:
            tempDirOnPostgres = "/tmp/" + ntpath.basename(s3TempKey)
            mysql = "select * FROM " + self.job["destinationSchema"] + ".download_file_from_s3(" + \
                    "'" + self.awsParams.s3['access_key_id'] + "'," +\
                    "'" + self.awsParams.s3['secret_access_key'] + "'," +\
                    "'" + bucketName + "'," +\
                    "'" + s3TempKey + "'," +\
                    "'" + tempDirOnPostgres + "')"

            cur = psConnect.cursor()
            cur.execute(mysql)
            cur.close()

            return tempDirOnPostgres
        except:
            self.logger.exception("we had an error in loadtoPostgres")
            raise

    def LoadDataFromPostgresTempDir(self, psConnect, fileInTempDir):
        '''
        loads the data from our temp folder to database
        '''
        mySql = "COPY " + self.job["destinationSchema"] + "." +  self.job["tableName"] +\
                " FROM '" + fileInTempDir + "'" +\
                " WITH DELIMITER AS '" + self.job["delimiter"] + "' CSV HEADER"
        cur = psConnect.cursor()
        cur.execute(mySql)
        psConnect.commit()
        cur.close()

    def Start(self, logger, moduleName, filelocs):
        '''
        main routine
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.info(self.moduleName + " - Processing: ")
            outputCSVfileName = self.localTempDirectory + '/PheonixDocuments.csv'

            self.logger.info(self.moduleName + " - Pull documents from Phoenix: ")
            jsonDocuments = self.PullDataFromPhoenix()
            self.logger.info(self.moduleName + " - save contents to CSV file from Phoenix: ")
            self.ExportToCSV(outputCSVfileName, jsonDocuments)
            self.logger.info(self.moduleName + " - push documents csv file to S3: ")
            bucketName, s3TempKey = S3Utilities.UploadFileToS3Temp(self.awsParams.s3, outputCSVfileName)

            self.logger.info(self.moduleName + " - Create document table: ")
            psConnect = self.GetPSConnection()
            self.CreatePostgresTables(psConnect)

            self.logger.info(self.moduleName + " - pull document s3 to database server temp: ")
            postgresTempFile = self.DownloadFromS3ToPSTempDir(psConnect, bucketName, s3TempKey)
            self.logger.info(self.moduleName + " - load documents csv file: ")
            self.LoadDataFromPostgresTempDir(psConnect, postgresTempFile)
            self.logger.info(self.moduleName + " - clean up temp file: ")
            S3Utilities.DeleteFile(self.awsParams.s3, bucketName, s3TempKey)
        except:
            logger.exception(moduleName + " - Exception in start!")
            raise
        