'''
Created on Mar 22, 2017

@author: Hector Hernandez
@summary: Extracts the risks values from the IHS Connect API.

'''
import os
import urllib2
import base64
import json
from pandas.io.json import json_normalize

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase

class ECRConnect(ApplicationBase):
    '''
    This class is used to get the Risk data from IHS Connect, transform it and load it into Redshift.
    '''

    def __init__(self):
        '''
        Initial settings
        '''
        super(ECRConnect, self).__init__()

        self.awsParams = ""
        self.csvFile = None
        self.csvFileHistory = None
        self.fileUtilities = FileUtilities(self.logger)
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def TransformToCsv(self, jData):
        '''
        Transforms from json to csv file.
        '''
        try:
            # Gets the latest version
            df = json_normalize(jData, 'Risks', ['Country'])
            df['ClassName'] = ''
            df['ClassAvg'] = ''
            df = df[['Country', 'Name', 'Value', 'Description', 'ClassName', 'ClassAvg', 'UpdatedOn']]

            df.to_csv(self.csvFile,
                      header=False,
                      sep=str(self.job["delimiter"]),
                      encoding='utf-8',
                      index=False)

            self.fileUtilities.GzipFile(self.csvFile, self.csvFile + ".gz")
            self.fileUtilities.RemoveFileIfItExists(self.csvFile)

            # Gets the history
            df = json_normalize(jData, ['Risks', 'History'], ['Country', ['Risks', 'Name']])
            df = df[['Country', 'Risks.Name', 'Value', 'UpdatedOn']]

            df.to_csv(self.csvFileHistory,
                      header=False,
                      sep=str(self.job["delimiter"]),
                      encoding='utf-8',
                      index=False)

            self.fileUtilities.GzipFile(self.csvFileHistory, self.csvFileHistory + ".gz")
            self.fileUtilities.RemoveFileIfItExists(self.csvFileHistory)
        except Exception as err:
            self.logger.error("Error while trying to transform json to csv. Error:" + err.message)
            raise

    def GetAndTransform(self):
        '''
        Download all files.
        '''
        try:
            request = urllib2.Request(self.job["connectAPI"]["baseurl"] + self.job["connectAPI"]["riskService"])
            base64string = base64.b64encode('%s:%s' % (self.job["connectAPI"]["username"], self.job["connectAPI"]["password"]))
            request.add_header("Authorization", "Basic %s" % base64string)

            response = urllib2.urlopen(request)
            jData = json.load(response)

            self.TransformToCsv(jData)
        except Exception as err:
            self.logger.error("Error while trying to get and transform from IHS Connect API service. Error:" + err.message)
            raise

    def LoadAllFromS3(self, s3Source, tableName):
        '''
        Process a single category configured in the categories dictionary in the jobConfig.
        '''
        try:
            s3DataSource = "s3://" + self.job["bucketName"] + s3Source

            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": self.job["destinationSchema"],
                                                 "tableName": tableName,
                                                 "s3Filename": s3DataSource,
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": self.job["delimiter"]
                                             },
                                             self.logger, "N")
        except Exception:
            self.logger.error(self.moduleName + " - Error while trying to save into Redshift from s3 folder.")
            raise

    def UploadToS3(self):
        '''
        Uploads all GZIP files created into S3 to be uploaded later...
        '''
        self.logger.info(self.moduleName + " - Uploading GZIP files to s3 folder...")
        fileName = self.job["fileNameOut"] + ".gz"
        fileNameHistory = self.job["fileNameOutHistory"] + ".gz"

        S3Utilities.CopyItemsAWSCli(self.localTempDirectory + "/" + fileName,
                                    's3://' + self.job["bucketName"] + self.job["s3ToDirectory"] + '/' + fileName)

        S3Utilities.CopyItemsAWSCli(self.localTempDirectory + "/" + fileNameHistory,
                                    's3://' + self.job["bucketName"] + self.job["s3ToDirectory"] + '/' + fileNameHistory)

    def ExecutePostETL(self):
        '''
        Will execute the post load sql script...
        '''
        try:
            sqlTemplate = self.location + "/" + self.job["postSQLScript"]
            sqlScript = self.localTempDirectory + "/" + self.job["postSQLScript"]

            self.fileUtilities.CreateActualFileFromTemplate(sqlTemplate, sqlScript, self.job["destinationSchema"], self.job["tableName"])
            RedshiftUtilities.PSqlExecute(sqlScript, self.logger)
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while updating the countries codes. Message: " + err.message)
            raise

    def Start(self, logger, moduleName, filelocs):
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)

            self.csvFile = self.localTempDirectory + "/" + self.job["fileNameOut"]
            self.csvFileHistory = self.localTempDirectory + "/" + self.job["fileNameOutHistory"]

            self.GetAndTransform()
            self.UploadToS3()
            self.LoadAllFromS3(self.job["s3ToDirectory"] + '/' + self.job["fileNameOut"] + '.gz', self.job["tableName"])
            self.LoadAllFromS3(self.job["s3ToDirectory"] + '/' + self.job["fileNameOutHistory"] + '.gz', self.job["tableName"] + '_history')
            self.LoadAllFromS3(self.job["xReference"]["s3DataDirectory"], self.job["tableName"] + self.job["xReference"]["tableNameSfx"])
            self.ExecutePostETL()
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            raise Exception(err.message)
