'''
Created on Feb 14, 2017

@author: VIU53188
@summary: Application will read the sample data spreadsheet provided by Lou Zhang
        1) take in the instance of ApplicationBase that sets up all the standard configurations
        2) open Excel file and look for Sheet "DataImport" normally the first sheet
        3) use pandas to put data into data frame
        4) Generate CSV from values
        5) load CSV into Postgres database for EAA Web server into staging table
        6) use staging table to populate final tables
'''

import os
import ntpath
import psycopg2         ###  supports connectivity to RedShift

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from Applications.Common.ApplicationBase import ApplicationBase
import pandas as pd

class SampleData(ApplicationBase):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        super(SampleData, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def GetPSConnection(self):
        '''
        get connection to PS local database
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

    def CreatePostgresTables(self, psConnect):
        '''
        Create the tables in the PS local database
        '''
        sqlTableCreationScript = self.BuildTableCreationScript(self.job['psSqlScript'])

        # The following code will recreate all the tables.  EXISTING DATA WILL BE DELETED
        RedshiftUtilities.ExecuteSQLScript(psConnect, sqlTableCreationScript, self.logger)
        self.logger.info(self.moduleName + " - SQL tables created.")

    def LoadBaseAttributes(self, psConnect):
        '''
        load the data into PS database
        '''
        ddlLocation = self.location + "/DDLS"
        seriesAttributeFile = ddlLocation + r"/Series_attributes.sql"
        with open(seriesAttributeFile, 'r') as myfile:
            mysql = myfile.read()
        #=======================================================================
        # mysql = """
        #     insert into eaa_dev.series_attributes
        #         (iD,name,category)
        #     values
        #         ('1000', 'GLM','TOP'),
        #         ('2000', 'ARIMA','TOP'),
        #         ('3000', 'LASSO','TOP'),
        #         ('4000', 'NN','TOP'),
        #         ('5000', 'SPECTRE','TOP');
        #
        #     commit;
        # """
        #=======================================================================
        try:
            cur = psConnect.cursor()
            cur.execute(mysql)
            psConnect.commit()
            cur.close()
        except:
            self.logger.exception("we had an error in LoadBaseAttributes")
            raise

    def LoadDataFromPostgresTempDir(self, psConnect, fileInTempDir):
        '''
        load data from temp folder
        '''
        mySql = "COPY " + self.job["destinationSchema"] + ".SampleResults FROM '" + fileInTempDir + "'" + \
                " WITH DELIMITER AS '" + self.job["delimiter"] + "' CSV HEADER"
        cur = psConnect.cursor()
        cur.execute(mySql)
        cur.close()

    def LoadBaseData(self, psConnect, iD, field):
        '''
        load data into table
        '''
        try:
            mySql = """
                insert into eaa_dev.series_data
                (
                  attr_id,
                  DATE,
                  TYPE,
                  value
                )
                select '""" + iD + """',
                  TO_DATE(valuationdate, 'YYYY-MM-DD'),
                  case when (TO_DATE(valuationdate, 'YYYY-MM-DD') > now()) then 'F' else 'A' end,
                   """ + field + """
                from eaa_dev.sampleresults;

                commit;        
            """
            cur = psConnect.cursor()
            cur.execute(mySql)
            psConnect.commit()
            cur.close()
        except:
            self.logger.exception("we had an error in LoadBaseData")
            raise

    def DownloadFromS3ToPSTempDir(self, psConnect, bucketName, s3TempKey):
        '''
        call function to pull file
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

    def Start(self, logger, moduleName, filelocs):
        '''
        main routine
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)

            s3Key = self.job["s3SrcDirectory"] + "/" + self.job["fileToLoad"]
            self.logger.info(self.moduleName + " - Processing file: " + s3Key)

            localFilepath = self.localTempDirectory + "/" + ntpath.basename(s3Key)
            S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"], s3Key, localFilepath)

            df = pd.read_excel(localFilepath, "Major Variables", index_col=None, na_values=['NaN'],
                               skiprows=1, parse_cols="C:E,G:I", header=None)

            #  Save the data as CSV
            outputCSVfileName = self.localTempDirectory + '/SampleData.csv'
            df.to_csv(outputCSVfileName, sep=str(self.job["delimiter"]), encoding='utf-8', index=False)

            # Update the CSV file into a temporary S3 location.  Postgres will download it from there to its local directory
            bucketName, s3TempKey = S3Utilities.UploadFileToS3Temp(self.awsParams.s3, outputCSVfileName)

            psConnect = self.GetPSConnection()
            # Postgres tables are created using a connection (rather than psql)
            self.CreatePostgresTables(psConnect)

            postgresTempFile = self.DownloadFromS3ToPSTempDir(psConnect, bucketName, s3TempKey)
            self.LoadDataFromPostgresTempDir(psConnect, postgresTempFile)

            S3Utilities.DeleteFile(self.awsParams.s3, bucketName, s3TempKey)

            self.LoadBaseAttributes(psConnect)
            self.LoadBaseData(psConnect, '1000', 'glm_value')
            self.LoadBaseData(psConnect, '2000', 'arima_value')
            self.LoadBaseData(psConnect, '3000', 'lasso_value')
#           self.LoadBaseData(psConnect,'4000', 'nn_value')
#            self.LoadBaseData(psConnect,'5000', 'spectre_value')

            psConnect.close()
            self.logger.debug(" SampleData CSV loaded to RedShift")

        except:
            logger.exception(moduleName + " - Exception in start!")
            raise
