'''
Created on Nov 13, 2017

@author: VIU53188
@summary: This application was modified from the Chemicals application to include spark athena framework
'''

import os
import re

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class ChemicalsAthenaSpark(ApplicationBase):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
@summary: Explanation of sql
        The primary table is PeriodValues all the other tables are used as supporting
        Product --> to get the definition of the product if needed
        Location --> obtain the description of location and although I am against hardcoding and because of the layout of the data
                1)  LocationTypeID of 2 is regional and this data could be double dipped so we are omitting those as well as
                2)  Exceptions to the rule are USR and DDR that also has the chance to be double dippled
        Category -->  obtain the description of the category ID
                1)  We are only interested in specific categories and based on our Chemical expert they are :
                    a.  10  --> Production
                    b.  12  --> Total Supply
                    c.  15  --> Total Demand
                    d.  19  --> Domestic Demand
        SubCategory -->  although we are not using this table it is important to note it since we are only looking at the summary level
                         where the value = 0 rather than each broken down level from the PeriodValues table

        Last comment is the we are storing the name of the database in the config file since it could change
        periodically depending on what is provided
        for instance currently the last good database is WASP_2015t
        There are two pulls of the database each one 6 months apart.  The first one will be in the form of WASP2015
        and the next on six months later will have a t at the end such as WASP2015t
        '''
        super(ChemicalsAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def BulkExtract(self, sqlPullDataScript, outputCSV):
        '''
        calls BCP module to pull data
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "BulkExtract" + " starting ")
            self.bcpUtilities.RunBCPJob(self.job["mssqlLoginInfo"],
                                        self.job["bcpUtilityDirOnLinux"],
                                        self.fileUtilities.LoadSQLQuery(sqlPullDataScript),
                                        outputCSV,
                                        self.job["delimiter"],
                                        packetSize='65535')
            self.logger.debug(self.moduleName + " -- " + "BulkExtract" + " finished ")
        except Exception as err:
            self.logger.exception(self.moduleName + " - we had an error in BulkExtract -- " + err.message)
            raise

    def CreatePullScript(self, tables):
        '''
        takes the template for the pull script and customizes it for the data we need
        '''
        sqlPullDataScript = None
        try:
            self.logger.debug(self.moduleName + " -- " + "CreatePullScript" + " starting ")
            sqlPullDataTemplate = self.location + '/sql/' + tables["pullTemplate"]
            sqlPullDataScript = self.localTempDirectory + "/sql/" + re.sub('Template.sql$', '.sql', tables["pullTemplate"])
            FileUtilities.RemoveFileIfItExists(sqlPullDataScript)

            with open(sqlPullDataTemplate) as infile, open(sqlPullDataScript, 'w') as outfile:
                for line in infile:
                    outfile.write(line)
            self.logger.debug(self.moduleName + " -- " + "CreatePullScript" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in CreatePullScript")
            raise
        return sqlPullDataScript

    def ProcessTables(self, dbCommon, tables):
        '''
        get the last partition value and use that as the date to pull data
        then put that data into Athena
        '''
        try:
            outputCSV = self.fileUtilities.csvFolder + self.moduleName + ".CSV"
            fieldTerminator = self.job["fieldTerminator"]
            if "fieldTerminator" in tables:
                fieldTerminator = tables["fieldTerminator"]            
            rowTerminator = None # Not using this. Stick with the default of CR/LF.  self.job["rowTerminator"]
 
            if "pullTemplate" in tables:
                sqlPullDataScript = self.CreatePullScript(tables)
                self.bcpUtilities.BulkExtract(self.fileUtilities.LoadSQLQuery(sqlPullDataScript),
                                              outputCSV, dbCommon, tables, fieldTerminator, rowTerminator,
                                              self.job["bcpUtilityDirOnLinux"], self.fileUtilities, self.logger)
                
            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            df = SparkUtilities.ReadCSVFile(spark, tables, self.job["fieldTerminator"], False,
                                            self.fileUtilities.csvFolder, self.logger)
            SparkUtilities.SaveParquet(df, self.fileUtilities)
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
            if "loadToRedshift" in tables and tables["loadToRedshift"] == "Y":
                    self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)

        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessTables")
            raise 

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)