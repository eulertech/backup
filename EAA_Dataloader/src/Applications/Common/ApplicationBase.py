'''
Created on Feb, 2017

@author: Christopher Lewis
@summary: Application Base class to perform many of the basic ETL process
'''

import re
from abc import ABCMeta

from AACloudTools import ConfigureAWS
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.BCPUtilities import BCPUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.AthenaUtilities import AthenaUtilities
from AACloudTools.EtlLoggingUtilities import EtlLoggingUtilities

# pylint: disable=too-many-instance-attributes
class ApplicationBase(object):
    __metaclass__ = ABCMeta

    '''
    Application Base class to perform many of the basic ETL process
    '''
    def __init__(self):
        '''
        Define the class attributes
        '''
        self.logger = None
        self.moduleName = None
        self.awsParams = None
        self.fileUtilities = None
        self.bcpUtilities = None
        self.job = None
        self.localTempDirectory = None
        self.location = None
        self.localTempDataDirectory = None
        self.etlUtilities = None

    def BuildTableCreationScript(self, sqlTemplateFilename):
        '''
        Construct the actual DDL script from the template by replacing the appropriate tokens
        '''
        sqlTableCreationTemplate = self.location + '/' + sqlTemplateFilename
        sqlTableCreationScript = self.localTempDirectory + "/" + re.sub('Template.sql$', '.sql', sqlTemplateFilename)
        self.fileUtilities.CreateActualFileFromTemplate(sqlTableCreationTemplate, sqlTableCreationScript,\
                                                        self.job["destinationSchema"], self.job["tableName"])
        self.logger.info(self.moduleName + " - SQL files created.")
        return sqlTableCreationScript

    def BuildTableCreationScriptTable(self, sqlTemplateFilename, tableName, templateFolder=None, sqlFolder=None):
        '''
        Construct the actual DDL script from the template for the specific table by replacing the appropriate tokens
        '''
        sqlTableCreationTemplate = self.location + '/'
        if templateFolder is None:
            sqlTableCreationTemplate = sqlTableCreationTemplate + sqlTemplateFilename
        else:
            sqlTableCreationTemplate = sqlTableCreationTemplate + templateFolder + '/' + sqlTemplateFilename
            
        sqlTableCreationScript = self.localTempDirectory + "/"
        if sqlFolder is not None:
            sqlTableCreationScript = sqlTableCreationScript + sqlFolder + "/"
        sqlTableCreationScript = sqlTableCreationScript + tableName + re.sub('Template.sql$', '.sql', sqlTemplateFilename)
#        sqlTableCreationScript = self.localTempDirectory + "/" + tableName + re.sub('Template.sql$', '.sql', sqlTemplateFilename)
        self.fileUtilities.CreateActualFileFromTemplate(sqlTableCreationTemplate, sqlTableCreationScript, self.job["destinationSchema"], tableName)
        self.logger.info(self.moduleName + " - " + tableName + " - SQL files created.")
        return sqlTableCreationScript

    def CreateTables(self, sqlTemplateFilename):
        '''
        Create the actual tables
        '''
        sqlTableCreationScript = self.BuildTableCreationScript(sqlTemplateFilename)

        # The following code will recreate all the tables.  EXISTING DATA WILL BE DELETED
        RedshiftUtilities.PSqlExecute(sqlTableCreationScript, self.logger)
        self.logger.info(self.moduleName + " - SQL tables created.")

    def LoadEnvironmentVariables(self, logger):
        '''
        sub method to just load in all environment variables
        '''
        self.logger = logger
        # Load the AWS configuration parameters for S3 and Redshift
        self.awsParams = ConfigureAWS.ConfigureAWS()
        self.awsParams.LoadAWSConfiguration(self.logger)
        return self

    def Start(self, logger, moduleName, filelocs):
        '''
        Start the process.  Do the common operations.
        '''     
        self = self.LoadEnvironmentVariables(logger)
#        self.logger = logger
        self.logger.info(moduleName + " - Getting configuration information.")

        self.moduleName = moduleName

        # Load the job parameters
        self.fileUtilities = FileUtilities(logger)
        jobConfigFile = self.location + '/' 'jobConfig.json'
        self.job = self.fileUtilities.LoadJobConfiguration(jobConfigFile)

        # This is where all the work files will be created
        self.localTempDirectory = FileUtilities.PathToForwardSlash(filelocs["relativeOutputfolder"] + "/" + moduleName)
        FileUtilities.CreateFolder(self.localTempDirectory)

        # This is where all the local data will be located
        if "relativeInputfolder" in filelocs:
            self.localTempDataDirectory = FileUtilities.PathToForwardSlash(filelocs["relativeInputfolder"] + "/" + moduleName)
            FileUtilities.CreateFolder(self.localTempDataDirectory)

        self.bcpUtilities = BCPUtilities(logger, self.fileUtilities, self.awsParams, self.localTempDirectory)

        # Create tables if we have a valid script
        if "sqlScript" in self.job:
            self.CreateTables(self.job["sqlScript"])

        #  Create etlprocess log table if it does not already exist
        if "tblEtl" in filelocs:
            self.etlUtilities = EtlLoggingUtilities(self.logger)
            self.etlUtilities.awsParams = self.awsParams
            self.etlUtilities.filelocs = filelocs
            self.etlUtilities.moduleName = self.moduleName
            self.etlUtilities.appschema = filelocs["tblEtl"]["appschema"]
            self.etlUtilities.StartEtlLogging()

        if "folders" in self.job:
            self.fileUtilities.moduleName = self.moduleName
            self.fileUtilities.localBaseDirectory = self.localTempDirectory
            self.fileUtilities.CreateFolders(self.job["folders"])            

    def CreateFolders(self, subFolder):
        '''
        Create the various subfolders defined in the jobConfig.jon for the table being processes
        '''
        self.fileUtilities.moduleName = self.moduleName
        self.fileUtilities.localBaseDirectory = self.localTempDirectory + "/" + subFolder
        self.fileUtilities.CreateFolders(self.job["folders"])

    def UploadFilesCreateAthenaTablesAndSqlScripts(self, table, localParquetFolderName, partitionValue=None):
        '''
        Upload Parquet files into S3
        Create Athena Table/Partition
        Create script to create a RedShift table and save to S3 (note that the ETL may not necessarily load data into Redshift)
        Create script to insert data into Redshift and save to S3  (note that the ETL may not necessarily load data into Redshift)
        '''
        if not FileUtilities.FilesExistInFolder(localParquetFolderName + "*.parquet"):
            # Nothing was created.  We have a problem
            self.logger.info(self.moduleName + " - No parquet files were created for current partition in: " + localParquetFolderName + ".  Nothing was processed on Athena.")
            return False
        
        self.fileUtilities.CreateTableSql(table, self.fileUtilities.sqlFolder)

        scriptPartitionValue = partitionValue
        if AthenaUtilities.IsTablePartitioned(table):
            # For partitioned tables, the script will insert a where clause by default.  However, if we are doing a new load
            # skip the where clause so that we can have SQL script that is capable of loading all the data from Athena
            # into RedShift in the future 
            s3FolderLocation = AthenaUtilities.ComposeAthenaS3DataFileKey(table["schemaName"], table["table"])
            if not S3Utilities.KeyExist(self.awsParams, s3FolderLocation): # Do not update scripts if data has been previously loaded
                scriptPartitionValue = None
        AthenaUtilities.SqlToLoadDataFromAthena(self.logger, table, self.fileUtilities.sqlFolder, scriptPartitionValue)
        
        AthenaUtilities.UploadFilesAndCreateAthenaTables(self.awsParams, localParquetFolderName, table,
            self.fileUtilities.sqlFolder, self.logger, partitionValue)
        return True
        
    def LoadDataFromAthenaIntoRedShiftLocalScripts(self, table, customWhereCondition=None):
        '''
        If at a later time we decide to drop the Redshift table and re-load the data from Athena, we need a utility to do that
        '''
        # Under the hood the table will be recreated if the new flag is on or if the table does not exist
        # Load the data from Athena into RedShift after that.  The load query only loads what needed from Athena
        scriptToCreateRedshiftTable = FileUtilities.ComposeCreateTableSqlFilename(table, self.fileUtilities.sqlFolder)
        RedshiftUtilities.PSqlExecute(scriptToCreateRedshiftTable, self.logger)
        
        scriptToLoadDataFromAthena = AthenaUtilities.ComposeInsertIntoSqlFilename(table, self.fileUtilities.sqlFolder)
        if customWhereCondition:
            # Replace the existing where clause with the custom clause
            customWhereCondition = " AND " + customWhereCondition + ";"
            replacements = {';': customWhereCondition}
            scriptToLoadDataFromAthenaCustom = scriptToLoadDataFromAthena + "_custom.sql"
            self.fileUtilities.ReplaceStringInFile(scriptToLoadDataFromAthena, scriptToLoadDataFromAthenaCustom, replacements)
            scriptToLoadDataFromAthena = scriptToLoadDataFromAthenaCustom
        RedshiftUtilities.PSqlExecute(scriptToLoadDataFromAthena, self.logger)
        
    def LoadDataFromAthenaIntoRedShiftS3Scripts(self, table):
        '''
        If at a later time we decide to drop the Redshift table and re-load the data from Athena, we need a utility to do that
        '''
        # Download scripts from S3 to local folder
        AthenaUtilities.DownloadScriptsForRedShift(self.awsParams, table, self.fileUtilities.sqlFolder)
        self.LoadDataFromAthenaIntoRedShiftLocalScripts(table)
      
    def ProcessTables(self, dbCommon, tables):
        """ Process Tables in the actual derived class """
        # YOU MUST IMPLEMENT THIS METHOD IN THE DERIVED CLASS
        raise NotImplementedError()

    def ProcessCatalogs(self, dbCommon, catalog):
        '''
        pulls data from each table in the catalog
        '''
        try:
            self.logger.debug(self.moduleName + " -- ProcessCatalogs for  " + catalog["name"] + " starting")
            for tables in catalog["tables"]:
                self.ProcessTables(dbCommon, tables)
            self.logger.debug(self.moduleName + " -- ProcessCatalogs for  " + catalog["name"] + " finished ----------.")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessCatalogs for " + catalog["name"])
            raise

    def ProcessDatabase(self, databaseSettings):
        '''
        takes the database settings and tries to process them
        '''
        try:
            self.logger.debug(self.moduleName + " -- ProcessDatabase for " + databaseSettings["common"]["name"] + " starting")
            for catalog in databaseSettings["catalogs"]:
                if "execute" not in catalog or catalog["execute"] == 'Y':
                    self.ProcessCatalogs(databaseSettings["common"], catalog)
                else:
                    self.logger.debug(self.moduleName + " -- ProcessDatabase skip for " + catalog["name"])

            if "cleanlocal" in self.job and self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)              
            
            self.logger.debug(self.moduleName + " -- ProcessDatabase for " + databaseSettings["common"]["name"] + " finished")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessDatabase for " + databaseSettings["common"]["name"])
            raise

    def ProcessInput(self, logger, moduleName, filelocs):
        '''
        Bootstrap code that process all the databases, catalogs and tables
        '''
        currProcId = None
        try:
            self.logger.debug(self.moduleName + " -- " + "Starting...")
            if "tblEtl" in filelocs:
                currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)

            if "Databases" in self.job:
                for databaseSettings in self.job["Databases"]:
                    if databaseSettings["execute"] == 'Y':
                        self.ProcessDatabase(databaseSettings)
                    else:
                        self.logger.debug(self.moduleName + " -- Skipping database: " + databaseSettings["common"]["name"])
            elif "catalogs" in self.job:
                self.ProcessDatabase(self.job)
            elif "tables" in self.job:
                dbCommon = None
                if "common" in self.job:
                    dbCommon = self.job["common"]
                self.ProcessCatalogs(dbCommon, self.job)
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)                

            self.logger.debug(self.moduleName + " -- " + " finished.")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if "tblEtl" in filelocs and self.etlUtilities.CompleteInstance(\
                filelocs["tblEtl"]["table"], currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
