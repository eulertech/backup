'''
Athena Utilties
Author - Christopher Lewis
License: IHS - not to be used outside the company
'''

from subprocess import Popen, PIPE
from time import sleep
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities

class AthenaUtilities(object):
    '''
    Various Redshift utiliites - e.g. load data from S3
    '''

    def __init__(self):
        '''
        Basic initialization of instance variable
        '''
    @staticmethod
    def ComposeAthenaSchemaName(schemaName):
        '''
        Compose Athena Schema Name
        '''
        athenaSchemaName = "athena_" + schemaName
        return athenaSchemaName
        
    @staticmethod
    def ComposeAthenaS3BaseKey(schemaName, tableName):
        '''
        Compose location where the S3 data will be saved in the passive lake
        '''
        # Bucket names cannot have underscore
        s3BaseKey = "s3://ihs-lake-01-athena-" + schemaName.replace("_", "-") + "/" + tableName + "/"
        return s3BaseKey
    
    @staticmethod
    def ComposeAthenaS3DataFileKey(schemaName, tableName):
        '''
        Place the data in a "data" subfolder
        '''
        s3DataFileKey = AthenaUtilities.ComposeAthenaS3BaseKey(schemaName, tableName) + "data/"
        return s3DataFileKey
        
    @staticmethod
    def ComposeAthenaS3ScriptKey(schemaName, tableName):
        '''
        Place the scripts in the "scripts" subfolder.  If the data in RedShift is unloade,
        we will use these scripts to reload the data
        '''
        s3ScriptKey = AthenaUtilities.ComposeAthenaS3BaseKey(schemaName, tableName) + "scripts/"
        return s3ScriptKey
    
    @staticmethod
    def ComposeTemporaryOutputLocation():
        return "s3://ihs-temp/AthenaStaging/OutputLocation/" # Location for temporary output
    
    @staticmethod
    def SqlToDropAthenaTable(tableSettings, athenaSchemaName):
        '''
        Drop sql script
        '''
        sqlScript = "DROP TABLE IF EXISTS {}.{};".format(athenaSchemaName, tableSettings["table"])
        return sqlScript

    @staticmethod
    def SqlToCreateAthenaTable(tableSettings, athenaSchemaName, parquetFileLocationOnS3):
        '''
        Athena SQL creation script.  An external table is created that points to S3 parquet file
        '''
        sqlScript = "CREATE EXTERNAL TABLE IF NOT EXISTS {}.{} (".format(athenaSchemaName, tableSettings["table"])
        ndx = 0
        for fld in tableSettings["fields"]:
            if AthenaUtilities.IsFieldPartitioned(fld):
                # Skip fields that are partitioned.  They need to be handled separately
                continue
            
            if ndx > 0:
                sqlScript = sqlScript + ", "
            ndx = ndx + 1
            
            # Athena/Redshift uses lower case for column names.  Make sure the Parquet schema matches the case
            fieldName = fld["name"].lower()
            fieldType = fld["type"]
            if "size" in fld:
                fieldSize = fld["size"]

            # Data types for Athena - http://docs.aws.amazon.com/athena/latest/ug/ddl/create-table.html
            if fieldType == "DATE":
                # Athena does not support DATE with Parquet: http://docs.aws.amazon.com/athena/latest/ug/ddl/create-table.html
                # For now store as string
                fieldType = "VARCHAR"
                fieldSize = "20"
            if fieldType == "TIMESTAMP":
                # Athena does support timestamp but is adjusting the timestamp based on UTC - Sever time
                # So the time gets shifted by ~5/6 hours.  Spoke with AWS support.  So far no good option
                # For now store as string
                fieldType = "VARCHAR"
                fieldSize = "20"
            elif  fieldType == "INTEGER":
                # Athena using INT for INTEGER
                fieldType = "INT"
            elif  fieldType == "REAL" or fieldType == "FLOAT4":
                # Athena using DOUBLE for 32-bit floating point
                # http://docs.aws.amazon.com/athena/latest/ug/known-limitations.html
                fieldType = "DOUBLE"
            elif  fieldType == "FLOAT" or fieldType == "FLOAT8":
                # Athena using DOUBLE for 64-bit floating point
                fieldType = "DOUBLE"
            elif  fieldType == "DECIMAL":
                # Athena is not reading DECIMAL stored in Parquet
                fieldType = "DOUBLE"

            sqlScript = sqlScript + fieldName + " " + fieldType
            if fieldType == "VARCHAR":
                sqlScript = sqlScript + "(" + fieldSize + ")"
            elif fieldType == "IDENTITY":
                sqlScript = sqlScript + "(" + fieldSize + ")"
            # TODO: Enable the following once DECIMAL is supported in Parquet
            #elif fieldType == "DECIMAL":
            #    sqlScript = sqlScript + "(" + fieldSize + ")"
            
        sqlScript = sqlScript + ")"
        
        if AthenaUtilities.IsTablePartitioned(tableSettings):
            # We have a partitioned table.  Define field in partition
            sqlScript = sqlScript + AthenaUtilities.SqlForPartitionFields(tableSettings)
             
        sqlScript = sqlScript + " STORED AS PARQUET LOCATION '" + parquetFileLocationOnS3 + "'"
        return sqlScript

    @staticmethod
    def ContainsSpecialFields(tableSettings):
        '''
        Athena has limitation on data types.  Handle those as special cases
        '''
        for fld in tableSettings["fields"]:
            fieldType = fld["type"]
            if fieldType == "DATE":
                # Athena does not support DATE in Parquet.  So save as string and then convert
                # from string to date during insert into RedShift
                return True
            elif fieldType == "TIMESTAMP":
                # Athena does support timestamp but adjust the time by UTC+Server time.
                # Support to AWS support but not solution yet.  So save as string and then convert
                # from string to timestamp during insert into RedShift
                return True
            elif  fieldType == "REAL" or fieldType == "FLOAT4":
                # Athena does not support FLOAT4, only DOUBLE.  So save as DOUBLE and then convert
                # from DOUBLE to FLOAT4 during insert into RedShift
                return True
            elif "athenaOnly" in fld and fld["athenaOnly"]=="Y":
                return True
        
        return False
    
    @staticmethod
    def HandleSpecialFields(tableSettings):
        '''
        If we have a date field we need to handle as a special case since Athena/Parquet does not support Date
        '''
        sqlSubStatement = ""
        for fld in tableSettings["fields"]:
            # Skip the field if it is for Athena management
            if "athenaOnly" in fld and fld["athenaOnly"]=="Y":
                continue

            if sqlSubStatement: # Subsequent field
                sqlSubStatement = sqlSubStatement + "\n        ,"
            else: # First field
                sqlSubStatement = sqlSubStatement + "\n        "
                
            fieldName = fld["name"]
            fieldType = fld["type"]
            if fieldType == "DATE":
                sqlSubStatement = sqlSubStatement + "to_date(" + fieldName + ", 'YYYY-MM-DD')"
            if fieldType == "TIMESTAMP":
                sqlSubStatement = sqlSubStatement + "to_timestamp(" + fieldName + ", 'YYYY-MM-DD HH24:MI:SS')"
            elif  fieldType == "REAL" or fieldType == "FLOAT4":
                sqlSubStatement = sqlSubStatement + "CAST(" + fieldName + " AS REAL)"
            else:
                sqlSubStatement = sqlSubStatement + fieldName
        
        sqlSubStatement = sqlSubStatement + "\n"
        return sqlSubStatement
    
    @staticmethod
    def IsFieldPartitioned(fld):
        '''
        Check if the field is partitioned
        '''
        return "isPartitioned" in fld and fld["isPartitioned"]=="Y"
    
    @staticmethod
    def IsTablePartitioned(tableSettings):
        '''
        Check if the table is partitioned
        '''
        for fld in tableSettings["fields"]:
            if AthenaUtilities.IsFieldPartitioned(fld):
                return True
        return False

    @staticmethod
    def GetPartitionKey(tableSettings):
        '''
        Return the partition key.  Only one key is supported currently
        '''
        for fld in tableSettings["fields"]:
            if AthenaUtilities.IsFieldPartitioned(fld):
                return fld["name"].lower()
        return ""
    
    @staticmethod
    def SqlForPartitionFields(tableSettings):
        #    " PARTITIONED BY (last_updated DATE)"
        '''
        Construct the SQL for partitioned field.  ONLY ONE FIELD IS SUPPORTED FOR NOW
        '''
        sqlSubStatement = " PARTITIONED BY ("
        for fld in tableSettings["fields"]:
            if AthenaUtilities.IsFieldPartitioned(fld):
                fieldType = fld["type"]
                if  fieldType == "INTEGER":
                    # TODO Handle in a more general way. Athena using INT for INTEGER
                    fieldType = "INT"
                
                sqlSubStatement = sqlSubStatement + fld["name"] + " " + fieldType
                if "size" in fld:
                    sqlSubStatement = sqlSubStatement + "(" + fld["size"] + ")"
        sqlSubStatement = sqlSubStatement + ")"
        return sqlSubStatement
    
    @staticmethod
    def ComposeInsertIntoSqlFilename(tableSettings, fileLoc):
        '''
        Compose the file name for sql script
        '''
        if fileLoc.endswith('/') is False:
            fileLoc = fileLoc + '/'
        fname = fileLoc + "InsertInto_" + tableSettings["table"] + ".sql"
        return fname
        
    @staticmethod
    def SqlToLoadDataFromAthena(logger, tableSettings, fileLoc, partitionValue):  # pylint: disable=too-many-branches
        '''
        Create SQL Script to load data from Athena into the redshift table
        '''
        fname = AthenaUtilities.ComposeInsertIntoSqlFilename(tableSettings, fileLoc)
        outfile = open(fname, "w")
        
        athenaSchemaName = AthenaUtilities.ComposeAthenaSchemaName(tableSettings["schemaName"])
        schemaDotTableName = athenaSchemaName + '.' + tableSettings["table"]
        try:
            # Build the INSERT statement
            outLine = "INSERT INTO " + tableSettings["schemaName"] + '.' + tableSettings["table"] + "\n    SELECT"
            
            # If we have a date field we need to handle as a special case since Athena/Parquet does not support Date
            if AthenaUtilities.ContainsSpecialFields(tableSettings):
                outLine = outLine + AthenaUtilities.HandleSpecialFields(tableSettings)
            else:
                outLine = outLine + " * " # Perform a straight copy
            
            outLine = outLine + "    FROM " + schemaDotTableName
            
            # Add where clause if a partition value is specified.  Should only be the case with paritioned tables
            if AthenaUtilities.IsTablePartitioned(tableSettings):
                if partitionValue:
                    # Copy data smartly rather than copying everything
                    partitionKey = AthenaUtilities.GetPartitionKey(tableSettings)
                    outLine = outLine + "\n    WHERE " + partitionKey + " = '" + partitionValue + "'"
            
            outLine = outLine + ";"
            outLine = FileUtilities.PutLine(outLine, outfile)
        except:   # pylint: disable=bare-except
            logger.exception("problem creating table SQL for " + schemaDotTableName)
        finally:
            outfile.close()
        return fname

    @staticmethod
    def UploadDataFilesToDesignatedS3Location(localParquetFilepath, tableSettings, partitionValue):
        '''
        Upload the data files, typically Parquet files, to the designated S3 location
        '''
        s3FolderLocation = AthenaUtilities.ComposeAthenaS3DataFileKey(tableSettings["schemaName"], tableSettings["table"])

        partitionKeyValueFolder = ""
        if AthenaUtilities.IsTablePartitioned(tableSettings):
            partitionKey = AthenaUtilities.GetPartitionKey(tableSettings)
            if not partitionKey:
                raise ValueError('Partition key cannot be null for partitioned tables.')
            if not partitionValue:
                raise ValueError('Partition value cannot be null for partitioned tables.')
            partitionKeyValueFolder = partitionKey + "=" + partitionValue + "/"

        s3FolderLocationData = s3FolderLocation + partitionKeyValueFolder
        
        # Only delete specific partition
        # There is no simple option to delete the whole S3 folder.  It would be easy to make a mistake and delete the entire
        # data set in the passive lake.  Do the FULL deletion MANUALLY
        if tableSettings["new"] == "Y" or ("clearPartition" in tableSettings and tableSettings["clearPartition"] == "Y"):
            S3Utilities.DeleteFileFromS3TempUsingAWSCLi(s3FolderLocationData, "--recursive")
        
        # Only copy the *.parquet files
        S3Utilities.S3RecursvieCopy(localParquetFilepath, s3FolderLocationData, "--exclude \"*\" --include \"*.parquet\" ")
        
        #=======================================================================
        # For testing purposes - Copy the file to a holding directory
        # import glob
        # import shutil
        # dst = "/s3/" + tableSettings["table"]
        # src = localParquetFilepath + "*.parquet"
        # for fileName in glob.glob(src):
        #     print(fileName)
        #     shutil.move(fileName, dst)
        #=======================================================================
        
        return s3FolderLocation

    @staticmethod
    def UploadScriptsToDesignatedS3Location(localScriptsFilepath, tableSettings):
        '''
        Upload the script files, typically table creation and upload, to the designated S3 location
        '''
        s3FolderLocation = AthenaUtilities.ComposeAthenaS3ScriptKey(tableSettings["schemaName"], tableSettings["table"])
        S3Utilities.DeleteFileFromS3TempUsingAWSCLi(s3FolderLocation, "--recursive")
        
        # Upload only scripts that we plan to keep for later reuse
        scriptToCreateRedshift = FileUtilities.ComposeCreateTableSqlFilename(tableSettings, localScriptsFilepath)
        scriptToInsertIntoRedshift = AthenaUtilities.ComposeInsertIntoSqlFilename(tableSettings, localScriptsFilepath)
        
        S3Utilities.S3Copy(scriptToCreateRedshift, s3FolderLocation)
        S3Utilities.S3Copy(scriptToInsertIntoRedshift, s3FolderLocation)
        return s3FolderLocation

    @staticmethod
    def DownloadScriptsFromDesignatedS3Location(tableSettings, localScriptsFilepath):
        '''
        Download the script files, typically table creation and upload, from the designated S3 location
        '''
        s3FolderLocation = AthenaUtilities.ComposeAthenaS3ScriptKey(tableSettings["schemaName"], tableSettings["table"])
        S3Utilities.S3RecursvieCopy(s3FolderLocation, localScriptsFilepath)
        return s3FolderLocation

    @staticmethod
    def WaitForQueryToFinish(queryId, logger):
        output = ""
        queryRunning = True
        while  queryRunning:
            command = "aws athena get-query-execution --query-execution-id " + queryId + " --output text --region us-west-2 "
            process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
            (output, error) = process.communicate()
            if error:
                logger.exception(error)
                raise
            
            process.wait()
            sleep(0.1)
            queryRunning = 'RUNNING' in output
            
        if not 'SUCCEEDED' in output:
            raise ValueError("No results from query: " + output)
    
    @staticmethod
    def GetQueryValue(queryId, logger):
        command = "aws athena get-query-results --query-execution-id " + queryId + " --output text --region us-west-2 "
        process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        (output, error) = process.communicate()
        if error:
            logger.exception(error)
            raise
        
        process.wait()
        output = output.rstrip('\n\r')
        if not output:
            raise ValueError("Query returned nothing")
        value = output.split('\t')[-1]
        return value
    
    @staticmethod
    def ExecuteAthenaQuery(sqlScript, athenaSchemaName, s3OutputLocation, logger, waitForQueryToFinish=True):
        command = "aws athena start-query-execution --query-string \"" + sqlScript + "\" --query-execution-context Database='" + athenaSchemaName + "' --result-configuration OutputLocation=\'" + s3OutputLocation + "' --output text --region us-west-2"
        logger.info(command)
        process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        (output, error) = process.communicate()
        if error:
            logger.exception(error)
            raise
        
        process.wait()
        queryId = output.rstrip('\n\r')
        if waitForQueryToFinish:
            AthenaUtilities.WaitForQueryToFinish(queryId, logger)
        return queryId

    @staticmethod
    def GetMaxValue(awsParams, athenaSchemaName, tableName, columnName, logger):
        # Need the proper credentials to write to the Athena lake
        old_key, old_secret_key = awsParams.SwitchS3CredentialsToAthena()
        
        sqlScript = "select max(" + columnName + ") as max_val from " + athenaSchemaName + "." + tableName
        s3OutputLocation = AthenaUtilities.ComposeTemporaryOutputLocation()
        queryId = AthenaUtilities.ExecuteAthenaQuery(sqlScript, athenaSchemaName, s3OutputLocation, logger)
        maxValue = AthenaUtilities.GetQueryValue(queryId, logger)
        if maxValue == "max_val": # If table exists but there is no S3 data
            maxValue = None
        logger.info("MaxValue in Athena Table: " + str(maxValue))
        
        awsParams.SwitchS3CredentialsTo(old_key, old_secret_key)

        return maxValue
        
    @staticmethod
    def CreateAthenaTablesUsingAthenaCLI(tableSettings, parquetFileLocationOnS3, partitionValue, logger):
        '''
        Create Athena Tables
        '''
        athenaSchemaName = AthenaUtilities.ComposeAthenaSchemaName(tableSettings["schemaName"])
        
        s3OutputLocation = AthenaUtilities.ComposeTemporaryOutputLocation()
        if tableSettings["new"] == "Y":
            sqlScript = AthenaUtilities.SqlToDropAthenaTable(tableSettings, athenaSchemaName)
            AthenaUtilities.ExecuteAthenaQuery(sqlScript, athenaSchemaName, s3OutputLocation, logger)
        
        sqlScript = AthenaUtilities.SqlToCreateAthenaTable(tableSettings, athenaSchemaName, parquetFileLocationOnS3)
        AthenaUtilities.ExecuteAthenaQuery(sqlScript, athenaSchemaName, s3OutputLocation, logger)
        
        if AthenaUtilities.IsTablePartitioned(tableSettings):
            partitionKey = AthenaUtilities.GetPartitionKey(tableSettings)
            # Update the metadata
            # Do not use MSCK REPAIR TABLE.  Sometime the S3 subfolder is not synced so MSCK does not add the partition
            #sqlScript = "MSCK REPAIR TABLE " + athenaSchemaName + "." + tableSettings["table"]
            sqlScript = "ALTER TABLE " + athenaSchemaName + "." + tableSettings["table"] + " ADD IF NOT EXISTS PARTITION (" + partitionKey + " = '" + partitionValue + "')"
            AthenaUtilities.ExecuteAthenaQuery(sqlScript, athenaSchemaName, s3OutputLocation, logger)
    
    @staticmethod
    def UploadFilesAndCreateAthenaTables(awsParams, localParquetFilepath, tableSettings, localScriptsFilepath, logger, partitionValue):
        '''
        Upload file to Designated S3 Athena passive lake location and create Athena tables
        Do this using Athena credentials
        '''
        # Need the proper credentials to write to the Athena lake
        old_key, old_secret_key = awsParams.SwitchS3CredentialsToAthena()
        
        # For partitioned tables, the creation scripts in S3 will be build once to insert ALL the data from Athena to Redshift
        # Incremental runs will not update the S3 scripts since they are designed to incrementally update the RedShift tables
        updateScriptsInS3 = True
        if AthenaUtilities.IsTablePartitioned(tableSettings):
            s3FolderLocation = AthenaUtilities.ComposeAthenaS3DataFileKey(tableSettings["schemaName"], tableSettings["table"])
            updateScriptsInS3 = not S3Utilities.KeyExist(awsParams, s3FolderLocation) # Do not update scripts if data has been previously loaded
        
        # Save  the Parquet file(s) in the designated S3 location and create the corresponding Athena tables
        s3FolderLocation = AthenaUtilities.UploadDataFilesToDesignatedS3Location(localParquetFilepath, tableSettings, partitionValue)
        AthenaUtilities.CreateAthenaTablesUsingAthenaCLI(tableSettings, s3FolderLocation, partitionValue, logger)
        
        # Save  the SQL Script files in the designated S3 location in case we need to delete the data from RedShift to save space
        # The scripts in S3 will reload ALL the data to make sure the table is fully re-built
        if updateScriptsInS3:
            AthenaUtilities.UploadScriptsToDesignatedS3Location(localScriptsFilepath, tableSettings)
        
        logger.info("AthenaUtilities -- " + "Done uploading data to S3:" + s3FolderLocation)

        awsParams.SwitchS3CredentialsTo(old_key, old_secret_key)

    @staticmethod
    def DownloadScriptsForRedShift(awsParams, tableSettings, localScriptsFilepath):
        '''
        Download the script files, typically table creation and upload, from the designated S3 location
        '''
        # Need the proper credentials to write to the Athena lake
        old_key, old_secret_key = awsParams.SwitchS3CredentialsToAthena()
        
        s3FolderLocation = AthenaUtilities.ComposeAthenaS3ScriptKey(tableSettings["schemaName"], tableSettings["table"])
        S3Utilities.S3RecursvieCopy(s3FolderLocation, localScriptsFilepath)
        
        awsParams.SwitchS3CredentialsTo(old_key, old_secret_key)
        
        return s3FolderLocation

    # The function only works on Linux.  Windows FAILS to connect even though the driver installs
    # using PIP (after VisualStudio 2015 Express is installed)
    #===========================================================================
    # @staticmethod
    # def CreateAthenaTablesUsingPyAthenaJDBC(tableSettings, parquetFileLocationOnS3):
    #     '''
    #     Create Athena Tables
    #     '''
    #     from pyathenajdbc import connect
    #     from pandas.io import sql
    #     
    #     my_access_key='AKIAIVXFAVTORRNTBGTA'
    #     my_secret_key='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'
    #     s3OutputLocation = AthenaUtilities.ComposeTemporaryOutputLocation()
    #     
    #     conn = connect(access_key=my_access_key,
    #                    secret_key=my_secret_key,
    #                    s3_staging_dir=s3OutputLocation,
    #                    region_name='us-west-2')
    #     
    #     if tableSettings["new"] == "Y":
    #         sqlScript = AthenaUtilities.SqlToDropAthenaTable(tableSettings)
    #         sql.execute(sqlScript, conn)
    #     
    #     sqlScript = AthenaUtilities.SqlToCreateAthenaTable(tableSettings, parquetFileLocationOnS3)
    #     sql.execute(sqlScript, conn)
    #     conn.close()
    #===========================================================================
