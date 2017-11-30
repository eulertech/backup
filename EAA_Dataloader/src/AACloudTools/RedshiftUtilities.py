'''
RedShift Utilties
Author - Christopher Lewis
License: IHS - not to be used outside the company
'''
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.OSUtilities import OSUtilities
import psycopg2

class RedshiftUtilities(object):
    '''
    Various Redshift utiliites - e.g. load data from S3
    '''

    def __init__(self):
        '''
        Basic initialization of instance variable
        '''

    @staticmethod
    def Connect(dbname, host, port, user, password):
        '''
        establish connection to redshift
        '''
        conn = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=password)
        return conn

    @staticmethod
    def LoadDataFromS3(rsConnect, s3, job, logger, isManifest='N'):
        '''
        Load file from s3 into a table in RedShift
        s3 - access key and secret key to access S3.  We are using the default load key
        job - Definition of the file to be loaded and the location of the table name etc.
        '''
        redshiftTableName = job['destinationSchema'] + "." + job['tableName']
        originalRecCount = RedshiftUtilities.GetRecordCount(rsConnect, redshiftTableName)

#        cur = rsConnect.cursor()
        ignoreheader = 0
        if "ignoreheader" in job:
            ignoreheader = job["ignoreheader"]
        
        command =  \
            "COPY " + redshiftTableName + " FROM '" + job['s3Filename'] + "'" + \
            " WITH CREDENTIALS AS 'aws_access_key_id=" + s3['access_key_id'] + \
            ";aws_secret_access_key=" + s3['secret_access_key'] + "'" + \
            " FORMAT AS DELIMITER AS '" + job['delimiter'] + "'" + \
            " " + job['fileFormat'] + " " +\
            " IGNOREHEADER " + str(ignoreheader) +\
            " NULL AS 'NULL' \
            EMPTYASNULL \
            DATEFORMAT AS '" + job['dateFormat'] + "' " + \
            "TIMEFORMAT AS 'auto' \
            ESCAPE \
            ACCEPTINVCHARS \
            TRIMBLANKS \
            MAXERROR AS 2"  # Allow only 1 error for the header

        if "quotes" in job:
            command = command
        else:
            command = command + ' REMOVEQUOTES '

        if isManifest == 'Y':
            command = command + ' ' + 'manifest'

        logger.info("  Table: " + redshiftTableName + " from file: " + job['s3Filename'] + ".  Please wait...")
        logger.debug(command)
#         print(command) # For debugging
#        cur.execute(command)
#        cur.close()
        with rsConnect:
            with rsConnect.cursor() as curs:
                curs.execute(command)
        # Make sure to commit or else the data won't be saved
        rsConnect.commit()

        newRecCount = RedshiftUtilities.GetRecordCount(rsConnect, redshiftTableName)
        recLoaded = newRecCount - originalRecCount
        logger.info("  Records loaded: " + str(recLoaded) + " into table: " + job['tableName'])
        return recLoaded

    @staticmethod
    # pylint: disable=too-many-arguments
    def LoadFileIntoRedshift(rsConnect, s3, logger, fileUtilities, localFilepath, destinationSchema,\
                             redshiftDestTable, fileFormat, dateFormat, delimiter, isManifest='N'):
        '''
        Load file from local drive to RedShift
        Zip the file, upload to S3 and then load into RedShift
        '''
        if isManifest == 'Y':
            zipLocalFilepath = localFilepath
        else:
            # Zip the file
            zipLocalFilepath = localFilepath + ".gz"
            fileUtilities.GzipFile(localFilepath, zipLocalFilepath)

        bucketName, s3TempKey = S3Utilities.UploadFileToS3Temp(s3, zipLocalFilepath)

        # Build the job definition file
        job = {}
        job["destinationSchema"] = destinationSchema
        job["tableName"] = redshiftDestTable
        job["s3Filename"] = S3Utilities.GetS3FileName(bucketName, s3TempKey)
        job["fileFormat"] = fileFormat
        job["dateFormat"] = dateFormat
        job["delimiter"] = delimiter

        RedshiftUtilities.LoadDataFromS3(rsConnect, s3, job, logger, isManifest)

        S3Utilities.DeleteFile(s3, bucketName, s3TempKey)

    @staticmethod
    def GetRecordCountFromSQL(rsConnect, sqlScript):
        '''
        Utility to get the record count from a script
        '''
        cur = rsConnect.cursor()
        try:
            cur.execute(sqlScript)
        except Exception as ex:
            rsConnect.rollback() # Rollback or else the connection will fail
            cur.close()
            raise ex

        data = cur.fetchall()
        cur.close()
        return data[0][0]

    @staticmethod
    def GetRecordCount(rsConnect, tableName):
        '''
        Utility to get the record count in a table
        '''
        sqlScript = "SELECT COUNT(*) FROM " + tableName
        return RedshiftUtilities.GetRecordCountFromSQL(rsConnect, sqlScript)

    @staticmethod
    def PSqlExecute(filepath, logger):
        '''
        Utility to allow us to run any SQL script.  Yes we can call execute, but execute has a limit on the buffer
        size.  PSqlExecute does not have that limit
        '''
        import os
        import sys

        hostName = os.environ["PGHOSTNAME"]
        dbName = os.environ["PGDBNAME"]
        port = os.environ["PGPORT"]
        command = "psql -q -v ON_ERROR_STOP=1 -h " + hostName + " -d " + dbName + " -p " + port + " -f " + filepath
        ret = OSUtilities.RunCommandAndLogStdOutStdErr(command, logger)
        if ret != 0:
            msg = "PSqlExecute script ERROR."
            logger.error(msg)
            sys.exit(msg)
        return ret

    @staticmethod
    def ExecuteSQLScript(rsConnect, fileName, logger):
        '''
        Alternative way of executing SQL by reading the file and then running execute.
        For large Scripts we had to use psql
        '''
        try:
            #  first get the DDL command from the fileName
            with open(fileName) as infile:
                ddl = infile.read()

            cur = rsConnect.cursor()
            cur.execute(ddl)
            rsConnect.commit()
            cur.close()
        except:
            logger.exception("we had an error in PSqlExecute")
            raise

    @staticmethod
    def UnloadDataToS3(rsConnect, s3, job, logger):
        '''
        Unloads a table to S3
        s3 - Access key and secret key to access S3.  We are using the default load key.
        job - Definition of the file to be loaded and the location of the table name etc.
        '''

        command = "UNLOAD('" + job['query'] + "')" + \
                    " TO '" + job['s3Folder'] + "'" + \
                    " WITH CREDENTIALS AS 'aws_access_key_id=" + s3['access_key_id'] + \
                    ";aws_secret_access_key=" + s3['secret_access_key'] + "'" + \
                    " ALLOWOVERWRITE"

        logger.info("  Unloading data to folder: " + job['s3Folder'] + ".  Please wait...")
        logger.debug(command)

        with rsConnect:
            with rsConnect.cursor() as curs:
                curs.execute(command)

        rsConnect.commit()