'''
BCP Utilties
Author - Varun Muriyanat
License: IHS - not to be used outside the company
'''

import platform
import datetime
import pypyodbc
from AACloudTools.OSUtilities import OSUtilities

class BCPUtilities(object):
    '''
    Utilities to run BCP jobs and dump data from SQL Server
    '''
    def __init__(self, logger, fileUtilities, awsParams, localTempDirectory):
        '''
        Constructor
        '''
        self.logger = logger
        self.fileUtilities = fileUtilities
        self.awsParams = awsParams
        self.localTempDirectory = localTempDirectory

    @staticmethod
    def GetFileToBeUploaded(outputFileName, charsToBeReplaced):
        '''
        Get list of files to be uploaded
        '''
        tablename = outputFileName
        if  charsToBeReplaced is not None:
            lenCharsToBeReplaced = len(charsToBeReplaced)
            if lenCharsToBeReplaced != 0:
#        if (charsToBeReplaced is not None) and (lenCharsToBeReplaced != 0):
                tablename = "scrubbed_" + outputFileName

        return tablename

    def GetFullFilePath(self, fileName):
        '''
        Construct the full path of the file
        '''
        return self.localTempDirectory + "/" + fileName

    # pylint: disable=too-many-arguments
    def RunBCPJob(self,
                  sqlServerloginInfo,
                  bcpUtilityDirOnLinux="/opt/mssql-tools/bin/",
                  inputQuery="",
                  outputFileName="",
                  fieldTerminator="|",
                  rowTerminator=None,
                  packetSize="4096"):
        '''
        Run the actual BCP job
        '''
        try:
            nullDev = " "
            # set the BCP commands appropriately to handle OS environments
            # Windows/Linux
            if platform.system().lower() == "linux":
                query = bcpUtilityDirOnLinux
                nullDev = " 1>/dev/null "
            elif platform.system().lower() == "windows":
                query = ""
                nullDev = " 1>NUL "
            else:
                self.logger.exception("OS other than Windows/Linux")

            query = query + "bcp \"" + inputQuery + "\" queryout " + outputFileName + " -c"

            #"rowTerm" is taken as default unless the key "chars_to_be_replaced" is declared in the config file
            if rowTerminator is not None:
                query = query + " -r \"" + rowTerminator + "\" "

            bcpErrorLog = " 2>>" + self.localTempDirectory + \
                "/" + str(datetime.date.today()) + "_bcperror.log "
            query = query + " -t \"" + fieldTerminator + "\" " + \
                sqlServerloginInfo  + " -a " + packetSize + nullDev + bcpErrorLog

            OSUtilities.RunCommandAndLogStdOutStdErr(query, self.logger)
        except:
            self.logger.exception(
                "we had an error in BCPUtilities in RunBCPJob")
            raise

    def BulkExtract(self, sqlPullDataScript, outputCSV, dbCommon, tables, fieldTerminator,
                    rowTerminator, bcpUtilityDirOnLinux, fileUtilities, logger):
        '''
        calls BCP module to pull data
        '''
        try:
            if "incrementalconditions" in tables:
                sourcetable = tables["incrementalconditions"]["sourcetable"]
            else:
                sourcetable = tables["sourcetable"]

            logger.debug("BCPUtilities -- " + "BulkExtract for " + sourcetable + " starting ")
            if "user" in dbCommon:
                loginInfo = "-S " + dbCommon["server"] +\
                            " -U " + dbCommon["user"] + " -P " + dbCommon["pwd"] +\
                            " -d " + dbCommon["name"]
            else:
                loginInfo = "-S " + dbCommon["server"] + " -d " + dbCommon["name"] + " -T"
            self.RunBCPJob(loginInfo, bcpUtilityDirOnLinux, sqlPullDataScript, outputCSV,
                           fieldTerminator, rowTerminator, packetSize='65535')
            logger.debug("BCPUtilities -- " + "BulkExtract for " + sourcetable + " finished ")
        except Exception as err:
            logger.exception("BCPUtilities -- we had an error in BulkExtract -- for " +\
                                  sourcetable + " error=" + err.message)
            raise

    @staticmethod
    def GetInnerFields(fieldSet):
        '''
        Get the inner set of fields for the SQL statements
        '''
        fields = ""
        for fldNdx, fld in enumerate(fieldSet):
            # Skip the field if it is for Athena management
            if "athenaOnly" in fld and fld["athenaOnly"] == "Y":
                continue

            if fldNdx > 0:
                fields = fields + "\n    ,"
            else:
                fields = fields + "    "

            if "validation" in fld:
                fields = fields + str(fld["validation"]) + " as "
            elif fld["type"] == 'BOOLEAN':
                # Parquet does not like 1/0 for Boolean.  It needs true/false/NULL or the record will be skipped
                fields = fields + "CASE WHEN " + str(fld["name"]) + " = 1 THEN 'true' WHEN " +\
                        str(fld["name"]) + " = 0 THEN 'false' ELSE NULL END AS "
            elif fld["type"] == 'VARCHAR':
                ###
                #  this is just because we do not handle easily if a field is just '' so
                #  we are adding the nullif statement as a default on varchar type of fields
                ###
                fields = fields + "NULLIF(" + str(fld["name"]) + ",'') as "

            fields = fields + str(fld["name"])
        return fields

    @staticmethod
    def ComponseRangeString(chunkStart, chunkEnd):
        '''
        generate a string with low and high numbers
        '''
        rangeString = "All"
        if chunkStart <> -1 and chunkEnd <> -1:
            rangeString = str(chunkStart) + "-" + str(chunkEnd)
        return rangeString

    @staticmethod
    def GetWhereClause(table, chunkStart, chunkEnd):
        '''
        if there is a where clause needed it returns it
        '''
        retVal = ''
        if "incrementalconditions" in table:
            incrementalConditions = table["incrementalconditions"]
            retVal = " where " + incrementalConditions["keyfield"] +\
                     " >= "  + str(chunkStart) +\
                     " and " +\
                     incrementalConditions["keyfield"] +\
                     " <= "  + str(chunkEnd)
        return retVal

    @staticmethod
    def FieldHasValidationFlag(fld):
        '''
        Check if the field is partitioned
        '''
        return "validation" in fld

    @staticmethod
    def TableHasValidationFlag(table):
        '''
        Check if the table is partitioned
        '''
        for fld in table["fields"]:
            if BCPUtilities.FieldHasValidationFlag(fld):
                return True
        return False


    @staticmethod
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals
    def CreatePullScript(dbCommon, table, chunkStart, chunkEnd, logger, fileUtilities, templateFolder):
        '''
        takes the template for the pull script and customizes it for the data we need
        based on the fields in the config file
        '''
        if not BCPUtilities.TableHasValidationFlag(table): # Simple case.  Just do a SELECT * because it is faster
            sqlPullDataScript = "SELECT * FROM " + table["sourcetable"]
        else:
            # Get all the fields by name.  Add the validation fixes for fields that have a validation flag
            sqlPullDataScript = "SELECT "
            index = 0
            for fld in table["fields"]:
                # Skip the field if it is for Athena management
                if "athenaOnly" in fld and fld["athenaOnly"] == "Y":
                    continue
                if index > 0:
                    sqlPullDataScript = sqlPullDataScript + ", "
                if BCPUtilities.FieldHasValidationFlag(fld):
                    sqlPullDataScript = sqlPullDataScript + fld["validation"] + " as "
                sqlPullDataScript = sqlPullDataScript + fld["name"]
                index = index + 1
            sqlPullDataScript = sqlPullDataScript + " FROM " + table["incrementalconditions"]["sourcetable"]

        whereClause = BCPUtilities.GetWhereClause(table, chunkStart, chunkEnd) # Blank for full data loads
        sqlPullDataScript = sqlPullDataScript + " " + whereClause

        return sqlPullDataScript

    @staticmethod
    def GetMaxValueSQLServer(dbCommon, tables, logger):
        '''
        This routine is used to pull the max key from the source table
        '''
        retVal = None
        try:
            pypyodbc.lowercase = False
            driver = dbCommon["driver"]
            server = dbCommon["server"]
            db = dbCommon["name"]
            connstr = 'DRIVER=%s;SERVER=%s;DATABASE=%s;Trusted_Connection=yes;' % (driver, server, db)
            conn = pypyodbc.connect(connstr)

            cursor = conn.cursor()
            sql = "SELECT MAX(" + tables["incrementalconditions"]["keyfield"] + ") FROM " + tables["incrementalconditions"]["sourcetable"]
            cursor.execute(sql)
            row = cursor.fetchone()
            while row:
                retVal = row[0]
                row = cursor.fetchone()

        except Exception as ex:
            conn.rollback()
            logger.exception("BCPUtilities - we had an error in GetMaxValueSQLServer")
            raise ex
        finally:
            cursor.close()
            conn.close()
        return retVal
