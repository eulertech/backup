'''
Created on Aug 17, 2017

@author: VIU53188
@summary: Pulls Maritime data and loads into S3.
'''

import os
import json
import datetime
import re
import pypyodbc

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase

class Maritime(ApplicationBase):
    '''
    Class used to pull Maritime data.
    '''

    def __init__(self):
        '''
        Initial settings
        '''
        super(Maritime, self).__init__()
        self.cBlank = ''
        self.fromDate = None
        self.paramsList = {}
        self.fromKey = None
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def GetFromDate(self):
        '''
        using the currDate passed in we need to calculate the next date to use
        '''
        try:
            currDate = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), '%Y-%m-%d')
            toDate = datetime.datetime.strptime(currDate, '%Y-%m-%d')
            retDate = toDate.strftime('%Y-%m-%d')
            return retDate
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetFromDate")
            raise

    def GetParamsList(self, tblEtl):
        '''
        get the parameter list if one was stored
        '''
        paramsList = []
        try:
            lastRunRecJson = self.etlUtilities.GetLastGoodRun(tblEtl, self.moduleName)
            if lastRunRecJson is not None:
                if lastRunRecJson["params"] is not None:
                    paramsList = json.loads(lastRunRecJson["params"])
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetParamsList")
            raise
        return paramsList

    def BulkExtract(self, sqlPullDataScript, outputCSV, dbCommon, tables):
        '''
        calls BCP module to pull data
        '''
        try:
            sourcetable = tables["sourcetable"]
            self.logger.debug(self.moduleName + " -- " + "BulkExtract for " + sourcetable + " starting ")
            fldDelimiter = self.job["delimiter"]
            if "delimiter" in tables:
                fldDelimiter = tables["delimiter"]
            self.bcpUtilities.RunBCPJob(dbCommon["mssqlLoginInfo"],
                                        self.job["bcpUtilityDirOnLinux"],
                                        self.fileUtilities.LoadSQLQuery(sqlPullDataScript),
                                        outputCSV,
                                        fldDelimiter,
                                        packetSize='65535')
            self.logger.debug(self.moduleName + " -- " + "BulkExtract for " + sourcetable + " finished ")
        except Exception as err:
            self.logger.exception(self.moduleName + " - we had an error in BulkExtract -- for " +\
                                  sourcetable + " error=" + err.message)
            raise

    def BulkUploadToS3(self, s3subfolder):
        '''
        Uploads all GZIP files created into S3 to be uploaded later...
        '''
        self.logger.info(self.moduleName + " - Uploading GZIP files to s3 folder..." + self.fromDate)
        s3Location = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] + "/" +\
                    self.fromDate + "/" + s3subfolder
        S3Utilities.CopyItemsAWSCli(self.fileUtilities.gzipFolder,
                                    s3Location,
                                    "--recursive --quiet")

    def LoadData(self, tblJson):
        '''
        load the data from s3 into RedShift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " starting ")
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            s3folder = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] + "/" +\
                        self.fromDate + "/" + tblJson["s3subfolder"]

            fldDelimiter = self.job["delimiter"]
            if "delimiter" in tblJson:
                fldDelimiter = tblJson["delimiter"]

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": tblJson["schemaName"],
                                                 "tableName": tblJson["table"],
                                                 "s3Filename": s3folder,
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": fldDelimiter,
                                                 "quotes": 'N'
                                             },
                                             self.logger, "N")
            self.logger.info(self.moduleName + " - Finished loading s3 data to " +\
                             tblJson["schemaName"] + '.' +  tblJson["table"])
            rsConnect.close()
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in LoadData")
            raise

    def GetMaxValueRedShift(self, tables):
        '''
        gets the maximum keyed values
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "GetMaxValueRedShift" + " starting ")
            sql = '''select max({}) as lastid
                        from {}.{}
            '''
            incrementalConditions = tables["incrementalconditions"]
            sql = sql.format(tables["distkey"],
                             incrementalConditions["sourceSchema"],
                             incrementalConditions["sourcetable"])
            if "maxcondition" in incrementalConditions:
                sql = sql + " where " +  incrementalConditions["maxcondition"]

            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)
            cur = rsConnect.cursor()
            cur.execute(sql)
            tretVal = [dict((cur.description[i][0], value) \
               for i, value in enumerate(row)) for row in cur.fetchall()]
            cur.close()
            lentretVal = len(tretVal)
            if lentretVal > 0:
                retVal = tretVal[0]
            self.logger.debug(self.moduleName + " -- " + "GetMaxValueRedShift" + " finished ")
        except Exception as ex:
            rsConnect.rollback()
            self.logger.exception(self.moduleName + " - we had an error in GetMaxValueRedShift")
            raise ex
        finally:
            cur.close()
            rsConnect.close()
        return retVal

    def GetMaxValueSQLServer(self, dbCommon, tables, currValue):
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
            sql = 'select max(%s) from %s where %s >= %s' % (tables["incrementalconditions"]["keyfield"],
                                                             tables["incrementalconditions"]["sourcetable"],
                                                             tables["incrementalconditions"]["keyfield"],
                                                             currValue)
            cursor.execute(sql)
            row = cursor.fetchone()
            while row:
                retVal = row[0]
                row = cursor.fetchone()

        except Exception as ex:
            conn.rollback()
            self.logger.exception(self.moduleName + " - we had an error in GetMaxValueSQLServer")
            raise ex
        finally:
            cursor.close()
            conn.close()
        return retVal

    def GetWhereClause(self, table, currVal):
        '''
        if there is a where clause needed it returns it
        '''
        retVal = ''
        if "incrementalconditions" in table:
            incrementalConditions = table["incrementalconditions"]
            retVal = " where " + incrementalConditions["keyfield"] +\
                     " >= "  + str(currVal) +\
                     " and " +\
                     incrementalConditions["keyfield"] +\
                     " <= "  + str(self.fromKey)
        return retVal

    @staticmethod
    def GetInnerFields(fieldSet):
        '''
        Get the inner set of fields for the SQL statements
        '''
        fields = ""
        for fldNdx, fld in enumerate(fieldSet):
            if fldNdx % 5 == 0:
                fields = fields + '\n'
            if fldNdx > 0:
                fields = fields + ", "
            if "validation" in fld:
                fields = fields + str(fld["validation"]) + " as "
            elif fld["type"] == 'VARCHAR':
                ###
                #  this is just because we do not handle easily if a field is just '' so
                #  we are adding the nullif statement as a default on varchar type of fields
                ###
                fields = fields + " NULLIF(" + str(fld["name"]) + ",'') as "

            fields = fields + str(fld["name"])
        return fields
# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
    def CreatePullScript(self, dbCommon, table, currVal, incVal, mxValue):
        '''
        takes the template for the pull script and customizes it for the data we need
        based on the fields in the config file
        '''
        sqlPullDataScript = None
        try:
            self.logger.debug(self.moduleName + " -- " + "CreatePullScript for " + table["table"] + " starting ")

            sqlTemplate = dbCommon["sqlPullDataScriptTemplate"]
            if "pullTemplate" in table:
                templateType = table["pullTemplate"]
                sqlTemplate = dbCommon[str(templateType)]
            sqlPullDataTemplate = self.location + '/sql/' + sqlTemplate
            ###
            #  fix name of output script
            ###
            outName = re.sub('Template.sql$', '.sql', sqlTemplate)
            outName = re.sub(dbCommon["name"], table["table"], outName)
            sqlPullDataScript = self.localTempDirectory + "/sql/" + str(currVal) + "_" +  outName
            FileUtilities.RemoveFileIfItExists(sqlPullDataScript)

            fields = self.GetInnerFields(table["fields"])

            self.fromKey = currVal + incVal
            if self.fromKey > mxValue:
                self.fromKey = mxValue
            whereClause = self.GetWhereClause(table, currVal)

            with open(sqlPullDataTemplate) as infile, open(sqlPullDataScript, 'w') as outfile:
                for line in infile:
                    line = line.replace('{intable}', table["sourcetable"])
                    line = line.replace('{infields}', fields)
                    line = line.replace('{whereclause}', whereClause)
                    outfile.write(line)
            self.logger.debug(self.moduleName + " -- " + "CreatePullScript" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in CreatePullScript")
            raise
        return sqlPullDataScript

    def ProcessTables(self, dbCommon, tables):
        '''
        Process the current table to load it up
        '''
        try:
            self.logger.debug(self.moduleName + " -- ProcessTables for  " + tables["table"] + " starting")
            self.fromKey = 0
            mxValue = 0
            currVal = 0
            incVal = 0
            if "incrementalconditions" in tables:
                incrementalConditions = tables["incrementalconditions"]
                if "startID" in incrementalConditions:
                    self.fromKey = incrementalConditions["startID"]
                    currVal = self.fromKey
                else:
                    rsIDJson = self.GetMaxValueRedShift(tables)
                    if rsIDJson["lastid"] is not None:
                        self.fromKey = rsIDJson["lastid"]
                        currVal = self.fromKey
                if "endID" in incrementalConditions:
                    mxValue = incrementalConditions["endID"]
                else:
                    mxValue = self.GetMaxValueSQLServer(dbCommon, tables, currVal)
                incVal = tables["incrementalconditions"]["chunksize"]
            while currVal <= mxValue:
                ###
                #  create script to pull data
                ###
                sqlPullDataScript = self.CreatePullScript(dbCommon, tables, currVal, incVal, mxValue)
                ###
                #  pull data using script
                ###
                dateBatch = self.fromDate + "_" + str(currVal) + "_"
                outputCSV = self.fileUtilities.csvFolder + dateBatch + tables["s3subfolder"] + ".CSV"
                outputGZ = self.fileUtilities.gzipFolder + dateBatch + tables["s3subfolder"] + '.csv.gz'
                self.BulkExtract(sqlPullDataScript, outputCSV, dbCommon, tables)
                self.fileUtilities.GzipFile(outputCSV, outputGZ)
                self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)
                ###
                ##  load to S3
                ###
                self.BulkUploadToS3(tables["s3subfolder"])
                self.fileUtilities.EmptyFolderContents(self.fileUtilities.gzipFolder)
                if incVal == 0:
                    currVal = mxValue + incVal + 1
                else:
                    currVal = currVal + incVal
            ###
            #  load from S3 to Redshift
            ###
            self.LoadData(tables)
            self.logger.debug(self.moduleName + " -- ProcessTables for  " + tables["table"] + " finished")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessDatabase for " + tables["name"])
            raise

    def BuildReplaceString(self, fldName, spcSet, numConv):
        '''
        Routine to create replacement string per field
        '''
        rtnValue = ''
        try:
            if numConv == 0:
                rtnValue = fldName
                return rtnValue

            if isinstance(spcSet, dict):
                tempSet = spcSet
            else:
                tempSet = spcSet[numConv-1]
            fromValue = tempSet["symbol"]
            toValue = tempSet["value"]
            innerValue = "Replace(" + fldName + "," +\
                        "'" + fromValue + "'," +\
                        "'" + toValue + "')"
            numConv = numConv - 1

            rtnValue = self.BuildReplaceString(innerValue, spcSet, numConv)
        except:
            self.logger.exception(self.moduleName + "- we had an error in BuildReplaceString ")
            raise
        return rtnValue

    def CreateUpdSpcCharScript(self, dbCommon, tblJson):
        '''
        takes the template for the Update sqlscript and customizes it
        '''
        specialCharacterScript = None
        try:
            self.logger.debug(self.moduleName + " -- " + "CreateUpdSpcCharScript " + " starting ")

            specialCharacterScriptTemplate = self.location + '/sql/' + dbCommon[tblJson["specialCharacterScript"]]

            outName = re.sub('Template.sql$', '.sql', dbCommon[tblJson["specialCharacterScript"]])
            outName = re.sub("TableName", tblJson["table"], outName)
            specialCharacterScript = self.localTempDirectory + "/sql/" + outName

            FileUtilities.RemoveFileIfItExists(specialCharacterScript)

            fields = "set "
            cmaNdx = 0
            for fldDesc in tblJson["fields"]:
                if "specialcharacters" in fldDesc:
                    fldName = fldDesc["name"]
                    numConversion = len(fldDesc["specialcharacters"])
                    repString = self.BuildReplaceString(fldName, fldDesc["specialcharacters"], numConversion)
                    if cmaNdx > 0:
                        fields = fields + ", "
                    cmaNdx = 1
                    fields = fields + fldName + " = " + str(repString)

            with open(specialCharacterScriptTemplate) as infile, open(specialCharacterScript, 'w') as outfile:
                for line in infile:
                    line = line.replace('{schemaname}', tblJson["schemaName"])
                    line = line.replace('{workingtable}', tblJson["workingtable"])
                    line = line.replace('{desttable}', tblJson["table"])
                    line = line.replace('{fieldnames}', fields)
                    outfile.write(line)
            self.logger.debug(self.moduleName + " -- " + "CreateUpdSpcCharScript " + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in CreateUpdSpcCharScript")
            raise
        return specialCharacterScript

    def ResetSpecialChars(self, dbCommon, tblJson):
        '''
        Routine to create and run the sql to create version in RedShift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "ResetSpecialChars for " + tblJson["table"] + " starting ")
            verScript = self.CreateUpdSpcCharScript(dbCommon, tblJson)
            self.logger.info(self.moduleName + " - script file name = " + verScript)
            RedshiftUtilities.PSqlExecute(verScript, self.logger)
            self.logger.debug(self.moduleName + " -- " + "ResetSpecialChars for " + tblJson["table"] + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in UpdateTable")
            raise

    def CreateUpdateScript(self, dbCommon, tblJson):
        '''
        takes the template for the Update sqlscript and customizes it
        '''
        sqlUpdateScript = None
        try:
            self.logger.debug(self.moduleName + " -- " + "UpDate Table Script" + " starting ")
            sqlUpdateTemplate = self.location + '/sql/' + dbCommon["sqlUpdateScript"]

            outName = re.sub('Template.sql$', tblJson["table"] + '.sql', dbCommon["sqlUpdateScript"])

            sqlUpdateScript = self.localTempDirectory + "/sql/"  + outName
            FileUtilities.RemoveFileIfItExists(sqlUpdateScript)

            with open(sqlUpdateTemplate) as infile, open(sqlUpdateScript, 'w') as outfile:
                for line in infile:
                    line = line.replace('{destschemaname}', tblJson["schemaName"])
                    line = line.replace('{workingschemaname}', tblJson["updateSection"]["workingschemaname"])
                    line = line.replace('{workingtable}', tblJson["updateSection"]["workingtable"])
                    line = line.replace('{desttable}', tblJson["table"])
                    line = line.replace('{keys}', tblJson["updateSection"]["keyfields"])
                    line = line.replace('{join}', tblJson["updateSection"]["join"])

                    outfile.write(line)
            self.logger.debug(self.moduleName + " -- " + "UpDate Table Script" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in UpDate Table Script")
            raise
        return sqlUpdateScript

    def ProcessCatalogs(self, dbCommon, catalog):
        '''
        pulls data from each table in the catalog
        '''
        try:
            self.logger.debug(self.moduleName + " -- ProcessCatalogs for  " + catalog["name"] + " starting")
            for tables in catalog["tables"]:
                fname = self.fileUtilities.CreateTableSql(tables, self.fileUtilities.sqlFolder)
                RedshiftUtilities.PSqlExecute(fname, self.logger)

            for tables in catalog["tables"]:
                if tables["type"] == 'working':
                    self.ProcessTables(dbCommon, tables)
###
#  now go back and update any tables that have special characters in them
###
            for tables in catalog["tables"]:
                if "specialCharacterScript" in tables:
                    self.ResetSpecialChars(dbCommon, tables)

            for tables in catalog["tables"]:
                if "updateSection" in tables:
                    updScriptName = self.CreateUpdateScript(dbCommon, tables)
                    RedshiftUtilities.PSqlExecute(updScriptName, self.logger)

            self.logger.debug(self.moduleName + " -- ProcessCatalogs for  " + catalog["name"] + " finished")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessCatalogs for " + catalog["name"])
            raise

    def ProcessDatabase(self, databaseSettings):
        '''
        takes the database settings and tries to process them
        '''
        try:
            self.logger.debug(self.moduleName + " -- ProcessDatabase for  " + databaseSettings["common"]["name"] + " starting")
            for catalog in databaseSettings["catalogs"]:
                if catalog["execute"] == 'Y':
                    self.ProcessCatalogs(databaseSettings["common"], catalog)
                else:
                    self.logger.debug(self.moduleName + " -- ProcessDatabase skip " + catalog["name"])
            self.logger.debug(self.moduleName + " -- ProcessDatabase for  " + databaseSettings["common"]["name"] + " finished")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessDatabase for " + databaseSettings["common"]["name"])
            raise

    def Start(self, logger, moduleName, filelocs):
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)

            self.paramsList = self.GetParamsList(filelocs["tblEtl"]["table"])
###
#  set up to run create folder
###
            self.fileUtilities.moduleName = self.moduleName
            self.fileUtilities.localBaseDirectory = self.localTempDirectory
            self.fileUtilities.CreateFolders(self.job["folders"])
###
            self.fromDate = self.GetFromDate()
            for databaseSettings in self.job["Databases"]:
                if databaseSettings["execute"] == 'Y':
                    self.ProcessDatabase(databaseSettings)
                else:
                    self.logger.debug(self.moduleName + " -- skip database " + databaseSettings["common"]["name"])

            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
