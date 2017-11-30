'''
Created on Aug 11, 2017

@author: VIU53188
@summary: We use this application to older set of records into a set of
            tables defined by a json nugget passed in the commonParams[cat]
            object
            description of nugget defined below
            There is one issue and that is since we do not have the config file to use we have hard coded then
            name of the template to use.
            PopulateHistoryTemplate.sql
'''
from __future__ import unicode_literals
import os
import re

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities

class GEForecastHistory(object):
    '''
    look at summary note for description
    '''
    def __init__(self):
        '''
        Initial settings
                    commonParams = {}
                    commonParams["cat"] = cat { see below }
                    commonParams["moduleName"] = self.moduleName
                    commonParams["loggerParams"] = "log"
                    commonParams["sqlFolder"] = self.sqlFolder

        cat is a json nugget that defines the tables required
             cat = {
                "name": "giif",
                "tables": [
                {
                    "schemaName": "eaa_dev",
                    "srctable": "GEForcast_giif_attributes_working",
                    "table": "GEForcastHistory_giif_attributes",
                    "new": "Y",
                    "type": "attributes",
                    "partition": {
                        "over": "mnemonic, frequencychar",
                        "order": "publisheddate"
                    },
                    "fields": [
                        { "name": "object_id", "type": "VARCHAR", "size": "30" },
                        { "name": "name", "type": "VARCHAR", "size": "100" },
                        { "name": "mnemonic", "type": "VARCHAR", "size": "100" },
                        { "name": "frequencyChar", "type": "VARCHAR", "size": "2" },
                        { "name": "geo", "type": "VARCHAR", "size": "20" },
                        { "name": "startDate", "type": "Date"},
                        { "name": "endDate", "type": "Date"},
                        { "name": "updatedDate", "type": "Date"},
                        { "name": "publishedDate", "type": "Date"},
                        { "name": "longLabel", "type": "VARCHAR", "size": "2000" },
                        { "name": "dataEdge", "type": "VARCHAR", "size": "100" }
                    ],
                    "sortkey":"object_id"
                },
                {
                    "schemaName": "eaa_dev",
                    "srctable": "GEForcast_giif_data_working",
                    "table": "GEForcastHistory_giif_data",
                    "new": "Y",
                    "type": "series",
                    "fields": [
                        { "name": "object_id", "type": "VARCHAR", "size": "30" },
                        { "name": "date", "type": "DATE" },
                        { "name": "value", "type": "FLOAT8" }
                    ],
                    "sortkey":"object_id, date"
                }]
            }
        '''
        self.commonParams = {}
        self.fileUtilities = None
        self.logger = None
        self.moduleName = None
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def CreateMigrationScript(self):
        '''
        takes the template for the Update sqlscript and customizes it
        '''
        sqlMigrateScript = None
        try:
###
#  make sure that we have a place to put the sql script
###
            sqlScript = 'PopulateHistoryTemplate.sql'
            self.logger.debug(self.moduleName + " -- " + "CreateMigrationScript" + " starting ")
            sqlMigrateTemplate = self.location + '/sql/' + sqlScript
            sqlMigrateScript = self.commonParams["sqlFolder"] + re.sub('Template.sql$', '.sql', sqlScript)
#commonParams["cat"]
            FileUtilities.CreateFolder(self.commonParams["sqlFolder"])
            FileUtilities.RemoveFileIfItExists(sqlMigrateScript)
            ###
            #  gather variables needed
            ###
            schemaName = None
            attrSrc = None
            dataSrc = None
            attrDest = None
            dataDest = None
            orderByFields = None
            partByFields = None

            for table in self.commonParams["cat"]["tables"]:
                schemaName = table["schemaName"]
                if table["type"] == "attributes":
                    attrSrc = table["srctable"]
                    attrDest = table["table"]
                    if "partition" in table:
                        orderByFields = table["partition"]["order"]
                        partByFields = table["partition"]["over"]
                elif table["type"] == "series":
                    dataSrc = table["srctable"]
                    dataDest = table["table"]

            with open(sqlMigrateTemplate) as infile, open(sqlMigrateScript, 'w') as outfile:
                for line in infile:
                    line = line.replace('{schemaName}', schemaName)
                    line = line.replace('{attrSrc}', attrSrc)
                    line = line.replace('{dataSrc}', dataSrc)
                    line = line.replace('{attrDest}', attrDest)
                    line = line.replace('{dataDest}', dataDest)
                    line = line.replace('{orderByFields}', orderByFields)
                    line = line.replace('{partByFields}', partByFields)
                    outfile.write(line)
            self.logger.debug(self.moduleName + " -- " + "CreateMigrationScript" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in CreateMigrationScript")
            raise
        return sqlMigrateScript

    def MigrateData(self):
        '''
        Routine to create and run the sql to create version in RedShift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + " MigrateData " + " starting ")
            verScript = self.CreateMigrationScript()
            self.logger.info(self.moduleName + " - script file name = " + verScript)
            RedshiftUtilities.PSqlExecute(verScript, self.logger)
            self.logger.debug(self.moduleName + " -- " + " MigrateData " + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in MigrateData")
            raise

    def BuildRecordSQL(self):
        '''
        build the sql that will be used for the meat of the sql query
        '''
        rtnSQL = None
        try:
            partBy = None
            orderBy = None

            for cat in self.commonParams["cat"]["tables"]:
                if cat["type"] == "attributes":
                    if "partition" in cat:
                        partBy = cat["partition"]["over"]
                        orderBy = cat["partition"]["order"]
                        rtnSQL = "select *, ROW_NUMBER() over(PARTITION By " + partBy +\
                                 " order by " + orderBy + " ) as rownum " +\
                                 " from " + cat["schemaName"] + "." + cat["srctable"]
        except:
            self.logger.exception(self.moduleName + " - we had an error in BuildRecordCountSQL")
            raise
        return rtnSQL

    def GetRecordCount(self, rsConnect):
        '''
        Utility to get the record count
        '''
        cur = rsConnect.cursor()
        attrs = self.GetAttributesConfig()
        series = self.GetSeriesConfig()

        command = "select ( " +\
                "(select count(*) from " + attrs["schemaName"] + "." + attrs["srctable"] + " where " + attrs["versionField"] + " = " +\
                "(select min(" + attrs["versionField"] + ") from " + attrs["schemaName"] + "." + attrs["srctable"] + ")) +" +\
                "(select count(*) from " + series["schemaName"] + "." + series["srctable"] + " where " + series["keyField"] + " in(" +\
                "select distinct object_id from " + attrs["schemaName"] + "." + attrs["srctable"] + " where " + attrs["versionField"] + " = " +\
                "(select min(publisheddate) from " + attrs["schemaName"] + "." + attrs["srctable"] + ")))" +\
                ") as recCount"
        try:
            cur.execute(command)
        except Exception as ex:
            rsConnect.rollback() # Rollback or else the connection will fail
            cur.close()
            raise ex

        data = cur.fetchall()
        cur.close()
        return data[0][0]

    def GetAttributesConfig(self):
        '''
        Returns the attributes configuration
        '''
        attrsConfig = {}

        for cat in self.commonParams["cat"]["tables"]:
            if cat["type"] == "attributes":
                attrsConfig['schemaName'] = cat["schemaName"]
                attrsConfig['srctable'] = cat["srctable"]
                attrsConfig['versionField'] = cat["versionField"]

        return attrsConfig

    def GetSeriesConfig(self):
        '''
        Returns the series configuration
        '''
        seriesConfig = {}

        for cat in self.commonParams["cat"]["tables"]:
            if cat["type"] == "series":
                seriesConfig['schemaName'] = cat["schemaName"]
                seriesConfig['srctable'] = cat["srctable"]
                seriesConfig['keyField'] = cat["keyField"]

        return seriesConfig

    def GetNumberOfIterations(self, rsConnect):
        '''
        Utility to get the number of iterations
        '''
        cur = rsConnect.cursor()
        attrs = self.GetAttributesConfig()
        command = "SELECT COUNT(distinct " + attrs["versionField"] + ") FROM " + attrs["schemaName"] + "." + attrs["srctable"]

        try:
            cur.execute(command)
        except Exception as ex:
            rsConnect.rollback() # Rollback or else the connection will fail
            cur.close()
            raise ex

        data = cur.fetchall()
        cur.close()
        return data[0][0]

    def GetOldestPublishedDate(self, rsConnect):
        '''
        Returns the oldest published date from the working table
        '''
        cur = rsConnect.cursor()
        attrs = self.GetAttributesConfig()
        command = "select to_char(min(" + attrs["versionField"] + "), 'YYYY-MM-DD') as publisheddate from " + attrs["schemaName"] +\
                "." + attrs["srctable"]

        try:
            cur.execute(command)
        except Exception as ex:
            rsConnect.rollback() # Rollback or else the connection will fail
            cur.close()
            raise ex

        data = cur.fetchall()
        cur.close()
        return data[0][0]

    def CreateTables(self):
        '''
        Start of routine
        sqlFolder must be populated

        '''
        try:
            self.moduleName = self.commonParams["moduleName"]
            self.logger = FileUtilities.CreateLogger(self.commonParams["loggerParams"])
            for tblJson in self.commonParams["cat"]["tables"]:
                fname = self.fileUtilities.CreateTableSql(tblJson, self.commonParams["sqlFolder"])
                RedshiftUtilities.PSqlExecute(fname, self.logger)
        except Exception:
            self.logger.exception(self.commonParams["moduleName"] + "- we had an error in StartHere")
            raise
