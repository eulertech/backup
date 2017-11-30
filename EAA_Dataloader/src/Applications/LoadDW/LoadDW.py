'''
Created on Apr, 2017

@author: Hector Hernandez
@summary: LoadDW (Load Data Warehouse) handles all activities related to the final load stage in the ETL process.
'''
import os
import ntpath
import random
import pandas

from Applications.Common.ApplicationBase import ApplicationBase
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities

class LoadDW(ApplicationBase):
    '''
    Handles all activities related to the final load stage in the ETL process
    '''

    def __init__(self):
        '''
        Constructor
        '''
        super(LoadDW, self).__init__()
        self.awsParams = ""
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def GetValueTypeField(self, fieldCfg, col, params):
        '''
        Returns the value formatted in a concatenation for a single field configured.
        '''

        try:
            val = params["refTable"] + "." + fieldCfg["source"]

            if col["type"] == "VARCHAR" and "replace" in fieldCfg:
                val = "REGEXP_REPLACE(" + val + ",'[" + ''.join(fieldCfg["replace"]["chars"]) + "]', '" + fieldCfg["replace"]["with"] + "')"
            elif col["type"] == "DATE" and "dtFormat" in fieldCfg:
                if fieldCfg["dtFormat"] != "":
                    val = "TO_DATE(" + val + ", '" + fieldCfg["dtFormat"] + "')"
                else:
                    raise Exception("Source column needs date format to be specified, column name: " + col["name"])
            elif col["type"] in self.job["no_precision_types"]:
                val = "CAST(" + val + " AS " + col["type"] + ")"
            elif col["type"] in self.job["precision_types"]:
                if "precision" in fieldCfg and "scale" in fieldCfg:
                    val = "CAST(" + val + " AS " + col["type"] +"(" + col["precision"] +"," + col["scale"] + "))"
                else:
                    raise Exception("Precision and Scale configuration needed for decimal column: " + col["name"])

            if params["valuesLen"] > 1:
                if params["valuesCounter"] != params["valuesLen"]:
                    val = val + " || '" + params["delimiter"] + "' || "

            return val
        except Exception as err:
            self.logger.error("Error at GetValueTypeField. Error: " + err.message)
            raise

    def AddJoinStatement(self, params):
        '''
        Returns the join expression formatted for a field configured as join type.
        '''

        fieldCfg = params["val"]
        fromStatement = params["fromStatement"]
        mainTable = params["mainTable"]
        alias = params["alias"]
        tb = params["tb"]

        try:
            if "sourceSchema" in fieldCfg:
                joinSchema = self.GetSourceSchema(fieldCfg)
            else:
                joinSchema = self.GetSourceSchema(tb)

            joinTable = joinSchema + "." + fieldCfg["from"]
            fromStatement = fromStatement + "\n\tINNER JOIN " + joinTable + " AS " + alias + " ON " + alias + "." + fieldCfg["matchFrom"] + \
                " = " + mainTable + "." + fieldCfg["matchSource"]

            if "filter" in fieldCfg:
                addFilter = fieldCfg["filter"]
                addFilter = addFilter.replace("{from}", alias)
                addFilter = addFilter.replace("{source}", mainTable)
                fromStatement = fromStatement + " AND " + addFilter

            return fromStatement
        except StandardError as err:
            self.logger.error("Error in AddJoinStatement(). Error: " + err.message)
            raise

    def GetValueTypeConstant(self, col, params):
        '''
        Returns the value formatted in a concatenation for a single value configured as a constant.
        '''

        try:
            colType = col["type"]
            fieldCfg = params["val"]
            delimiter = params["delimiter"]
            valuesLen = params["valueLen"]
            valuesCounter = params["valuesCounter"]
            constStatement = "'" + fieldCfg["source"] + "'"

            if colType == "DATE":
                if "dtFormat" in fieldCfg:
                    constStatement = "TO_DATE(" + constStatement + ", '" + fieldCfg["dtFormat"] + "')"
            elif colType in self.job["no_precision_types"]:
                constStatement = "CAST(" + constStatement + " AS " + colType +")"
            elif colType in self.job["precision_types"]:
                if "precision" in fieldCfg and "scale" in fieldCfg:
                    constStatement = "CAST(" + constStatement + " AS " + colType +"(" + col["precision"] +"," + col["scale"] + "))"
                else:
                    raise Exception("Precision and Scale configuration needed for decimal column: " + col["name"])

            if valuesLen > 1:
                if valuesCounter != valuesLen:
                    constStatement = constStatement + " || '" + delimiter + "' || "

            return constStatement
        except StandardError as err:
            self.logger.error("Error in GetValueTypeConstant(). Error: " + err.message)
            raise

    def GetValueTypeQuery(self, fieldCfg, params):
        '''
        Returns the value formatted in a concatenation for a single value configured as a query type.
        '''

        try:
            queryVal = "(" + fieldCfg["source"] + ")"
            queryVal = queryVal.replace("{schemaName}", self.job["destinationSchema"])
            queryVal = queryVal.replace("{sourceSchema}", params["sourceSchema"])

            if params["valuesLen"] > 1 and params["valuesCounter"] == params["valuesLen"]:
                queryVal = queryVal + " || '" + params["delimiter"] + "' || "

            return queryVal
        except Exception as err:
            self.logger.error("Error in GetValueTypeQuery(). Error: " + err.message)
            raise

    def CreateFileFromStatement(self, sqlStatement, appName, tableName):
        '''
        Creates the sql script based on a given statement
        '''

        try:
            scriptFileNameOut = self.localTempDirectory + "/" + appName + "_" + tableName + ".sql"
            self.fileUtilities.RemoveFileIfItExists(scriptFileNameOut)
            self.fileUtilities.WriteToFile(scriptFileNameOut, sqlStatement)

            return scriptFileNameOut
        except Exception as err:
            self.logger.error("Error in CreateFileFromStatement(). Error: " + err.message)
            raise

    def AppendLoadStatement(self, loopParams, tb):
        '''
        Helps the GetLoadScript() to append sql script statements into the insert, select and from statements variables
        '''

        values = []
        fieldValue = ""
        delimiter = ""
        selectStatement = loopParams["selectStatement"]
        insertStatement = loopParams["insertStatement"]
        fromStatement = loopParams["fromStatement"]
        colCounter = loopParams["colCounter"]
        valuesCounter = 0

        if "delimiter" in loopParams["col"]:
            delimiter = loopParams["col"]["delimiter"]

        valEval = loopParams["col"]["value"]

        if isinstance(valEval, dict):
            values.append(valEval)
        elif isinstance(valEval, list):
            values = None
            values = valEval
        else:
            values = [{"value": valEval, "type": "constant"}]

        valueLen = len(values)

        for val in values:
            valuesCounter = valuesCounter + 1

            if val["type"] == "field":
                if "from" in val:
                    alias = loopParams["col"]["name"] + str(random.randint(0, 1000))
                    fieldValue = fieldValue + self.GetValueTypeField(val, loopParams["col"],
                                                                     {"delimiter": delimiter,
                                                                      "valuesLen": valueLen,
                                                                      "valuesCounter": valuesCounter,
                                                                      "refTable": alias})

                    fromStatement = self.AddJoinStatement({"val": val,
                                                           "fromStatement": fromStatement,
                                                           "mainTable": loopParams["mainTable"],
                                                           "alias": alias,
                                                           "tb": tb})
                else:
                    fieldValue = fieldValue + self.GetValueTypeField(val, loopParams["col"],
                                                                     {"delimiter": delimiter,
                                                                      "valuesLen": valueLen,
                                                                      "valuesCounter": valuesCounter,
                                                                      "refTable": loopParams["mainTable"]})
            elif val["type"] == "constant":
                fieldValue = fieldValue + self.GetValueTypeConstant(loopParams["col"], {"val": val,
                                                                                        "delimiter": delimiter,
                                                                                        "valueLen": valueLen,
                                                                                        "valuesCounter": valuesCounter})
            elif val["type"] == "query":
                fieldValue = fieldValue + self.GetValueTypeQuery(val,
                                                                 {"delimiter": delimiter,
                                                                  "valuesLen": valueLen,
                                                                  "valuesCounter": valuesCounter,
                                                                  "sourceSchema": loopParams["sourceSchema"]})

        if loopParams["colsLen"] == colCounter:
            selectStatement = selectStatement + fieldValue
            insertStatement = insertStatement + loopParams["col"]["name"]
        else:
            selectStatement = selectStatement + fieldValue + ",\n"
            insertStatement = insertStatement + loopParams["col"]["name"] + ", "

        return insertStatement, selectStatement, fromStatement

    def GetLoadScript(self, tb, appName, sourceSchema):
        '''
        Creates the sql script to load the data per application (both data and attributes)...
        '''

        try:
            self.logger.info("[" + appName + "] - Creating loading script for " + self.job["tableName"] + "_" + tb["destination"])

            colCounter = 0
            colsLen = len(tb["cols"])
            mainTable = sourceSchema + "." + tb["source"]
            insertStatement = "INSERT INTO " + self.job["destinationSchema"] + "." + self.job["tableName"] + "_" + tb["destination"] + "("
            selectStatement = "\nSELECT "
            fromStatement = "\nFROM " + mainTable

            for col in tb["cols"]:
                if col["type"] == "COMPUTED":
                    colsLen = colsLen - 1

            for col in tb["cols"]:
                if col["type"] == "COMPUTED":
                    continue

                colCounter = colCounter + 1

                params = {
                    "col": col,
                    "insertStatement": insertStatement,
                    "selectStatement": selectStatement,
                    "colCounter": colCounter,
                    "colsLen": colsLen,
                    "fromStatement": fromStatement,
                    "mainTable": mainTable,
                    "sourceSchema": sourceSchema
                }

                insertStatement, selectStatement, fromStatement = self.AppendLoadStatement(params, tb)

            insertStatement = insertStatement + ")"
            selectStatement = selectStatement + fromStatement

            if tb["destination"] == "attributes":
                colsPos = []

                for i in range(colsLen):
                    colsPos.append(str(i + 1))

                selectStatement = "\nSELECT * \nFROM (" + selectStatement + ") \nGROUP BY " + (",").join(colsPos)

            insertStatement = insertStatement + selectStatement
            insertStatement = insertStatement + ";\n\nCOMMIT;"

            return self.CreateFileFromStatement(insertStatement, appName, self.job["tableName"] + "_" + tb["destination"])
        except Exception as err:
            self.logger.error("Error in GetLoadScript(). Error: " + err.message)
            raise

    def ExecuteScript(self, scriptName, tableName=None, sourceSchema=None):
        '''
        Executes a given sql script file from the app's root directory.
        '''
        try:
            templateFile = self.fileUtilities.PathToForwardSlash(os.path.join(self.location, scriptName))

            if sourceSchema:
                outputFile = self.fileUtilities.PathToForwardSlash(os.path.join(self.localTempDirectory, scriptName + "_tmp"))
                self.fileUtilities.ReplaceStringInFile(templateFile, outputFile, {"{sourceSchema}": sourceSchema})
                templateFile = outputFile

            dsFile = self.fileUtilities.PathToForwardSlash(os.path.join(self.localTempDirectory, scriptName + "_ds"))
            self.fileUtilities.CreateActualFileFromTemplate(templateFile, dsFile, self.job["destinationSchema"], tableName)

            outFile = self.fileUtilities.PathToForwardSlash(os.path.join(self.localTempDirectory, scriptName))
            self.fileUtilities.ReplaceStringInFile(dsFile, outFile, {"{tableName}": self.job["tableName"]})

            RedshiftUtilities.PSqlExecute(outFile, self.logger)
        except Exception as err:
            self.logger.error("[" + self.moduleName + "] - Error while trying to execute script: " + scriptName)
            raise Exception(err.message)

    def ExecuteAppProcessingScripts(self, app, scriptType):
        '''
        Executes scripts in keys "preProcessScripts", "postProcessScripts" and "transformScript".
        '''

        def PrivExecuteScript(scriptName, obj=None):
            '''
            Private method for cleanness
            '''
            self.logger.info("[" + app["name"] + "] - Executing " + scriptName)
            self.ExecuteScript(scriptName, None, self.GetSourceSchema(obj))

        try:
            if scriptType in app:
                scripts = app[scriptType]

                if isinstance(scripts, list):
                    for script in app[scriptType]:
                        if isinstance(script, dict):
                            PrivExecuteScript(script["name"], script)
                        else:
                            PrivExecuteScript(script, None)
                elif isinstance(scripts, dict):
                    PrivExecuteScript(scripts["name"], scripts)
                else:
                    PrivExecuteScript(scripts)
        except Exception as err:
            self.logger.error("[" + app["name"] + "] - Error: " + err.message)
            raise

    def GetTransposedScript(self, transp, appName, sourceSchema):
        '''
        Generates a script to transpose columns into rows...
        '''

        try:
            unionStatement = "SELECT " + ','.join(transp["select"]) + ", '#COLNAME#' AS " + transp["colDescriptionName"]
            unionStatement = unionStatement + ", \"#COLNAME#\" AS " + transp["colValueName"] + " from "
            unionStatement = unionStatement + sourceSchema + "." + transp["source"]

            listUnion = []

            for col in transp["unpivot"]:
                listUnion.append(unionStatement.replace("#COLNAME#", col))

            sqlStatement = "SELECT * INTO " + self.job["destinationSchema"] + "." + transp["alias"] + " FROM (\n"
            sqlStatement = sqlStatement + ' UNION ALL\n'.join(listUnion)
            sqlStatement = sqlStatement + "\n);"
            sqlStatement = sqlStatement + "\n\nCOMMIT;"

            return self.CreateFileFromStatement(sqlStatement, appName, transp["alias"])
        except Exception as err:
            self.logger.error("Error in GetTransposedScript(). Error: " + err.message)
            raise

    def ProcessComputedFields(self):
        '''
        Calculates the fields set as to be computed by the process... for now is only the start date and end ate from attributes table.
        '''

        try:
            statement = "UPDATE " + self.job["destinationSchema"] + "." + self.job["tableName"] + "_attributes\n"
            statement = statement + "\tSET startDate = dat.datemin, endDate = dat.datemax\n"
            statement = statement + "\t\tFROM (SELECT name,  MIN(date) AS datemin, MAX(date) AS datemax FROM " +\
                        self.job["destinationSchema"] + "." +\
                        self.job["tableName"] + "_data GROUP BY name) dat\n"
            statement = statement + "\t\t\tJOIN " + self.job["destinationSchema"] + "." + self.job["tableName"] +\
                        "_attributes attr ON attr.name = dat.name AND source not in('" + "','".join(self.job["computeSourceExemptions"]) + "')\n"
            statement = statement + "WHERE (attr.startDate IS NULL OR attr.endDate IS NULL);\n"
            statement = statement + "COMMIT;"

            self.logger.info(
                "Executing compute script for attributes range dates....")

            scriptName = self.CreateFileFromStatement(statement, "loadDw_Compute", "attributes")
            RedshiftUtilities.PSqlExecute(scriptName, self.logger)
        except Exception as err:
            self.logger.error("Error in ProcessComputedFields(). Error: " + err.message)
            raise

    def GetSourceSchema(self, obj):
        '''
        Determines the source Schema to be replaced in a sql statement looking for the key --> {sourceSchema}
        '''
        sourceSchema = self.job["destinationSchema"]

        if isinstance(obj, dict):
            if "sourceSchema" in obj:
                if obj["sourceSchema"] != "":
                    sourceSchema = obj["sourceSchema"]

        return sourceSchema

    def ProcessTranspose(self, app):
        '''
        Check if the application requires to build a transpose table.
        '''

        try:
            if "transpose" in app:
                for transp in app["transpose"]:
                    scriptOut = self.GetTransposedScript(transp, app["name"], self.GetSourceSchema(transp))

                    if scriptOut is not None:
                        self.logger.info("[" + app["name"] + "] - Transposing columns...")
                        RedshiftUtilities.PSqlExecute(scriptOut, self.logger)
        except Exception as err:
            self.logger.error("Error in ProcessTranspose(). Error: " + err.message)
            raise

    def ProcessApplications(self):
        '''
        Process every application configured in the job configuration key "applications"
        '''

        try:
            for app in self.job["applications"]:
                if app["execute"] == "Y":
                    self.logger.info("[" + app["name"] + "] - Load to data warehouse process started")

                    self.ExecuteAppProcessingScripts(app, "preProcessScripts")
                    self.ProcessTranspose(app)
                    self.ExecuteAppProcessingScripts(app, "transformScript")

                    for tb in app["mapping"]:
                        self.CheckBaseColumns(tb, app["name"])
                        scriptOut = self.GetLoadScript(tb, app["name"], self.GetSourceSchema(tb))

                        if scriptOut is not None:
                            self.logger.info("[" + app["name"] + "] - Loading rows in " + self.job["tableName"] + "_" + tb["destination"])
                            RedshiftUtilities.PSqlExecute(scriptOut, self.logger)

                    self.ExecuteAppProcessingScripts(app, "postProcessScripts")
        except Exception as err:
            self.logger.error("Error in ProcessApplications(). Error: " + err.message)
            raise

    def LoadCatalogsToRedshift(self):
        '''
        load catalogs (only excel is supported for now...) into redshift.
        '''
        for cat in self.job["catalogs"]:
            rsConnect = None

            try:
                self.logger.info("Creating catalog: " + cat["createScript"])
                self.ExecuteScript(cat["createScript"], cat["tableName"])

                s3Key = cat["s3FileSource"]
                localFilepath = self.localTempDirectory + "/" + ntpath.basename(s3Key)

                self.logger.info("[" + self.moduleName + "] - Downloading catalog for: " + cat["tableName"])

                S3Utilities.DownloadFileFromS3(self.awsParams.s3, cat["bucketName"], s3Key, localFilepath)
                fileNameCSV = localFilepath.replace(os.path.splitext(localFilepath)[1], cat["pandasInputExt"])

                rsConnect = RedshiftUtilities.Connect(self.awsParams.redshift['Database'],
                                                      self.awsParams.redshift['Hostname'],
                                                      self.awsParams.redshift['Port'],
                                                      self.awsParams.redshiftCredential['Username'],
                                                      self.awsParams.redshiftCredential['Password'])

                dataFrame = pandas.read_excel(localFilepath,
                                              sheetname=cat["excelSheetName"],
                                              index_col=None,
                                              na_values=['NaN'],
                                              skiprows=cat["skipRows"],
                                              skip_footer=cat["skipFooter"])

                dataFrame.to_csv(fileNameCSV, sep=str(cat["delimiter"]), encoding='utf-8', index=False)

                self.logger.info("[" + self.moduleName + "] - Loading data for table: " + cat["tableName"])

                RedshiftUtilities.LoadFileIntoRedshift(rsConnect,
                                                       self.awsParams.s3,
                                                       self.logger,
                                                       self.fileUtilities,
                                                       fileNameCSV,
                                                       self.job["destinationSchema"],
                                                       cat["tableName"],
                                                       cat["fileFormat"],
                                                       cat["dateFormat"],
                                                       cat["delimiter"])
            except Exception as err:
                self.logger.error("Error while trying to load catalogs to Redshift. Error: " + err.message)
                raise
            finally:
                if rsConnect is not None:
                    rsConnect.close()

    def CheckBaseColumns(self, mapping, appName):
        '''
        Checks if extra columns are needed to be added...
        '''

        try:
            checkingCollection = mapping["destination"] + "BaseColumns"
            baseColumns = self.job[checkingCollection]
            addColumns = []

            for col in mapping["cols"]:
                if col["name"] not in baseColumns:
                    if col["type"] == "VARCHAR":
                        if "length" not in col:
                            raise Exception("Need to define length property for " + col["name"] + " in " + mapping["destination"])

                        colType = col["type"] + "(" + col["length"] + ") ENCODE LZO"
                    else:
                        colType = col["type"]

                    self.job[checkingCollection].append(col["name"])

                    addColumns.append({"tableName": self.job["tableName"] + "_" + mapping["destination"],
                                       "columnName": col["name"],
                                       "columnType": colType})

            addColsLen = len(addColumns)
            if addColsLen > 0:
                self.AddColumnsToTable(addColumns, appName)
        except Exception as err:
            self.logger.error("Error in CheckBaseColumns(). Error: " + err.message)
            raise

    def AddColumnsToTable(self, aColumns, appName):
        '''
        Add extra columns to the base tables if needed
        '''

        try:
            statements = []

            for col in aColumns:
                statements.append("ALTER TABLE " + self.job["destinationSchema"] + "." +
                                  col["tableName"] + " ADD COLUMN " + col["columnName"] + " " + col["columnType"])

            scriptName = self.CreateFileFromStatement((";\n").join(statements), appName + "_alter_", "baseTable")
            RedshiftUtilities.PSqlExecute(scriptName, self.logger)
        except Exception as err:
            self.logger.error("Error in AddColumnsToTable(). Error: " + err.message)
            raise

    def ExecuteFinalCleanUp(self):
        '''
        Executes clean up script if any in at the root level for all applications processed.
        '''

        try:
            if "cleanUpScript" in self.job:
                if self.job["cleanUpScript"] != "":
                    self.logger.info("[" + self.moduleName + "] - Cleaning up database objects...")
                    self.ExecuteScript(self.job["cleanUpScript"])
        except Exception as err:
            self.logger.error("Error in ExecuteFinalCleanUp(). Error: " + err.message)
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        Main control of the final stage in the ETL process
        '''
        ApplicationBase.Start(self, logger, moduleName, filelocs)

        try:
            self.LoadCatalogsToRedshift()
            self.ProcessApplications()
            self.ProcessComputedFields()
        except StandardError as err:
            self.logger.error("[" + self.moduleName + "] - Process failed. Error: " + err.message)
            raise
        finally:
            self.ExecuteFinalCleanUp()
