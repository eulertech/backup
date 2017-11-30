'''
Created on Aug, 2017

@author: Hector Hernandez
@summary: Consolidates all times series in one.
'''
import os
import time

from Applications.Common.ApplicationBase import ApplicationBase
from Applications.GEForecast.GEForecastHistory import GEForecastHistory
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities

class HindsightFederated(ApplicationBase):
    '''
    Consolidates all times series in one.
    '''

    def __init__(self):
        '''
        Constructor
        '''
        super(HindsightFederated, self).__init__()
        self.awsParams = ''
        self.seriesVersion = None
        self.buildHistoryIterations = 0
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def CleanDBObjects(self):
        '''
        Executes clean up script.
        '''
        try:
            if "cleanUpScript" in self.job:
                if self.job["cleanUpScript"] != "":
                    self.logger.info(self.moduleName + " - Cleaning up database temporal objects...")

                    cleaningUpFile = self.localTempDirectory + '/' + self.job["cleanUpScript"]
                    self.fileUtilities.CreateActualFileFromTemplate(self.location + '/sql/' + self.job["cleanUpScript"],
                                                                    cleaningUpFile,
                                                                    self.job["destinationSchema"],
                                                                    "")

                    RedshiftUtilities.PSqlExecute(cleaningUpFile, self.logger)
        except Exception as err:
            self.logger.error("Error in ExecuteFinalCleanUp(). Error: " + err.message)
            raise

    def ExecuteCreateTableScript(self, serieT, tableT):
        '''
        Executed the federated timeseries tables script
        '''
        tableName = self.job['federatedTables'][serieT][tableT]['name']

        tb = {}
        tb['fields'] = []

        tb['schemaName'] = self.job['destinationSchema']
        tb['table'] = tableName
        tb["new"] = self.job['federatedTables'][serieT][tableT]['new']
        tb['sortkey'] = self.job['federatedTables'][serieT]['sortkey']

        if 'distkey' in self.job['federatedTables'][serieT]:
            if self.job['federatedTables'][serieT]['distkey'] <> '':
                tb['distkey'] = self.job['federatedTables'][serieT]['distkey']

        for field in self.job['federatedTables'][serieT]['fields']:
            if 'exclusiveTo' in field:
                if tableT in field['exclusiveTo']:
                    tb['fields'].append(field)
            else:
                tb['fields'].append(field)

        fname = self.fileUtilities.CreateTableSql(tb, self.localTempDirectory)
        RedshiftUtilities.PSqlExecute(fname, self.logger)

    def CreateFederatedTables(self):
        '''
        Creates the federated timeseries tables
        '''
        try:
            seriesTypes = ['seriesAttributes', 'seriesData']
            tableTypes = ['history', 'latest']

            for serieT in seriesTypes:
                for tableT in tableTypes:
                    if self.job['federatedTables'][serieT][tableT]['new'] == 'Y':
                        self.ExecuteCreateTableScript(serieT, tableT)
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while creating output tables. Message:" + err.message)
            raise

    def GetSeriesDataInserts(self, sourceInfo, incrementalFilter):
        '''
        Creates the script to get the inserts to process from the sources tables.
        '''
        try:
            templateFile = self.location + '/sql/GetSeriesDataInsertsTemplate.sql'
            scriptFileOut = self.localTempDirectory + '/GetSeriesDataInserts_' + sourceInfo['view']['table'] + '.sql'
            scriptFileOutTmp = scriptFileOut + '.tmp'

            replacements = {
                '{seriesVersion}': self.seriesVersion,
                '{viewName}': sourceInfo['view']['name'],
                '{historyTable}': self.job['federatedTables']['seriesData']['history']['name'],
                '{incrementalFilter}': incrementalFilter
            }

            self.ExecutePsqlScript(templateFile, scriptFileOutTmp, scriptFileOut, replacements)
        except Exception as err:
            self.logger.error(self.moduleName + ' - Error while creating the database script. Message:' + err.message)
            raise

    def GetSeriesDataDeletes(self, sourceInfo, incrementalFilter):
        '''
        Creates the script to get the deletes to process from the sources tables.
        '''
        try:
            templateFile = self.location + '/sql/GetSeriesDataDeletesTemplate.sql'
            scriptFileOut = self.localTempDirectory + '/GetSeriesDataDeletes_' + sourceInfo['view']['table'] + '.sql'
            scriptFileOutTmp = scriptFileOut + '.tmp'

            replacements = {
                '{seriesVersion}': self.seriesVersion,
                '{viewName}': sourceInfo['view']['name'],
                '{historyTable}': self.job['federatedTables']['seriesData']['history']['name'],
                '{incrementalFilter}': incrementalFilter
            }

            self.ExecutePsqlScript(templateFile, scriptFileOutTmp, scriptFileOut, replacements)
        except Exception as err:
            self.logger.error(self.moduleName + ' - Error while creating the database script. Message:' + err.message)
            raise

    def GetSeriesDataChanges(self, sourceInfo, incrementalFilter):
        '''
        Creates the script to get the modified data series to process from the source tables.
        '''
        try:
            templateFile = self.location + '/sql/GetSeriesDataChangesTemplate.sql'
            scriptFileOut = self.localTempDirectory + '/GetSeriesDataChanges_' + sourceInfo['view']['table'] + '.sql'
            scriptFileOutTmp = scriptFileOut + '.tmp'

            replacements = {
                '{seriesVersion}': self.seriesVersion,
                '{viewName}': sourceInfo['view']['name'],
                '{historyTable}': self.job['federatedTables']['seriesData']['history']['name'],
                '{incrementalFilter}': incrementalFilter
            }

            self.ExecutePsqlScript(templateFile, scriptFileOutTmp, scriptFileOut, replacements)
        except Exception as err:
            self.logger.error(self.moduleName + ' - Error while creating the database script. Message:' + err.message)
            raise

    def GetSeriesAttributesChanges(self, sourceInfo, incrementalFilter):
        '''
        Creates the script to get the inserts to process from the sources tables.
        '''
        try:
            templateFile = self.location + '/sql/GetSeriesAttributesChangesTemplate.sql'
            scriptFileOut = self.localTempDirectory + '/GetSeriesAttributesChanges_' + sourceInfo['view']['table'] + '.sql'
            scriptFileOutTmp = scriptFileOut + '.tmp'

            sourceFields = []
            targetFields = []
            historyFields = []

            for col in sourceInfo['mapping']:
                if col['type'] == 'constant':
                    sourceFields.append("'" + col['source'] + "'")
                else:
                    sourceFields.append(col['source'])

                historyFields.append('h.' + col['target'])
                targetFields.append(col['target'])

            replacements = {
                '{seriesVersion}': self.seriesVersion,
                '{viewName}': sourceInfo['view']['name'],
                '{sourceFields}': ','.join(sourceFields),
                '{targetFields}': ','.join(targetFields),
                '{historyFields}': ','.join(historyFields),
                '{historyTable}': self.job['federatedTables']['seriesAttributes']['history']['name'],
                '{incrementalFilter}': incrementalFilter
            }

            self.ExecutePsqlScript(templateFile, scriptFileOutTmp, scriptFileOut, replacements)
        except Exception as err:
            self.logger.error(self.moduleName + ' - Error while creating the database scripts for Message:' + err.message)
            raise

    def BuildAttributesLatestVersion(self):
        '''
        Set the latest version for the attributes table
        '''
        templateFile = self.location + '/sql/BuildAttributesLatestVersionTemplate.sql'
        scriptFileOut = self.localTempDirectory + '/BuildAttributesLatestVersion.sql'
        scriptFileOutTmp = scriptFileOut + '.tmp'

        replacements = {
            '{federatedAttributesHistoryTable}': self.job['federatedTables']['seriesAttributes']['history']['name'],
            '{federatedAttributesLatestTable}': self.job['federatedTables']['seriesAttributes']['latest']['name']
        }

        self.ExecutePsqlScript(templateFile, scriptFileOutTmp, scriptFileOut, replacements)

    def UnloadData(self, s3TempFolder):
        '''
        Unloads only the names or series keys to s3
        '''
        unloadQuery = "select distinct name from " + self.job['destinationSchema'] + "." + \
                        self.job['federatedTables']['seriesAttributes']['history']['name']

        rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                              host=self.awsParams.redshift['Hostname'],
                                              port=self.awsParams.redshift['Port'],
                                              user=self.awsParams.redshiftCredential['Username'],
                                              password=self.awsParams.redshiftCredential['Password'])

        RedshiftUtilities.UnloadDataToS3(rsConnect, self.awsParams.s3,
                                         {
                                             "query": unloadQuery,
                                             "s3Folder": s3TempFolder
                                         },
                                         self.logger)

    def BuildDataLatestVersion(self):
        '''
        Get the list of files created and starts building by chunks
        '''
        s3TempFolder = 's3://' + self.job['bucketName'] + '/' + self.job['s3SrcDirectory']
        self.UnloadData(s3TempFolder)

        for idx in range(self.job["s3partitions"]):
            if idx < 10:
                fileName = "000" + str(idx) + "_part_00"
            else:
                fileName = "00" + str(idx) + "_part_00"

            templateFile = self.location + '/sql/BuildDataLatestVersionTemplate.sql'
            scriptFileOut = self.localTempDirectory + '/BuildDataLatestVersion_' + fileName + '.sql'
            scriptFileOutTmp = scriptFileOut + '.tmp'

            replacements = {
                '{federatedDataHistoryTable}': self.job['federatedTables']['seriesData']['history']['name'],
                '{federatedDataLatestTable}': self.job['federatedTables']['seriesData']['latest']['name'],
                '{s3TempBucket}': s3TempFolder,
                '{s3FileName}': fileName,
                '{aws_access_key_id}': self.awsParams.s3['access_key_id'],
                '{aws_secret_access_key}': self.awsParams.s3['secret_access_key']
            }

            self.ExecutePsqlScript(templateFile, scriptFileOutTmp, scriptFileOut, replacements)

    def ExecutePsqlScript(self, templateFile, scriptFileOutTmp, scriptFileOut, replacements):
        '''
        Executes a psqsl..
        '''
        self.fileUtilities.CreateActualFileFromTemplate(templateFile, scriptFileOutTmp, self.job["destinationSchema"], None)
        self.fileUtilities.ReplaceStringInFile(scriptFileOutTmp, scriptFileOut, replacements)
        self.fileUtilities.RemoveFileIfItExists(scriptFileOutTmp)

        RedshiftUtilities.PSqlExecute(scriptFileOut, self.logger)

    def BuildLatestVersion(self):
        '''
        Builds the Time Series stage table from the history.
        '''
        try:
            if self.job['buildLatestVersion'] == 'Y':
                self.BuildAttributesLatestVersion()
                self.BuildDataLatestVersion()
        except Exception as err:
            self.logger.error(self.moduleName + ' - Error while building time series version table for. Message:' + err.message)
            raise

    def CreateDataView(self, sourceInfo, sourceSchema):
        '''
        Creates the script to get the inserts to process from the sources tables.
        '''
        try:
            templateFile = self.location + '/sql/CreateViewTemplate.sql'
            scriptFileOut = self.localTempDirectory + '/CreateView_' + sourceInfo['view']['name'] + '.sql'
            scriptFileOutTmp = scriptFileOut + '.tmp'
            distinctTxt = ""
            sourceFields = []

            if 'applyDistinct' in sourceInfo:
                if sourceInfo['applyDistinct'] == 'Y':
                    distinctTxt = 'distinct'

            for col in sourceInfo['mapping']:
                if col['type'] == 'constant':
                    sourceFields.append("'" + col['source'] + "' as " + col['target'])
                else:
                    sourceFields.append(col['source'] + ' as ' + col['target'])

            replacements = {
                '{seriesVersion}': self.seriesVersion,
                '{sourceSchema}': sourceSchema,
                '{viewName}': sourceInfo['view']['name'],
                '{sourceTable}': sourceInfo['view']['table'],
                '{sourceFields}': ','.join(sourceFields),
                '{schemaName}': self.job['destinationSchema'],
                '{distinct}': distinctTxt
            }
            
            replacements['{sourceFields}'] = replacements['{sourceFields}'].replace('{sourceSchema}', sourceSchema)
            
            if 'joins' in sourceInfo['view']:
                replacements['{viewJoins}'] = ''.join(sourceInfo['view']['joins'])
                replacements['{viewJoins}'] = replacements['{viewJoins}'].replace('{sourceSchema}', sourceSchema)
            else:
                replacements['{viewJoins}'] = ''

            if 'filters' in sourceInfo['view']:
                replacements['{viewFilters}'] = 'where ' + ''.join(sourceInfo['view']['filters'])
                replacements['{viewFilters}'] = replacements['{viewFilters}'].replace('{seriesVersion}', self.seriesVersion)
            else:
                replacements['{viewFilters}'] = ''

            self.ExecutePsqlScript(templateFile, scriptFileOutTmp, scriptFileOut, replacements)
        except Exception as err:
            self.logger.error(self.moduleName + ' - Error while creating the data view script. Message:' + err.message)
            raise

    def GetIncrementalFilter(self, config):
        '''
        Gets the incremental filter configured to be used in the source data.
        '''
        incrementalFilter = ''

        if config is not None:
            if config <> '':
                if config['active'] == 'Y':
                    incrementalFilter = config['filter']
                    incrementalFilter = incrementalFilter.replace('{schemaName}', self.job['destinationSchema'])
                else:
                    incrementalFilter = ''

        return incrementalFilter

    def PullGEForecastHistoryTables(self, cat):
        '''
        Pull GEForecast History
        '''
        try:
            commonParams = {}
            commonParams["cat"] = cat
            commonParams["moduleName"] = self.moduleName
            commonParams["loggerParams"] = "log"
            commonParams["sqlFolder"] = self.localTempDirectory

            gefh = GEForecastHistory()
            gefh.commonParams = commonParams
            gefh.fileUtilities = self.fileUtilities
            gefh.CreateTables()
            gefh.MigrateData()
        except:
            raise

    def GetGEForecastHistoryIterations(self, cat):
        '''
        Get GEForecast History iterations
        '''
        try:
            numIterations = 0

            commonParams = {}
            commonParams["cat"] = cat
            commonParams["moduleName"] = self.moduleName
            commonParams["loggerParams"] = "log"
            commonParams["sqlFolder"] = self.localTempDirectory

            gefh = GEForecastHistory()
            gefh.commonParams = commonParams
            gefh.fileUtilities = self.fileUtilities
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            numIterations = int(gefh.GetNumberOfIterations(rsConnect))
            rsConnect.close()
            print numIterations
            return numIterations
        except:
            raise

    def GetBuildHistoryIterations(self, tsApp):
        '''
        Checks if the history need to be iterated on the source returning the number of iterations.
        '''
        iterations = 1

        if 'preProcess' in tsApp:
            if 'routine' in tsApp['preProcess']:
                if tsApp['preProcess']['routine'] == 'PullGEForecastHistoryTables':
                    iterations = self.GetGEForecastHistoryIterations(tsApp['preProcess']['config'])
                    iterations = iterations

        return iterations

    def GetBuildHistorySeriesVersion(self, cat):
        '''
        Get GEForecast History Series Version
        '''
        try:
            seriesVersion = None

            commonParams = {}
            commonParams["cat"] = cat
            commonParams["moduleName"] = self.moduleName
            commonParams["loggerParams"] = "log"
            commonParams["sqlFolder"] = self.localTempDirectory

            gefh = GEForecastHistory()
            gefh.commonParams = commonParams
            gefh.fileUtilities = self.fileUtilities
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            seriesVersion = gefh.GetOldestPublishedDate(rsConnect)
            rsConnect.close()

            return seriesVersion
        except:
            raise

    def GetSeriesVersion(self, tsApp):
        '''
        Return the version to be set for the processing application.
        '''
        seriesVersion = time.strftime('%Y-%m-%d')

        if 'preProcess' in tsApp:
            if 'routine' in tsApp['preProcess']:
                if tsApp['preProcess']['routine'] == 'PullGEForecastHistoryTables':
                    seriesVersion = self.GetBuildHistorySeriesVersion(tsApp['preProcess']['config'])

        return seriesVersion

    def Start(self, logger, moduleName, filelocs):
        '''
        Starting point and main control.
        '''
        ApplicationBase.Start(self, logger, moduleName, filelocs)

        try:
            self.CreateFederatedTables()

            for tsApp in self.job['timeSeriesApps']:
                if tsApp['execute'] == 'Y':
                    self.buildHistoryIterations = self.GetBuildHistoryIterations(tsApp)

                    for nIter in range(self.buildHistoryIterations):
                        self.seriesVersion = self.GetSeriesVersion(tsApp)
                        self.logger.info(str(nIter) + ' - ' + self.seriesVersion)

                        self.CleanDBObjects()

                        if 'preProcess' in tsApp:
                            if tsApp['preProcess']['routine'] == 'PullGEForecastHistoryTables':
                                self.PullGEForecastHistoryTables(tsApp['preProcess']['config'])

                        self.CreateDataView(tsApp['seriesAttributes'], tsApp['sourceSchema'])
                        self.CreateDataView(tsApp['seriesData'], tsApp['sourceSchema'])

                        self.GetSeriesAttributesChanges(tsApp['seriesAttributes'], self.GetIncrementalFilter(tsApp['incremental']))

                        if 'insert' in tsApp['seriesData']['transactions']:
                            self.GetSeriesDataInserts(tsApp['seriesData'], self.GetIncrementalFilter(tsApp['incremental']))

                        if 'update' in tsApp['seriesData']['transactions']:
                            self.GetSeriesDataChanges(tsApp['seriesData'], self.GetIncrementalFilter(tsApp['incremental']))

                        if 'delete' in tsApp['seriesData']['transactions']:
                            self.GetSeriesDataDeletes(tsApp['seriesData'], self.GetIncrementalFilter(tsApp['incremental']))

            self.BuildLatestVersion()
        except StandardError as err:
            self.logger.error(self.moduleName + " - Process failed. Message: " + err.message)
            raise
        finally:
            self.CleanDBObjects()
