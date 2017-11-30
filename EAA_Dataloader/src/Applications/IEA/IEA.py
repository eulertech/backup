'''
Created on Mar 17, 2017

@author: VIU53188
@summary: This application will pull data from the MODS web site and load it into S3
    Below are the steps

    1)  Pull down the zip files from web site
    2)  Place it on S3 server
    3)  Unzip the file
    4)  Create crossreference tables  these are keyed
    5)  Create working tables and final tables
    6)  load the raw data into working tables so what is currently in the file matches what is in the table
    7)  if we need to populate the reference table then we must have a function to find each component.
    8)  Load the Cross Reference tables with an initial set of data from sample tables based on config
    9)  load data into final tables based on specific sql stored in seperate files

'''
import os
import re
import ntpath
from threading import Thread

import pandas
import requests

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase

class IEA(ApplicationBase): # pylint: disable=too-many-public-methods
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(IEA, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    def PrepMultiThread(self, cOption):
        '''
        prepare to load data
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "PrepMultiThread Optoin -> " + str(cOption) + " starting ")

            processArray = []
            for proc in self.job["FileSets"]:
                if proc["execute"] == "Y":
                    processArray.append(proc)
            runs = []
            for proc in processArray:
                if cOption == '1':
                    ###
                    #  build tables -- BuildTables
                    ###
                    runs.append(Thread(target=self.AsyncBuildTables, args=(proc,)))
                elif cOption == '2':
                    ###
                    #  fix unzipped files -- FixUnzippedFiles
                    ###
                    runs.append(Thread(target=self.AsyncFixUnzippedFiles, args=(proc,)))
                elif cOption == '3':
                    ###
                    #  load files to RedShift -- LoadFilesToRedShift
                    ###
                    runs.append(Thread(target=self.AsyncLoadFilesToRedShift, args=(proc,)))
                elif cOption == "4":
                    ###
                    #  load Data -- LoadData
                    ###
                    runs.append(Thread(target=self.AsyncLoadData, args=(proc,)))
                elif cOption == "5":
                    ###
                    #  remove working tables from RedShift
                    ###
                    runs.append(Thread(target=self.AsyncDropTables, args=(proc,)))

            [r.start() for r in runs]  # pylint: disable=expression-not-assigned
            [r.join() for r in runs] # pylint: disable=expression-not-assigned
            self.logger.debug(self.moduleName + " -- " + "PrepMultiThread Option -> " + str(cOption) + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in LoadData ")
            raise

    def AsyncBuildCXR(self, proc, basesql):
        '''
        actually build the cross reference tables
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "AsyncBuildCXR for " + proc["name"] + " starting ")
            ###
            ##  first create the file so we can use it
            ###
            sqlTableCreationScript = ApplicationBase.BuildTableCreationScriptTable(self,
                                                                                   basesql,
                                                                                   proc["name"],
                                                                                   templateFolder="sql",
                                                                                   sqlFolder="sql")
            ###
            ##  execute DDL so we now have the blank table
            ###
            RedshiftUtilities.PSqlExecute(sqlTableCreationScript, self.logger)
            self.logger.debug(self.moduleName + " -- " + "AsyncBuildCXR for " + proc["name"] + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in AsyncBuildCXR")
            raise

    def BuildCrossReferenceTables(self):
        '''
        set up process to build the cross reference tables
        '''
        try:
            processCxrArray = []
            basesql = self.job["CrossReference"]["basesql"]
            for proc in self.job["CrossReference"]["Categories"]:
                if proc["execute"] == "Y":
                    processCxrArray.append(proc)
            runs = []
            for proc in processCxrArray:
                runs.append(Thread(target=self.AsyncBuildCXR, args=(proc, basesql,)))

            [r.start() for r in runs] # pylint: disable=expression-not-assigned
            [r.join() for r in runs] # pylint: disable=expression-not-assigned
        except:
            self.logger.exception(self.moduleName + "- we had an error in BuildCrossReferenceTables ")
            raise

    def AsyncPullFilesFromWeb(self, user, pwd, url):
        '''
        pull the data
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "AsyncPullFilesFromWeb for " + url + " starting ")
            ###
            #  create url to use to pull file and write the binary to local temp folder
            ###
            fname = ntpath.basename(url)
            filename = self.localTempDirectory + '/zips/' + fname
            req = requests.get(url, auth=(user, pwd))
            if req.status_code == 200:
                with open(filename, 'wb') as out:
                    for bits in req.iter_content():
                        out.write(bits)
            self.logger.debug(self.moduleName + " -- " + "AsyncPullFilesFromWeb for " + url + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in AsyncPullFilesFromWeb - " + url)
            raise

    def PullFilesFromWeb(self):
        '''
        set up to pull data
        '''
        try:
            params = self.job["MODSWebsite"]
            user = params["username"]
            pwd = params["pass"]
            urls = []
            urls.append(params["SupplyDemandBalanceStocks"])
            urls.append(params["FieldByField"])
            runs = []
            runs.append(Thread(target=self.AsyncPullFilesFromWeb, args=(user, pwd, params["SupplyDemandBalanceStocks"])))
            runs.append(Thread(target=self.AsyncPullFilesFromWeb, args=(user, pwd, params["SupplyDemandBalanceStocksHistory"])))
            runs.append(Thread(target=self.AsyncPullFilesFromWeb, args=(user, pwd, params["FieldByField"])))

            [r.start() for r in runs] # pylint: disable=expression-not-assigned
            [r.join() for r in runs] # pylint: disable=expression-not-assigned
        except:
            self.logger.exception(self.moduleName + "- we had an error in PullFilesFromWeb")
            raise

    def PushFilesToS3(self):
        '''
        push files to s3 server
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "PushFilesToS3" + " starting ")
            S3Utilities.SyncFolderAWSCli(self.localTempDirectory+'/zips',
                                         "s3://" + self.job["bucketName"] + '/' + self.job["s3SrcDirectory"] + '/zips',
                                         args='''--quiet --include "*.zip"''', dbug="Y")
            self.logger.debug(self.moduleName + " -- " + "PushFilesToS3" + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in PushFilesToS3")
            raise

    def AsyncUnzipFiles(self, url):
        '''
         unzip the file so that we can access the internal files locally
        '''
        try:
            fname = ntpath.basename(url)
            fnamenoext = fname.split('.')[0]
            self.logger.debug(self.moduleName + " -- " + "AsyncUnzipFiles --" + fname + " starting ")
            localFilename = self.localTempDirectory + '/zips'+ '/' + fname
            self.logger.info(self.moduleName + " - started unzipping " + localFilename)
            self.fileUtilities.UnzipFile(localFilename, self.localTempDirectory + '/data/' + fnamenoext)
            self.logger.debug(self.moduleName + " -- " + "AsyncUnzipFiles --" + fname + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in AsyncUnzipFiles - " + url)
            raise

    def UnzipFiles(self):
        '''
        set up to unzip files
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "UnzipFiles" + " starting ")
            params = self.job["MODSWebsite"]
            runs = []

            runs.append(Thread(target=self.AsyncUnzipFiles, args=(params["SupplyDemandBalanceStocks"],)))
            runs.append(Thread(target=self.AsyncUnzipFiles, args=(params["SupplyDemandBalanceStocksHistory"],)))
            runs.append(Thread(target=self.AsyncUnzipFiles, args=(params["FieldByField"],)))

            [r.start() for r in runs] # pylint: disable=expression-not-assigned
            [r.join() for r in runs] # pylint: disable=expression-not-assigned
            self.logger.debug(self.moduleName + " -- " + "UnzipFiles" + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in UnzipFiles")
            raise

    def AsyncBuildTables(self, proc):
        '''
        build the tables in RedShift
        '''
        try:
            ###
            ##  first create the file so we can use it
            ###
            self.logger.info(self.moduleName + " - " + proc["processname"] +  " - SQL tables starting.")
            sqlTableCreationScript = ApplicationBase.BuildTableCreationScriptTable(self,
                                                                                   proc["sqlscript"],
                                                                                   proc["processname"],
                                                                                   templateFolder="sql",
                                                                                   sqlFolder="sql")
            ###
            #  now we create the table from the file created
            ###
            RedshiftUtilities.PSqlExecute(sqlTableCreationScript, self.logger)
            self.logger.info(self.moduleName + " - " + proc["processname"] +  " - SQL tables created finished.")
        except:
            self.logger.exception(self.moduleName + "- we had an error in AsyncBuildCXR")
            raise

    def AsyncFixUnzippedFiles(self, proc):
        '''
        fix the files that were unzipped
        '''
        try:
            ###
            #  the contents of the file needs to have the white spaces condensed and then make
            #  each element comma separated so that we can load it in to the working tables
            ###
            navalues = None

            for pFile in proc["processfile"]:
                pFileNoPath = pFile.replace('/', '_')
                self.logger.info(self.moduleName + " - " + proc["processname"] +  " - started fix file "  + pFile)
                localFilepath = self.localTempDirectory + '/data/' + pFile
                scrubbedFilepath = self.localTempDirectory + "/scrubbed/" + "scrub_" + pFileNoPath

                with open(localFilepath) as infile, open(scrubbedFilepath, 'w') as outfile:
                    for line in infile:
                        line = line.strip()
                        line = re.sub('\\s+', ',', line)
                        outfile.write(line + '\n')

                if 'pandas_replace' in proc:
                    if pFile == proc['pandas_replace']['processfile']:
                        if 'na_values' in proc['pandas_replace']:
                            navalues = proc['pandas_replace']['na_values']

                        df = pandas.read_csv(scrubbedFilepath,
                                             sep=self.job['delimiter'],
                                             names=proc['pandas_replace']['columnNames'],
                                             na_values=navalues)

                        df = df[proc['pandas_replace']['usecolumnNames']]
                        df.to_csv(scrubbedFilepath, header=False, sep=str(self.job["delimiter"]), encoding='utf-8', index=False)

                self.logger.info(self.moduleName + " - " + proc["processname"] +  " - completed fix file "  + pFile)
        except:
            self.logger.exception(self.moduleName + "- we had an error in AsyncFixUnzippedFiles")
            raise

    def AsyncLoadFilesToRedShift(self, proc):
        '''
        load files into RedShift
        '''
        try:
            for pFile in proc["processfile"]:
                pFileNoPath = pFile.replace('/', '_')
                self.logger.debug(self.moduleName + " -- " + "AsyncLoadFilesToRedShift for " + pFile + " starting ")
                rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)
                outputfileName = self.localTempDirectory + '/scrubbed/' + 'scrub_' + pFileNoPath
                rsTable = 'working_'+ proc["processname"]

                RedshiftUtilities.LoadFileIntoRedshift(rsConnect,
                                                       self.awsParams.s3,
                                                       self.logger, self.fileUtilities, outputfileName,
                                                       self.job["destinationSchema"], rsTable,
                                                       self.job["fileFormat"], self.job["dateFormat"],
                                                       self.job["delimiter"])
                rsConnect.close()
                self.logger.debug(self.moduleName + " -- " + "AsyncLoadFilesToRedShift for " + pFile + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in AsyncLoadFilesToRedShift ")
            raise

    def AsyncLoadCrossReferences(self, proc):
        '''
        load data into cross reference tables
        '''
        try:
            ###
            #  creates a SQL statement that crosses all work tables that are associated for a particular Cross Reference table
            #  and inserts that data
            ###
            self.logger.info(self.moduleName + "- started populating cross reference data for  " + proc["name"])
            sql = """
            insert into """ + self.job["destinationSchema"] + """.""" + proc["name"] +\
            """    (source)
            select """ + proc["sourcefield"] + """
            from ("""
            ndx = 0
            for table in proc["sourcetables"]:
                if ndx > 0:
                    sql = sql + " union "
                sql = sql + "\n select " + proc["sourcefield"] +\
                            " from " + self.job["destinationSchema"] + "." + table["name"]
                ndx = ndx + 1

            sql = sql + " ) group by " + proc["sourcefield"]
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)
            cur = rsConnect.cursor()
            cur.execute(sql)
            cur.close()
            rsConnect.commit()
            rsConnect.close()
            self.logger.info(self.moduleName + "- completed populating cross reference data for  " + proc["name"])
        except:
            self.logger.error(self.moduleName + "- we had an error in AsyncLoadCrossReferences ")
            raise

    def LoadCrossReferences(self):
        '''
        set up to load cross reference tables
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadCrossReferences " + " starting ")

            processCxrArray = []
            for proc in self.job["CrossReference"]["Categories"]:
                if proc["execute"] == "Y":
                    processCxrArray.append(proc)
            runs = []
            for proc in processCxrArray:
                runs.append(Thread(target=self.AsyncLoadCrossReferences, args=(proc,)))

            [r.start() for r in runs] # pylint: disable=expression-not-assigned
            [r.join() for r in runs]  # pylint: disable=expression-not-assigned
            self.logger.debug(self.moduleName + " -- " + "LoadCrossReferences " + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in LoadCrossReferences ")
            raise

    def GetListOfFields(self, fields, useSchema):
        '''
        giet list of fields from json
        '''
        retVal = ""
        try:
            ndx = 0
            for fld in fields:
                if ndx > 0:
                    retVal = retVal + ","
                if useSchema is True:
                    if 'CXRTable' in fld:
                        retVal = retVal + "NVL2(" +\
                                    fld["set"] + ".destination," + fld["set"] + ".destination," +  "s." +  fld["name"] +\
                                    ")"
                    else:
                        retVal = retVal + "s." +  fld["name"]
                else:
                    retVal = retVal + fld["name"]

                ndx = ndx + 1
        except:
            self.logger.exception(self.moduleName + "- we had an error in GetListOfFields ")
            raise
        return retVal

    def GetSourceListOfFields(self, fields):
        '''
        get a list of source fields
        '''
        retVal = ""
        try:
            ndx = 0
            for fld in fields:
                if ndx > 0:
                    retVal = retVal + ","
                if fld["name"] == 'period_type':
                    retVal = retVal + """
                        case length(replace(period, 'NEW', '')) 
                               when 7 then 'M'
                               when 6 then 'Q'
                               when 4 then 'Y'
                              end period_type  """
                else:
                    if "replace" in fld:
                        retVal = retVal + "replace(" + fld["name"] + ",'" + fld["replace"]["oldValue"] + "'" +\
                                ",'" + fld["replace"]["newValue"] + "') as " + fld["name"]
                    else:
                        retVal = retVal + fld["name"]
                ndx = ndx + 1
        except:
            self.logger.exception(self.moduleName + "- we had an error in GetSourceListOfFields ")
            raise
        return retVal

    def GetInnerJoins(self, fields):
        '''
        build statement for inner joins if needed
        '''
        retVal = ""
        try:
            ndx = 0
            for fld in fields:
                if 'CXRTable' in fld:
                    retVal = retVal + " left join " + self.job["destinationSchema"] + "." + fld["CXRTable"] + " " + fld["set"] + " on " +\
                    "s." + fld["name"] + " = " + fld["set"] + ".source "
                    ndx = ndx + 1
        except:
            self.logger.exception(self.moduleName + "- we had an error in GetInnerJoins ")
            raise
        return retVal

    def AsyncLoadData(self, proc):
        '''
        load data
        '''
        filterforWorking = ""

        try:
            self.logger.debug(self.moduleName + " -- " + "AsyncLoadData  for " + proc["processname"] + " starting ")

            if 'filterFromWorking' in proc:
                filterforWorking = " where " + proc["filterFromWorking"]

            sql = "insert into " + self.job["destinationSchema"] + "." + proc["processname"]  +\
            " (" + self.GetListOfFields(proc["fields"], False) + ') ' +\
            "select "  + self.GetListOfFields(proc["fields"], True) +\
            " from ( select " + self.GetSourceListOfFields(proc["fields"]) +\
            " from " + self.job["destinationSchema"] + ".working_" + proc["processname"]  +\
            " " + filterforWorking + ") s" +\
            self.GetInnerJoins(proc["fields"])

            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)
            cur = rsConnect.cursor()
            cur.execute(sql)
            cur.close()
            rsConnect.commit()
            rsConnect.close()
            self.logger.debug(self.moduleName + " -- " + "AsyncLoadData  for " + proc["processname"] + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in AsyncLoadData ")
            raise

    def AsyncDropTables(self, proc):
        '''
        load data
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "AsyncDropTables for working_" + proc["processname"] + " starting ")
            rsTable = 'working_'+ proc["processname"]
            sql = "DROP TABLE IF EXISTS " +  self.job["destinationSchema"] + "." + rsTable + ";"

            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            cur = rsConnect.cursor()
            cur.execute(sql)
            cur.close()
            rsConnect.commit()
            rsConnect.close()
            self.logger.debug(self.moduleName + " -- " + "AsyncDropTables for working_" + proc["processname"] + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in AsyncLoadData ")
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        main routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
###
#  set up to run create folder
###
            self.fileUtilities.moduleName = self.moduleName
            self.fileUtilities.localBaseDirectory = self.localTempDirectory
            self.fileUtilities.CreateFolders(self.job["folders"])
###
            self.BuildCrossReferenceTables()
            self.PullFilesFromWeb()
            self.PushFilesToS3()
            self.UnzipFiles()
            self.PrepMultiThread("1")  ### Build Tables
            self.PrepMultiThread("2")  ### FixUnzippedFiles
            self.PrepMultiThread("3")  ### LoadFilesToRedShift
            self.LoadCrossReferences()
            self.PrepMultiThread("4")  ### LoadData
            self.PrepMultiThread("5")  ### remove working tables
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)
            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
