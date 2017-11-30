'''
Created on Jan 23, 2017

@author: VIU53188
@summary: This application will pull data from Totem and put the data into S3
'''
import os
import json
import re
import datetime
import urllib
import urllib2
from xml.dom.expatbuilder import parseString
import requests
import untangle

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase

class Totem(ApplicationBase):
    '''
    classdocs
    '''
        # pylint: disable=too-many-instance-attributes
    def __init__(self):
        super(Totem, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        self.logonCreds = {}
        self.baseUrl = None
        self.sourceTableName = None
        self.currProcId = None
        self.fromDate = None

    def GetFileList(self, dates):
        '''
        gets a list of the files available for the specific date requested
        '''
        retVal = []
        ##
        # generate the base url that we plan to use
        ##
        url = self.baseUrl +  \
            "/browse/results" + \
            "/" + dates + \
            "/files"
        ##
        # make the call
        ##
        try:
            self.logger.debug(self.moduleName + " -- " + "GetFileList" + " starting ")
            req = requests.post(url, data=self.logonCreds)
            if req.status_code == 200:
                xmldoc = parseString(req.text)
                itemlist = xmldoc.getElementsByTagName('fileName')
                for elm in itemlist:
                    for cn in elm.childNodes:
                        retVal.append(cn.nodeValue.encode())
            else:
                errorMsg = "HTTP Code: " + str(req.status_code) + " Message: " + str(req.text)
                self.logger.exception(self.moduleName + " - " + errorMsg)
                raise Exception(errorMsg)

            self.logger.debug(self.moduleName + " -- " + "GetFileList" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetFileList")
            raise
        return retVal

    def UnpackFile(self, topfolderName):
        '''
        routine to unpack the contents of a zipped file based on the extension
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "UnpackFile" + " starting ")

            ndx = 1
            while ndx != 0:
                ndx = 0
                for fExt in self.job["archiveexts"]:
                    flist = self.fileUtilities.GetListOfFiles(topfolderName + '/', fExt["name"].encode())
                    for fname in flist:
                        fnameLocation = topfolderName + '/' + fname
                        self.fileUtilities.UnzipUsing7z(fnameLocation, topfolderName)
                        self.fileUtilities.DeleteFile(fnameLocation)
                        ndx = 1
            self.logger.debug(self.moduleName + " -- " + "UnpackFile" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetFileList")
            raise

    def ProcessGzipFile(self, hstream, fName):
        '''
        since the file was in a gzip format we need:
        1) save off the file
        2) unzip the contents
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "ProcessGzipFile" + " starting ")
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + '/working')
            fLocation = self.localTempDirectory + '/working/' + fName
            localGzipfile = open(fLocation, "wb")
            dmy = localGzipfile.write(hstream) # pylint: disable=unused-variable
            localGzipfile.close()
            topfolderName = self.localTempDirectory + '/working'
            ###
            #  first time it must have a gz extention then we unpack the tar file
            ###
            self.UnpackFile(topfolderName)
            ###
            #  get a list of al the xml files
            ###
            fNames = self.fileUtilities.GetListOfFilesRecursively(topfolderName, '*.xml')
            for fn in fNames:
                fShortName = fn.rsplit('/', 1)
                self.Convert2CSV(fn, fShortName[1])
            self.logger.debug(self.moduleName + " -- " + "ProcessGzipFile" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessGzipFile")
            raise

    def GetFile(self, dte, fName):
        '''
        purpose is to find the file for a specific date and based on the parameters and
        pull that file to the local machine
        '''
        ##
        # generate the url that we plan to use
        ##
        url = self.baseUrl +  \
            "/browse/results"   + \
            "/" + dte + \
            "/files"  + \
            "/" +  fName

        formvalues = urllib.urlencode(self.logonCreds)
        ##
        # make the call
        ##
        try:
            self.logger.debug(self.moduleName + " -- " + "GetFile" + " starting : " + fName)

            fnamesplitarray = fName.rsplit('.', 1)
            fExt = fnamesplitarray[1]

            req = urllib2.Request(url, data=formvalues)
            response = urllib2.urlopen(req)
            hstream = response.read()
            if fExt == 'xml':
                self.Convert2CSV(hstream, fName)
            elif fExt == 'gz':
                self.ProcessGzipFile(hstream, fName)
            else:
                pass
            self.logger.debug(self.moduleName + " -- " + "GetFile" + " finished : " + fName)
        except Exception as ex:
            if hasattr(ex, 'reason'):
                self.logger.debug('%s - We have failed to reach the server: %s' % (self.moduleName, url))
                self.logger.debug('%s - Reason: code : %s Description: %s ' % (self.moduleName, ex.code, ex.reason)) # pylint: disable=no-member
            elif hasattr(ex, 'code'):
                self.logger.debug('%s - Failed with the following error: %s' % (self.moduleName, url))
                self.logger.debug('%s - Error Code: %s ' % (self.moduleName, ex.code)) # pylint: disable=no-member
            else:
                self.logger.debug('%s - we had an error in getFile' % (self.moduleName))
                self.logger.debug(ex)
            raise

    def GetOutputFilename(self, infileName, inExt='csv'):
        '''
        gets the file name to use
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "GetOutputFilename" + " starting ")
            fnamesplitarray = infileName.rsplit('.', 1)
            newfilename = infileName.replace(fnamesplitarray[1], inExt, 1)
            self.logger.debug(self.moduleName + " -- " + "GetOutputFilename" + " finished ")
            return newfilename
        except:
            self.logger.exception('%s - we had an error in TotemPy during GetOutputFilename' % (self.moduleName))
            raise

    def Convert2CSV(self, fstream, fname):
        '''
        converts the data aka xml to a csv file and then gzips the file
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "Convert2CSV" + " starting " + fname)
            csvfileName = self.fileUtilities.csvFolder + self.GetOutputFilename(fname, 'csv')
            obj = untangle.parse(fstream)
            if self.IterateOverObject(obj, csvfileName) is True:
                gzipfileName = self.fileUtilities.gzipFolder + self.GetOutputFilename(fname, 'csv') + '.gz'
                self.fileUtilities.GzipFile(csvfileName, gzipfileName)
            self.logger.debug(self.moduleName + " -- " + "Convert2CSV" + " finished " + fname)
        except:
            self.logger.exception('%s - we had an error in convert2CSV with %s' % (self.moduleName, fname))
            raise

    def IterateOverObject(self, jsonobj, outfile):
        '''
        this is the routine that actually creates the csv file
        '''
        # pylint: disable=too-many-nested-blocks
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-branches
        import csv
        def CheckValue(obj, name):
            '''
            checks the value to see if it is valid
            '''
            if hasattr(obj, name):
                return obj.__dict__.get(name).cdata
            return ""

        retVal = False
        try:
            self.logger.debug(self.moduleName + " -- " + "IterateOverObject" + " starting ")
            with open(outfile, 'wb') as csvfile:
                cw = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_ALL)
                l1 = []
                l2 = []
                l3 = []
                for res in jsonobj.totem.results:
                    l1 = []
                    for l1Item in self.job["level1items"]:
                        l1.append(CheckValue(res, l1Item["name"]))
                    if hasattr(res, 'energy'):
                        for egr in res.energy:
                            if hasattr(egr, 'underlier'):
                                for und in egr.underlier:
                                    l2 = []
                                    for l2item in self.job["level2items"]:
                                        l2.append(CheckValue(und, l2item["name"]))
                                    for inst in und.instrument:
                                        l3 = []
                                        for l3item in self.job["level3items"]:
                                            l3.append(CheckValue(inst, l3item["name"]))
            ###
            #  this is where you write the line
            ###
                                        row = []
                                        for part1 in l1:
                                            row.append(unicode(part1).encode("utf-8"))
                                        for part2 in l2:
                                            row.append(unicode(part2).encode("utf-8"))
                                        for part3 in l3:
                                            row.append(unicode(part3).encode("utf-8"))
                                        cw.writerow(row)
                                retVal = True
                            else:
                                self.logger.error(self.moduleName + " - iterateoverobject invalid xlm node underlier not found")
                                break
                    else:
                        self.logger.error(self.moduleName + " - iterateoverobject invalid xlm node energy not found")
                        break

        except Exception as ex:  # pylint: disable=broad-except
            self.logger.exception(self.moduleName + " - we had an error in iterateoverobject " + ex.message)
        self.logger.debug(self.moduleName + " -- " + "IterateOverObject" + " finished ")
        return retVal


    def LoadData(self):
        '''
        load the data from s3 into RedShift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " starting ")
            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])

            s3folder = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"]
            for table in self.job["tables"]:
                if "destName" in table:
                    self.sourceTableName = table["destName"]
                    RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                                     {
                                                         "destinationSchema": self.job["destinationSchema"],
                                                         "tableName": table["table"],
                                                         "s3Filename": s3folder,
                                                         "fileFormat": self.job["fileFormat"],
                                                         "dateFormat": self.job["dateFormat"],
                                                         "delimiter": self.job["delimiter"]
                                                     },
                                                     self.logger, "N")
                    self.logger.info(self.moduleName + " - Finished loading s3 data to " +\
                                     self.job["destinationSchema"] + '.' +  table["table"])
            rsConnect.close()
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in LoadData")
            raise

    def CreateUpdateScript(self, pSchema, pTable):
        '''
        takes the template for the Update sqlscript and customizes it
        '''
        sqlUpdateScript = None
        try:
            self.logger.debug(self.moduleName + " -- " + "UpDate Table Script" + " starting ")
            sqlUpdateTemplate = self.location + '/' + self.job["sqlUpdateScript"]
            sqlUpdateScript = self.localTempDirectory + "/" + re.sub('Template.sql$', '.sql', self.job["sqlUpdateScript"])
            FileUtilities.RemoveFileIfItExists(sqlUpdateScript)
            with open(sqlUpdateTemplate) as infile, open(sqlUpdateScript, 'w') as outfile:
                for line in infile:
                    line = line.replace('{schemaName}', self.job["destinationSchema"])
                    line = line.replace('{tbname}', 'Totem')
                    line = line.replace('{tbtotem}', self.sourceTableName)
                    line = line.replace('{tbstats}', pSchema + "." + pTable)
                    line = line.replace('{procid}', str(self.currProcId))
                    outfile.write(line)
            self.logger.debug(self.moduleName + " -- " + "UpDate Table Script" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in UpDate Table Script")
            raise
        return sqlUpdateScript

    def UpdateTable(self, pSchema, pTable):
        '''
        Routine to create and run the sql to create version in RedShift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "Update tables for " + pSchema + "." + pTable + " starting ")
            verScript = self.CreateUpdateScript(pSchema, pTable)
            self.logger.info(self.moduleName + " - script file name = " + verScript)
            RedshiftUtilities.PSqlExecute(verScript, self.logger)
            self.logger.debug(self.moduleName + " -- " + "Update tables for " + pSchema + "." + pTable + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in CreateVersion")
            raise

    def MoveToS3(self):
        '''
        move gzip files to s3 and clean local instance
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "MoveToS3 " + " starting ")
            ###
            #  move any gzip files to the s3 server
            ###
            s3folder = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"]
            S3Utilities.SyncFolderAWSCli(self.fileUtilities.gzipFolder,
                                         s3folder,
                                         args='''--quiet --include "*.gz"''', dbug="N")
            # Cleanup local files
            FileUtilities.EmptyFolderContents(self.fileUtilities.gzipFolder)
            self.logger.debug(self.moduleName + " -- " + "MoveToS3 " + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in MoveToS3")
            raise

    def SetUpLocalEnvironment(self):
        '''
        used to separate out process to populate local variables
        '''
        self.logonCreds = {
            'USERNAME': self.job["General"]["user"],
            'PASSWORD': self.job["General"]["pwd"]
        }
        self.baseUrl = self.job["General"]["baseurl"] + "/" +\
            self.job["DatesParams"]["service_type"] + \
            "/" + self.job["DatesParams"]["product"] +\
            "/" + self.job["DatesParams"]["service"]
###
#  set up to run create folder
###
        self.fileUtilities.moduleName = self.moduleName
        self.fileUtilities.localBaseDirectory = self.localTempDirectory
        self.fileUtilities.CreateFolders(self.job["folders"])
###
        for tblJson in self.job["tables"]:
            fname = self.fileUtilities.CreateTableSql(tblJson, self.fileUtilities.sqlFolder)
            RedshiftUtilities.PSqlExecute(fname, self.logger)

    def CleantdArray(self, lastPeriod):
        '''
        cleans up the array to process with
        '''
        tdArray = []
        prevmonth = []
        try:
            self.logger.debug(self.moduleName + " -- " + "CleantdArray " + " starting ")
            ###
            #  get a list of all date avalailable
            ###
            availableDatesArray = self.GetDates()
            numElements = len(lastPeriod)
###
#  if no value was stored in the previous run for current month then we
#  want to get the first value available
###
            if numElements == 0:  # there was not a value stored
                prevmonth = availableDatesArray[0]
            else:
                prevmonth = lastPeriod
            startDate = datetime.datetime.strptime(prevmonth, "%Y-%m")
            strcurrMonth = datetime.datetime.strftime(datetime.date.today(), "%Y-%m")
            tdArray = []
            ###
            #  clean up dates adding every date that is equal to or greater
            #  then the current month
            ###
            for dte in availableDatesArray:
                date1 = datetime.datetime.strptime(dte, "%Y-%m")
                if date1 >= startDate:
                    tdArray.append(dte.encode())

            numElements = len(tdArray)
            if numElements == 0:
                tdArray.append(strcurrMonth.encode())

            self.logger.debug(self.moduleName + " -- " + "CleantdArray " + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in CleantdArray")
            raise
        return  prevmonth, strcurrMonth, tdArray

    def GetDates(self):
        '''
        get the dates available
        '''
        ##
        #  purpose is to make a call and find all the relevant dates based on parameters
        ##
        retVal = []
        ##
        # generate the base url that we plan to use
        ##
        url = self.baseUrl +  \
            "/browse/results/dirlist"
        try:
        ##
        # make the call
        ##
            self.logger.debug(self.moduleName + " -- " + "GetDates" + " starting ")

            req = requests.post(url, data=self.logonCreds)

            if req.status_code == 200:
                xmldoc = parseString(req.text)
                itemlist = xmldoc.getElementsByTagName('directory')
                for elm in itemlist:
                    for cn in elm.childNodes:
                        retVal.append(cn.nodeValue.encode())
            else:
                errorMsg = "HTTP Code: " + str(req.status_code) + " Message: " + str(req.text)
                self.logger.exception(self.moduleName + " - " + errorMsg)
                raise Exception(errorMsg)

            self.logger.debug(self.moduleName + " -- " + "GetDates" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetDates")
            raise
        return retVal

    def Start(self, logger, moduleName, filelocs):
        '''
        main routine for Totem
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.SetUpLocalEnvironment()

            self.currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            lastRunRecJson = self.etlUtilities.GetLastGoodRun(filelocs["tblEtl"]["table"], self.moduleName)
            paramsList = []
            if lastRunRecJson is not None:
                paramsList = json.loads(lastRunRecJson["params"])
###
#  if we have run this before let's get a list of what they were so that we can use it
###
            prevmonth = []
            currmonth = []

            lenDatesArray = len(paramsList)
            if lenDatesArray > 0:
                if "currmonth" in paramsList:
                    prevmonth = paramsList["currmonth"]
###
#  check and make sure we at least process the current month
###
            prevmonth, currmonth, tdArray = self.CleantdArray(prevmonth)
            for dte in tdArray:
                ##
                #  run thru the dates and find the files associated with each date
                ##
#                if dte > '2010-05':
#                    continue
                self.logger.debug(self.moduleName + " -- " + "date processing " + dte)
                tflArray = self.GetFileList(dte)
                for fls in tflArray:
                    self.GetFile(dte, fls)
                    FileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)
                    FileUtilities.EmptyFolderContents(self.localTempDirectory + '/working')
            self.MoveToS3()
            ###
            #  now load the s3 files into Redshift
            ###
            self.LoadData()
            if self.etlUtilities.SetInstanceParameters(filelocs["tblEtl"]["table"],\
                                                  self.currProcId,\
                                                  json.dumps({"lastrun":prevmonth, "currmonth": currmonth})) is not True:
                self.logger.info(self.moduleName + " - we could not set the instance.")

            self.UpdateTable(filelocs["tblEtl"]["schemaName"], filelocs["tblEtl"]["table"])
            if self.job["cleanlocal"] == "Y":
                for fld in self.job["folders"]:
                    self.fileUtilities.CreateLocalFolder(fld)

            self.logger.info(self.moduleName + " - Finished processing.")
        except:
            self.logger.exception(moduleName + " - Exception!")
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             self.currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise
