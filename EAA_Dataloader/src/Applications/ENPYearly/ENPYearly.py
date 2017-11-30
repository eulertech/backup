'''
Created on Feb 2, 2017

@author: VIU53188
@summary: This application will pull ENP data and load it into RedShift
        1)  take in the json config string from the main routine this contains the common values {baseurl, username, password}
        2)  logger so that all log entries are put in the same place
        3)  Read the access file from a specific location
        4)  convert access table identified in the config file to a csv file
        5)  load the CSV file into RedShift
'''
import os
import pypyodbc

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class ENPYearly(ApplicationBase):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        super(ENPYearly, self).__init__()
        self.location = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        self.params = ""
        self.sql = """
        SELECT [Data Level], Region, Country, 
        IIF(IsNull([Iris ID]),[Iris ID],CDbl([Iris ID])),
        [Crude Stream],[Project ID], [Project Name], Hydrocarbon, Basin, Subbasin, Operator, 
        [Start Year], Terrain, Sanctioned, Omit, 
        [Latitude Dec Deg], 
        [Longitude Dec Deg],
        IsNull(BEP), Tranches, Years,
        IIF(IsNull([Production (Kbbl/d)]),[Production (Kbbl/d)],CStr([Production (Kbbl/d)])),
        IIF(IsNull([Prod 2015-2040 (MMbbl)]),[Prod 2015-2040 (MMbbl)],CStr([Prod 2015-2040 (MMbbl)])),
        IIF(IsNull([Produced to date (MMbbl)]),[Produced to date (MMbbl)],CStr([Produced to date (MMbbl)]))
         from {table}
         """
    def GetConnectionString(self):
        '''
        get the connection String
        '''
        from os.path import abspath
        try:
            self.logger.debug(self.moduleName + " -- " + "GetConnectionString" + " starting ")
            if self.job["accessdbname"].endswith('.accdb'):
                driver = 'Microsoft Access Driver (*.mdb, *.accdb)'
            else:
                driver = 'Microsoft Access Driver (*.mdb)'

            dbFile = self.job["dblocation"] + "/" + self.job["accessdbname"]
            connString = 'DRIVER=%s;DBQ=%s;ExtendedAnsiSQL=1' % (driver, abspath(dbFile))
            self.logger.debug(self.moduleName + " -- " + "GetConnectionString" + " finished ")
            return connString
        except Exception as ex:
            self.logger.debug("we had an error in ENP during GetConnectionString")
            self.logger.debug(ex)
            raise ex

    def EstablishConnection(self):
        '''
        returns a connection object
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "EstablishConnection" + " starting ")
            connstr = self.GetConnectionString()
            self.logger.debug(connstr)
            pypyodbc.lowercase = False
            conn = pypyodbc.connect(connstr)
            self.logger.debug(self.moduleName + " -- " + "EstablishConnection" + " finished ")
            return conn
        except Exception as ex:
            self.logger.debug("we had an error in ENP during EstablishConnection")
            self.logger.debug(ex)
            raise ex

    def FixSQLStatement(self):
        '''
        adjust the sql string with correct tables
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "FixSQLStatement" + " starting ")
            sqlline = self.sql.replace('{table}', self.job["accessTable"])
            self.logger.debug(self.moduleName + " -- " + "FixSQLStatement" + " finished ")
            return sqlline
        except Exception as ex:
            self.logger.debug("we had an error in ENP during FixSQLStatement")
            self.logger.debug(ex)
            raise ex

    @staticmethod
    def OutputCol(col):
        '''
        make sure we can report the correct values
        '''
        retVal = ''
        if col:
            if isinstance(col, unicode):
                retVal = col.encode('utf-8')
            elif isinstance(col, float):
                if cmp(col, int(col)) == 0:
                    retVal = int(col)
                else:
                    retVal = col
            else:
                retVal = col
        return retVal

    def ConvertToCSV(self, curr, outfile):
        '''
        convert cursor to csv file
        '''
        import csv
        recNdx = 1

        try:
            self.logger.debug(self.moduleName + " -- " + "ConvertToCSV to " + outfile + " starting ")
            self.logger.debug(" ENP start getting all records at one time")
            row = curr.fetchone()
            #rows = _curr.fetchall()
            self.logger.debug(" ENP records have been fetched")
            with open(outfile, 'wb',) as fl:
                cFile = csv.writer(fl, delimiter=',')
                while True:
                    if row is None:
                        break
                    cFile.writerow(map(self.OutputCol, row))
                    row = curr.fetchone()
                    recNdx += 1
            self.logger.debug(self.moduleName + " -- " + "ConvertToCSV to " + outfile + " finished ")
        except Exception as ex:
            self.logger.debug("we had an error in ENP during converttoCSV")
            self.logger.debug(ex)
            raise ex

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
    #  establish connection to Access database
    ###
            conn = self.EstablishConnection()
            cur = conn.cursor()
            sqlline = self.FixSQLStatement()
            cur.execute(sqlline)

            outputfileName = self.localTempDirectory + '/ENPdata.csv'
            self.ConvertToCSV(cur, outputfileName)
###
#  load the CSV to RedShift
###
            self.logger.debug(self.moduleName + " - ENP load CSV to RedShift")

            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            RedshiftUtilities.LoadFileIntoRedshift(rsConnect,
                                                   self.awsParams.s3,
                                                   self.logger,
                                                   self.fileUtilities,
                                                   outputfileName, self.job["destinationSchema"],
                                                   self.job["tableName"],
                                                   self.job["fileFormat"],
                                                   self.job["dateFormat"],
                                                   self.job["delimiter"])

            self.logger.debug(self.moduleName + " - ENP CSV loaded to RedShift")

            # Cleanup
            rsConnect.close()
            cur.close()
            conn.close()
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)
            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
