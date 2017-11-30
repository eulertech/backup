'''
Created on Feb 2, 2017

@author: Hector Hernandez
@summary: This application will pull ENP data and load it into Athena
'''
import os
import pypyodbc

from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class ENPYearlyAthenaSpark(ApplicationBase): #pylint:disable=abstract-method
    '''
    This class is used to get the ENP data from IHS Connect, transform it and load it into Athena.
    '''

    def __init__(self):
        '''
        Constructor
        '''
        super(ENPYearlyAthenaSpark, self).__init__()
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

    def GetConnectionString(self, dbCommon):
        '''
        get the connection String
        '''
        from os.path import abspath
        try:
            self.logger.debug(self.moduleName + " -- " + "GetConnectionString" + " starting ")
            if dbCommon["accessdbname"].endswith('.accdb'):
                driver = 'Microsoft Access Driver (*.mdb, *.accdb)'
            else:
                driver = 'Microsoft Access Driver (*.mdb)'

            dbFile = dbCommon["dblocation"] + "/" + dbCommon["accessdbname"]
            connString = 'DRIVER=%s;DBQ=%s;ExtendedAnsiSQL=1' % (driver, abspath(dbFile))
            self.logger.debug(self.moduleName + " -- " + "GetConnectionString" + " finished ")
            return connString
        except Exception as ex:
            self.logger.debug("we had an error in ENP during GetConnectionString")
            self.logger.debug(ex)
            raise ex

    def EstablishConnection(self, dbCommon):
        '''
        returns a connection object
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "EstablishConnection" + " starting ")
            connstr = self.GetConnectionString(dbCommon)
            self.logger.debug(connstr)
            pypyodbc.lowercase = False
            conn = pypyodbc.connect(connstr)
            self.logger.debug(self.moduleName + " -- " + "EstablishConnection" + " finished ")
            return conn
        except Exception as ex:
            self.logger.debug("we had an error in ENP during EstablishConnection")
            self.logger.debug(ex)
            raise ex

    def FixSQLStatement(self, dbCommon):
        '''
        adjust the sql string with correct tables
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "FixSQLStatement" + " starting ")
            sqlline = self.sql.replace('{table}', dbCommon["accessTable"])
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

    def ProcessTables(self, dbCommon, tables):
        '''
        Will load the ENP Yearly Table
        '''
        try:
            outputfileName = self.fileUtilities.csvFolder + '/ENPdata.csv'

            conn = self.EstablishConnection(dbCommon)
            cur = conn.cursor()
            sqlline = self.FixSQLStatement(dbCommon)
            cur.execute(sqlline)

            self.ConvertToCSV(cur, outputfileName)

            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            schema = SparkUtilities.BuildSparkSchema(tables)
            df = (spark.read
                  .format("com.databricks.spark.csv")
                  .options(header='false', delimiter=self.job["delimiter"])
                  .schema(schema)
                  .load(outputfileName)
                 )
            self.logger.info(self.moduleName + " -- " + "Done reading " + str(df.count()) + " rows.  Now saving as parquet file...")
            SparkUtilities.SaveParquet(df, self.fileUtilities)
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
            self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to load table. Error:" + err.message)
            raise

    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)