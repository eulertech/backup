'''
Created on Nov 14, 2017

@author: Hector Hernandez
@summary: This application will pull ENP Monthly data from Shooju and load it into Athena
'''
import os
import csv
from datetime import date
import shooju

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class ENPMonthlyAthenaSpark(ApplicationBase): #pylint:disable=abstract-method
    '''
    This application will pull ENP Montly data from Shooju and load it into RedShift
    '''
    def __init__(self):
        '''
        Constructor
        '''
        super(ENPMonthlyAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def ProcessRequest(self, dbCommon):
        '''
        Gets data from shooju and creates csv file
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "ProcessRequest starting ")

            conn = shooju.Connection(server='ihs', user=dbCommon["svc_act"], api_key=dbCommon["apikey"])
            suppFields = conn.get_fields(dbCommon["supplypath"])

            cat = {}
            cat["category"] = "Supply"
            cat["frequency"] = "M"
            cat["description"] = suppFields["description"]
            cat["source"] = suppFields["source"]
            cat["unit"] = suppFields["unit"]
            sdate = date(dbCommon["startdate"]["year"], dbCommon["startdate"]["month"], dbCommon["startdate"]["day"])
            pts = conn.get_points(dbCommon["supplypath"], date_start=sdate, max_points=dbCommon["maxpoint"])

            outputFileName = self.fileUtilities.csvFolder + '/ENP_Monthlydata.csv'
            csvfile = open(outputFileName, 'wb')
            csvWriter = csv.writer(csvfile)

            #  load an array that will contain a class object that looks just like we need it for the CSV
            outRecArray = []
            for pt in pts:
                outRecArray = []
                outRecArray.append(cat["category"])
                outRecArray.append(cat["frequency"])
                outRecArray.append(cat["description"])
                outRecArray.append(cat["source"])
                outRecArray.append(cat["unit"])
                outRecArray.append(pt.date)
                outRecArray.append(pt.value)
                csvWriter.writerow(outRecArray)

            self.logger.debug(self.moduleName + " -- " + "ProcessRequest starting ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessRequest")
            raise
        finally:
            csvfile.close()
        return outputFileName

    def ProcessTables(self, dbCommon, tables):
        '''
        Will load the ENP Monthly Table
        '''
        try:
            outputFileName = self.ProcessRequest(dbCommon)

            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            schema = SparkUtilities.BuildSparkSchema(tables)
            df = (spark.read
                  .format("com.databricks.spark.csv")
                  .options(header='false', delimiter=self.job["delimiter"])
                  .schema(schema)
                  .load(outputFileName)
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
