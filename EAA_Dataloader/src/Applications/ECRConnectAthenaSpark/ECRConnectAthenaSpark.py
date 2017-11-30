'''
Created on Nov 13, 2017

@author: Hector Hernandez
@summary: Extracts the risks values from the IHS Connect API.

'''
import os
import urllib2
import base64
import json

from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class ECRConnectAthenaSpark(ApplicationBase): #pylint:disable=abstract-method
    '''
    This class is used to get the Risk data from IHS Connect, transform it and load it into Redshift.
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(ECRConnectAthenaSpark, self).__init__()

        self.jDataPulled = False
        self.xRefPulled = False
        self.fileUtilities = FileUtilities(self.logger)
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def DownloadData(self, dbCommon):
        '''
        Download the Risks JSON data from Connect's API.
        '''
        try:
            if self.jDataPulled is False:
                request = urllib2.Request(dbCommon["baseurl"] + dbCommon["riskService"])
                base64string = base64.b64encode('%s:%s' % (dbCommon["username"], dbCommon["password"]))
                request.add_header("Authorization", "Basic %s" % base64string)

                response = urllib2.urlopen(request)
                jData = json.load(response)

                for countryNode in jData:
                    for risk in countryNode["Risks"]:
                        riskCurrent = {}
                        riskCurrent["country"] = countryNode["Country"]
                        riskCurrent["risk_name"] = risk["Name"]
                        riskCurrent["risk_value"] = risk["Value"]
                        riskCurrent["risk_description"] = risk["Description"]
                        riskCurrent["risk_class"] = ""
                        riskCurrent["risk_class_avg"] = ""
                        riskCurrent["updated_on"] = risk["UpdatedOn"]

                        with open(self.localTempDirectory + "/raw/ecr_risks_current.json", "a+") as outFile:
                            outFile.write('%s\n' % (json.dumps(riskCurrent)))

                        for riskH in risk["History"]:
                            riskHistory = {}
                            riskHistory["country"] = countryNode["Country"]
                            riskHistory["risk_name"] = risk["Name"]
                            riskHistory["risk_value"] = riskH["Value"]
                            riskHistory["updated_on"] = riskH["UpdatedOn"]

                            with open(self.localTempDirectory + "/raw/ecr_risks_history.json", "a+") as outFile:
                                outFile.write('%s\n' % (json.dumps(riskHistory)))

                self.jDataPulled = True
        except Exception as err:
            self.logger.error("Error while trying to get and transform from IHS Connect API service. Error:" + err.message)
            raise

    def LoadClassRefDF(self, spark):
        '''
        Loads de class reference data
        '''
        xReferencesDF = {}

        for catalog in self.job["catalogs"]:
            if catalog["name"] == "xReferences":
                for xrefTable in catalog["tables"]:
                    if self.xRefPulled is False:
                        S3Utilities.CopyItemsAWSCli("s3://" + self.job["bucketName"] + xrefTable["s3SourceFolder"] + xrefTable["sourceFileName"],
                                                    self.fileUtilities.csvFolder,
                                                    "--quiet")

                    xReferencesDF[xrefTable["table"]] = SparkUtilities.ReadCSVFile(spark, xrefTable, self.job["delimiter"], False,
                                                                                   self.fileUtilities.csvFolder + "/" + xrefTable["sourceFileName"],
                                                                                   self.logger)

        self.xRefPulled = True
        return xReferencesDF

    def ProcessCatalogs(self, dbCommon, catalog):
        '''
        Process each risks table.
        '''
        try:
            if catalog["name"] == "Risks":
                self.logger.debug(self.moduleName + " -- " + "Processing data for catalog: " + catalog["name"])
                self.DownloadData(dbCommon)

                for tableDDL in catalog["tables"]:
                    dfFixed = None

                    spark = SparkUtilities.GetCreateSparkSession(self.logger)
                    xReferencesDF = self.LoadClassRefDF(spark)

                    xReferencesDF["class_xref"].createOrReplaceTempView("class_xref")
                    xReferencesDF["iso3166_xref"].createOrReplaceTempView("iso3166_xref")

                    dfMaster = spark.read.json(self.localTempDirectory + "/raw/ecr_risks_" + tableDDL["type"] +".json")

                    if tableDDL["type"] == "current":
                        dfMaster.createOrReplaceTempView("risks")
                        dfFixed = spark.sql("""
                                                SELECT iso3166_xref.countryname AS country, clsRef.risk_desc AS risk_name,
                                                       CAST(risks.risk_value AS DOUBLE) AS risk_value, risks.risk_description, clsRef.class_name AS risk_class,
                                                       avgs.class_avg AS risk_class_avg, risks.updated_on
                                                FROM risks 
                                                    inner join iso3166_xref on iso3166_xref.iso3166 = risks.country
                                                    inner join class_xref clsRef on clsRef.risk_name = risks.risk_name
                                                    inner join (SELECT country, risk_name, risk_class, AVG(risk_value) 
                                                                OVER(PARTITION BY country, risk_class) AS class_avg FROM risks) avgs
                                                        ON avgs.country = risks.country
                                                           AND avgs.risk_name = risks.risk_name
                                                           AND avgs.risk_class = risks.risk_class
                                                """)
                    else:
                        dfMaster.createOrReplaceTempView("risksHistory")
                        dfFixed = spark.sql("""
                                                SELECT iso3166_xref.countryname AS country, clsRef.risk_desc AS risk_name,
                                                       CAST(risksHistory.risk_value AS DOUBLE) AS risk_value, risksHistory.updated_on
                                                FROM risksHistory 
                                                    inner join iso3166_xref on iso3166_xref.iso3166 = risksHistory.country
                                                    inner join class_xref clsRef on clsRef.risk_name = risksHistory.risk_name
                                                """)

                    self.logger.info(self.moduleName + " -- " + "Done reading " + str(dfFixed.count()) + " rows.  Now saving as parquet file...")
                    SparkUtilities.SaveParquet(dfFixed, self.fileUtilities)
                    self.UploadFilesCreateAthenaTablesAndSqlScripts(tableDDL, self.fileUtilities.parquet)
                    self.LoadDataFromAthenaIntoRedShiftLocalScripts(tableDDL)

                    spark.catalog.dropTempView("class_xref")
                    spark.catalog.dropTempView("iso3166_xref")
                    spark.catalog.dropTempView("risks")
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to load table. Error: " + err.message)
            raise

    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)