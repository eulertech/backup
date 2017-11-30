import pandas as pd
import numpy as np
import time
import json
import psycopg2
import pyodbc

#do not show warning on joining in Pandas
pd.options.mode.chained_assignment = None  # default='warn'

class HindsightQC:
    def __init__(self, logger, fileUtilities, bcpUtilities, localTempDirectory):
        self.starttime = time.time()
        self.stoptime = None
        self.logger = logger
        self.config = None
        self.redshiftConnection = None
        self.sqlserverConnection = None   
        self.fileUtilities = fileUtilities
        self.bcpUtilities = bcpUtilities 
        self.localTempDirectory = localTempDirectory
        self.LoadConfig() #load the configuration files
        self.strftime = '%Y-%m-%d' #load this from config file        
        self.qcfolder = self.localTempDirectory + self.config["QCFolder"] #
        #self.qclogfile = self.qcfolder + time.strftime(self.strftime) + "_querylog.log"
        #self.CreateBasicLogger()

    def GetDate(self):
        return time.strftime(self.strftime)

    def TimeElaspsed(self):
        return time.time() - self.starttime

    #loads the configuration file and assigns to a class variable
    def LoadConfig(self):
        with open(self.fileUtilities.GetApplicationDirectory("Hindsight") + "QCconfig.json", "r") as configfile:
            self.config = json.load(configfile)            

    def CreateBasicLogger(self):
        self.logger.basicConfig(format='%(asctime)s %(message)s', 
                            datefmt='%m/%d/%Y %I:%M:%S %p',
                            level=self.logger.DEBUG,
                            filename=self.qclogfile)
        self.logger = self.logger.getLogger('Hindsight QC')        

    def GetFilePrefix(self, timing):
        return self.qcfolder + self.GetDate() + "_" + timing + "_"        

    def GetRedshiftConnection(self):
        self.logger.info("Connecting to Redshift")                     
        try:
            self.redshiftConnection=psycopg2.connect(dbname=self.config["Redshift"]["ConnectionInfo"]['dbname'], 
                                                     host=self.config["Redshift"]["ConnectionInfo"]['host'], 
                                                     port=self.config["Redshift"]["ConnectionInfo"]['port'], 
                                                     user=self.config["Redshift"]["ConnectionInfo"]['user'], 
                                                     password=self.config["Redshift"]["ConnectionInfo"]['pwd'])
        except Exception as err:
            print(err)

    def GetSQLServerConnection(self): 
        self.logger.info("Connecting to SQL Server")        
        try:
            self.sqlserverConnection = pyodbc.connect(self.config["SQLServer"]["ConnectionInfo"])
        except:
            self.logger.exception("Error in getting connected to the Cloud SQL Server")
            raise

    def CloseSQLServerConnection(self):
        self.logger.info("Closing SQL Server Connection")
        try:
            self.sqlserverConnection.close()
        except:
            self.logger.exception("Error while closing SQL Server Connection")
            raise

    def CloseRedshiftConnection(self):
        self.logger.info("Closing Redshift Connection")
        try:
            self.redshiftConnection.close()
        except:
            self.logger.exception("Error while closing Redshift Connection")
            raise

    def GetTheCounts(self, timing, subJob):
        self.bcpUtilities.RunBCPJob(self.config["SQLServer"]["sqlServerloginInfo"],   #sql server login info                                         
                                    self.config["SQLServer"]["bcpUtilityDirOnLinux"], #bcp client on linux machine
                                    self.fileUtilities.LoadSQLQuery(self.fileUtilities.GetApplicationDirectory("Hindsight") + subJob["InputQuery"]), #input query                                       
                                    self.GetFilePrefix(timing) + subJob["OutputFile"]) #output file                                 

    def Get_sql_server_rowcounts(self, timing):
        self.logger.info("Fetching: " + self.GetFilePrefix(timing))

        try:
            self.GetTheCounts(timing, self.config["RowCounts"]["SQLServer"]) #rowcounts grouped by series_id

            df = self.GetDF(self.GetFilePrefix(timing) + self.config["RowCounts"]["SQLServer"]["OutputFile"],
                            "series_id,series_row_cnt,Date,timestamp", #columns
                            str(self.config["delimiter"])) #delimiter   

            df.to_csv(path_or_buf=self.GetFilePrefix(timing) + self.config["RowCounts"]["SQLServer"]["OutputFile"],
                      sep=str(self.config["delimiter"]),
                      na_rep="",
                      header=True,
                      index=False)
        except:
            self.logger.exception("Error in getting: " + self.GetFilePrefix(timing))
            raise

    def Get_redshift_rowcounts(self, timing):
        table = self.config["Redshift"]["tables"]["post"] #"hindsight_etl.stg_series_data_cleaned" #post ETL               

        self.logger.info("Fetching: " + self.GetFilePrefix(timing) + table)

        try:
            self.GetRedshiftConnection()
            #TODO: load the query from a file
            df = pd.read_sql("select series_id, count(series_id) as row_count, GETDATE() as date from " + table + " GROUP BY series_id", con=self.redshiftConnection)

            self.redshiftConnection.close()            
            df.to_csv(path_or_buf=self.GetFilePrefix(timing) + self.config["RowCounts"]["Redshift"]["OutputFile"],
                      sep=str(self.config["delimiter"]),
                      na_rep="",
                      header=True,
                      index=False)
        except:
            self.logger.exception("Error: " + self.GetFilePrefix(timing))
            raise

    def Get_Inter_version_rowcounts(self):
        self.logger.info("Getting Inter-version rowcount mismatches ")

        try:
            self.GetRedshiftConnection()            
            query = self.fileUtilities.LoadSQLQuery(self.fileUtilities.GetApplicationDirectory("Hindsight") + str(self.config["RowCounts"]["Comparison"]["InterVersion"]["InputQuery"])) #input query

            df = pd.read_sql(query, con=self.redshiftConnection)

            self.redshiftConnection.close()
        except:
            self.logger.exception("Error getting Inter-version rowcount mismatches")
            raise

        return df

    def GetDF(self, path, colstr, delimiter):
        self.logger.info("Reading file " + path)
        df = pd.read_csv(path, names=colstr.split(","), header=0, sep=delimiter) #reads from the csv into a dataframe
        self.logger.info("Done reading file " + path)
        return df

    def CompareSeriesIdRowCounts(self, timing):        
        self.logger.info("Comparing files " + self.GetFilePrefix(timing) + self.config["RowCounts"]["Redshift"]["OutputFile"] + " and "  + self.GetFilePrefix(timing) + self.config["RowCounts"]["SQLServer"]["OutputFile"])        
        #TODO: Get list of columns from config file        
        redshift = self.GetDF(self.GetFilePrefix(timing) + self.config["RowCounts"]["Redshift"]["OutputFile"],
                   "series_id_red,row_count_red,date_red",
                   str(self.config["delimiter"])).set_index(["series_id_red"])        

        #TODO: Get list of columns from config file
        #Always compare with pre-etl output from SQL Server
        sql = self.GetDF(self.GetFilePrefix("pre") + self.config["RowCounts"]["SQLServer"]["OutputFile"],               
                   "series_id_sql,row_count_sql,date_sql,timestamp",
                   str(self.config["delimiter"])).set_index(["series_id_sql"])

        #combined = pd.concat([redshift, sql], axis=1, join='outer', ignore_index=False)

        merged = sql.merge(redshift, how='outer', left_index=True, right_index=True, indicator=True)
        merged["difference"] = abs(merged["row_count_sql"] - merged["row_count_red"])
        merged["diff_ratio"] = merged["difference"] / np.minimum(merged["row_count_sql"], merged["row_count_red"]) * 100

        return merged

    def SaveAndLog(self, timing, seriesid_rowcounts):        
        self.logger.info("Total rows in SQL Server before ETL: " + str(int(np.sum(seriesid_rowcounts.row_count_sql))))
        self.logger.info("Total rows in Redshift after ETL: " + str(int(np.sum(seriesid_rowcounts.row_count_red))))
        self.logger.info("Row count difference b/w SQL Server & Redshift: " + str(abs(int(np.sum(seriesid_rowcounts.row_count_sql)) - int(np.sum(seriesid_rowcounts.row_count_red)))))
        self.logger.info("No of rows in sql that got removed from sql while the etl was running: " + str(int(np.nan_to_num(np.sum(seriesid_rowcounts[seriesid_rowcounts._merge=="left_only"].row_count_sql)))))
        self.logger.info("No of rows in sql that got added while the etl was running: " + str(int(np.nan_to_num(np.sum(seriesid_rowcounts[seriesid_rowcounts._merge=="right_only"].row_count_red)))))
        self.logger.info("No of seriesids that got removed from sql while the etl was running: " + str(len(seriesid_rowcounts[seriesid_rowcounts._merge == "left_only"])))
        self.logger.info("No of seriesids that got added while the etl was running: " + str(len(seriesid_rowcounts[seriesid_rowcounts._merge == "right_only"])))
        self.logger.info("No of seriesids with rowcount matches: " + str(len(seriesid_rowcounts[(seriesid_rowcounts._merge == "both") & (seriesid_rowcounts.difference == 0)])))
        self.logger.info("No of seriesids with rowcount mismatches: " + str(len(seriesid_rowcounts[(seriesid_rowcounts._merge == "both") & (seriesid_rowcounts.difference != 0)])))
        self.logger.info("No of seriesids with rowcount mismatches beyond acceptable threshold of 50%: " + str(len(seriesid_rowcounts[seriesid_rowcounts.diff_ratio >= 50])))

        mismatches = seriesid_rowcounts[(seriesid_rowcounts._merge == "both") & (seriesid_rowcounts.difference != 0)]        
        mismatches.to_csv(self.GetFilePrefix(timing) + self.config["RowCounts"]["Comparison"]["InterSystem"]["OutputFile"], 
                          header=False, 
                          sep=str(self.config["delimiter"]), 
                          index=True,                       
                          quoting=None)
        #TODO: Get list of columns from config file        
        mismatches = self.GetDF(self.GetFilePrefix(timing) + self.config["RowCounts"]["Comparison"]["InterSystem"]["OutputFile"],
                                "series_id,row_count_red,date_red,row_count_sql,date_sql,timestamp,difference",
                                str(self.config["delimiter"]))

        mismatches.to_csv(self.GetFilePrefix(timing) + self.config["RowCounts"]["Comparison"]["InterSystem"]["OutputFile"], 
                          header=True, 
                          sep=str(self.config["delimiter"]), 
                          index=False,                       
                          quoting=None)

    def ValidateETL(self): 
        IsAllOk = True

        timing = "post" #Comparison always happens after the ETL        
        seriesid_rowcounts = self.CompareSeriesIdRowCounts(timing)    
        self.SaveAndLog(timing, seriesid_rowcounts)

        #Set ETL as no go if the rowcount difference is beyond allowed threshold
        if len(seriesid_rowcounts[seriesid_rowcounts.diff_ratio >= 50]) > 0:
            IsAllOk = False

        #Get rowcount mismatch beyond the threshold from hindsight_prod.row_count_by_seriesid
        interversion_rowcounts = self.Get_Inter_version_rowcounts()
        self.logger.info("No of seriesids with prior version rowcount mismatches beyond acceptable threshold of 50%: " + str(len(interversion_rowcounts.series_id)))

        #create ETL report
        self.CreateReport(timing, seriesid_rowcounts, interversion_rowcounts)

        if len(interversion_rowcounts.series_id) > 0:
            IsAllOk = False

        self.logger.info("Is ETL OK to go: " + str(IsAllOk))

        return IsAllOk 

    def CreateReport(self, timing, seriesid_rowcounts, interversion_rowcounts):
        report_file = self.qcfolder + self.GetDate() + "_etl_report.csv"

        with open(report_file, "w") as rf:
            rf.write("\"Total rows in SQL Server before ETL:\",{:d}\n".format(int(np.sum(seriesid_rowcounts.row_count_sql))))
            rf.write("\"Total rows in Redshift after ETL:\",{:d}\n".format(int(np.sum(seriesid_rowcounts.row_count_red))))
            rf.write("\"Row count difference b/w SQL Server & Redshift:\",{:d}\n".format(abs(int(np.sum(seriesid_rowcounts.row_count_sql)) - int(np.sum(seriesid_rowcounts.row_count_red)))))
            rf.write("\"No. of rows in sql that got removed from sql while the etl was running:\",{:d}\n".format(int(np.nan_to_num(np.sum(seriesid_rowcounts[seriesid_rowcounts._merge=="left_only"].row_count_sql)))))
            rf.write("\"No. of rows in sql that got added while the etl was running:\",{:d}\n".format(int(np.nan_to_num(np.sum(seriesid_rowcounts[seriesid_rowcounts._merge=="right_only"].row_count_red)))))
            rf.write("\"No. of seriesids that got removed from sql while the etl was running:\",{:d}\n".format(len(seriesid_rowcounts[seriesid_rowcounts._merge == "left_only"])))
            rf.write("\"No. of seriesids that got added while the etl was running:\",{:d}\n".format(len(seriesid_rowcounts[seriesid_rowcounts._merge == "right_only"])))
            rf.write("\"No. of seriesids with rowcount matches:\",{:d}\n".format(len(seriesid_rowcounts[(seriesid_rowcounts._merge == "both") & (seriesid_rowcounts.difference == 0)])))
            rf.write("\"No. of seriesids with rowcount mismatches:\",{:d}\n".format(len(seriesid_rowcounts[(seriesid_rowcounts._merge == "both") & (seriesid_rowcounts.difference != 0)])))
            rf.write("\"No. of seriesids with rowcount mismatches beyond acceptable threshold of 50%:\",{:d}\n".format(len(seriesid_rowcounts[seriesid_rowcounts.diff_ratio >= 50])))
            rf.write("\"No. of seriesids with prior version rowcount mismatches beyond acceptable threshold of 50%:\",{:d}\n".format(len(interversion_rowcounts.series_id)))
