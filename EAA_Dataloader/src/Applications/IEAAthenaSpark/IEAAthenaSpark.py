'''
Main script to process the IEA data
Author - Thomas Coffey.  Rewritten by Christopher Lewis
License: IHS - not to be used outside the company
'''
import os
import re
import ntpath
import pandas
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class IEAAthenaSpark(ApplicationBase):
    '''
    Code to process the Auto Light Vehicles data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(IEAAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    def DownDataFiles(self, dbCommon):
        '''
        Download all the file from the web and unzip them
        '''
        user = dbCommon["username"]
        password = dbCommon["password"]
        # Download all the files in one go
        for item in dbCommon["files"]:
            url = item["url"]
            fname = ntpath.basename(url)
            zipFileName = self.fileUtilities.gzipFolder + fname
            FileUtilities.DownloadFromURLUserPassword(url, zipFileName, user, password)
            # Unzip contents in separate folder
            unzipFolder = self.fileUtilities.gzipFolder + fname.split('.')[0] + "/"
            self.fileUtilities.UnzipFile(zipFileName, unzipFolder)
    
    def ProcessFiles(self, tables):
        '''
        fix the files that were unzipped
        '''
        try:
            #  the contents of the file needs to have the white spaces condensed and then make
            #  each element comma separated so that we can load it in to the working tables
            navalues = None
            for fileSubpath in tables["inputFiles"]:
                localFilepath = self.fileUtilities.gzipFolder + fileSubpath
                self.logger.info(self.moduleName + " - Processing file: " + localFilepath)
                
                scrubbedFilepath = self.fileUtilities.csvFolder + fileSubpath.replace('/', '_')
                with open(localFilepath) as infile, open(scrubbedFilepath, 'w') as outfile:
                    for line in infile:
                        line = line.strip()
                        line = re.sub('\\s+', ',', line)
                        outfile.write(line + '\n')

                if 'pandas_replace' in tables:
                    if fileSubpath == tables['pandas_replace']['processfile']:
                        if 'na_values' in tables['pandas_replace']:
                            navalues = tables['pandas_replace']['na_values']
                        df = pandas.read_csv(scrubbedFilepath,
                                             sep=self.job['delimiter'],
                                             names=tables['pandas_replace']['columnNames'],
                                             na_values=navalues)
                        df = df[tables['pandas_replace']['usecolumnNames']]
                        df.to_csv(scrubbedFilepath, header=False, sep=str(self.job["delimiter"]), encoding='utf-8', index=False)

                self.logger.info(self.moduleName + " - Done processing file: " + localFilepath)
        except:
            self.logger.exception(self.moduleName + "- we had an error in AsyncFixUnzippedFiles")
            raise

    def ProcessTables(self, dbCommon, tables):
        '''
        Process each file
        '''
        self.logger.debug(self.moduleName + " -- ProcessTables for  " + tables["table"] + " starting")
        FileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)   # Clear the folder from the previous run
        self.ProcessFiles(tables)
        spark = SparkUtilities.GetCreateSparkSession(self.logger)
        
        # We will compute "period_type" later
        schemaWithoutPeriodType = SparkUtilities.BuildSparkSchema(tables, excludeComputed=True)
        df = (spark.read
                .format("com.databricks.spark.csv")
                .options(header=False, delimiter=self.job['delimiter'],
                         ignoreTrailingWhiteSpace=True, ignoreLeadingWhiteSpace=True)
                .schema(schemaWithoutPeriodType)
                .load(self.fileUtilities.csvFolder)
            )        

        if "filterData" in tables:
            df = df.filter(tables["filterData"])
        
        # Replace "NEW" with blank.  E.g. DEC1990NEW to DEC1990
        from pyspark.sql import functions as F  #@UnresolvedImport
        df = SparkUtilities.RenameColumnsInList(df, [("period", "period_old")]) # Rename column since we cannot edit in place
        df = df.withColumn("period", F.regexp_replace(df["period_old"], "NEW", ""))

        # Compute "period_type".  Following simple rules have been applied
        #    MAY2013 - 7 characters so assumed to be 'M'
        #    Q12017  - 6 characters so assumed to be 'Q'
        #    2017    - 4 characters so assumed to be 'Y'
        df = df.withColumn("period_type", F.when(F.length(df.period)==7, "M").when(F.length(df.period)==6, "Q").when(F.length(df.period)==4, "Y").otherwise(""))
        
        # Reorder the columns based on the input column order
        schema = SparkUtilities.BuildSparkSchema(tables)
        df = df.select(schema.names)
        
        SparkUtilities.SaveParquet(df, self.fileUtilities)
        self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
        self.LoadDataFromAthenaIntoRedShiftS3Scripts(tables)
        self.logger.debug(self.moduleName + " -- ProcessTables for  " + tables["table"] + " finished")

    def ProcessCatalogs(self, dbCommon, catalog):
        '''
        Pull the data first and then process each table
        '''
        self.DownDataFiles(dbCommon)
        for tables in catalog["tables"]:
            self.ProcessTables(dbCommon, tables)
        
    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)