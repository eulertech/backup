'''
# Spark Utilties
@author: Christopher Lewis
@license: IHS - not to be used outside the company
@change: added new method to get a list of file and modified for coding standards
'''
import glob
import shutil
import findspark
class SparkUtilities(object):
    '''
    Utilities for working with Spark objects
    '''
    @staticmethod
    def CreateSparkContext(logger):
        '''
        Create a spark context
        '''
        findspark.init()
        import pyspark #@UnresolvedImport
        from pyspark.sql import SQLContext #@UnresolvedImport
        logger.debug("SparkUtilities -- " + "pyspark Imported.")

        conf = pyspark.SparkConf().setAppName("LocalSparkCluster")
        conf = (conf
                .setMaster("local[*]")
                #.setMaster("spark://10.45.89.69:7077")
                .set("spark.driver.memory", "4G")
               )
        sc = pyspark.SparkContext(conf=conf)
        sqlContext = SQLContext(sc)

        logger.debug("SparkUtilities -- " + "SparkContext context created.")
        return sc, sqlContext

    @staticmethod
    def GetCreateSparkSession(logger):
        '''
        Create a spark context
        '''
        findspark.init()
        from pyspark.sql import SparkSession #@UnresolvedImport
        logger.debug("SparkUtilities -- " + "pyspark Imported.")

        spark = (SparkSession.builder
                 .master("local[*]")
                 .appName("LocalSparkCluster")
                 .config("spark.driver.memory", "4G")
                 .getOrCreate()
                 )
        
        logger.debug("SparkUtilities -- " + "SparkSession acquired.")
        return spark

    @staticmethod
    def BuildSparkSchemaJson(table, forceAllFieldsToString=False, useValidation=False, excludeComputed=False):
        '''
        Build the Spark Schema based on the table fields
        http://spark.apache.org/docs/2.2.0/api/python/_modules/pyspark/sql/types.html
        _acceptable_types = {
            BooleanType: (bool,),
            ByteType: (int, long),
            ShortType: (int, long),
            IntegerType: (int, long),
            LongType: (int, long),
            FloatType: (float,),
            DoubleType: (float,),
            DecimalType: (decimal.Decimal,),
            StringType: (str, unicode),
            BinaryType: (bytearray,),
            DateType: (datetime.date, datetime.datetime),
            TimestampType: (datetime.datetime,),
            ArrayType: (list, tuple, array),
            MapType: (dict,),
            StructType: (tuple, list, dict),
        }

        '''
        schemaJson = {}
        schemaJson['fields'] = []
        schemaJson['type'] = 'struct'
        for fld in table["fields"]:
            # Skip the field if it is for Athena management
            if "athenaOnly" in fld and fld["athenaOnly"] == "Y":
                continue
            # Skip computed fields if the flag is set
            if excludeComputed and "computed" in fld and fld["computed"] == "Y":
                continue

            item = {}
            item['metadata'] = {}
            item['nullable'] = True

            # The field names for input data may different from what is being used in database
            if useValidation and "validation" in fld:
                item['name'] = fld["validation"]
            else:
                item['name'] = fld["name"]

            # Athena/Redshift uses lower case for column names.  Make sure the Parquet schema matches the case
            item['name'] = item["name"].lower()

            if forceAllFieldsToString:
                item['type'] = 'string' # Ignore field type.  Just return string (for data cleaning)
            elif fld["type"] == "VARCHAR":
                item['type'] = 'string'
            elif fld["type"] == "SMALLINT":
                item['type'] = 'short'
            elif fld["type"] == "INTEGER":
                item['type'] = 'integer'
            elif fld["type"] == "BIGINT":
                item['type'] = 'long'
            elif fld["type"] == "REAL" or fld["type"] == "FLOAT4":
                # NOTE: Even though Parquet supports float, we have to use double because Athena only supports double
                # If data is saved as float in Parquet, Athena won't be able to read it.
                #item ['type'] = 'float'
                item['type'] = 'double'
            elif fld["type"] == "FLOAT" or fld["type"] == "FLOAT8":
                item['type'] = 'double'
            elif fld["type"] == "DATE":
                item['type'] = 'string' # Date is supported by Parquet but Athena does not support date in parquet format
            elif fld["type"] == "TIMESTAMP":
                item['type'] = 'string' # Timestamp is supported by Parquet but athena is shifting the time by UTC
            elif fld["type"] == "BOOLEAN":
                item['type'] = 'boolean'
            elif fld["type"] == "DECIMAL":
                item['type'] = 'double' # TODO 'decimal(' + fld["size"] + ')'
            else:
                raise ValueError('Data type: ' + fld["type"] + ' is not supported.  Please add support for this type.')
            schemaJson['fields'].append(item)
        return schemaJson

    @staticmethod
    def BuildSparkSchema(table, forceAllFieldsToString=False, useValidation=False, excludeComputed=False):
        '''
        returns the schema for spark
        '''
        from pyspark.sql.types import StructType #@UnresolvedImport
        schemaJson = SparkUtilities.BuildSparkSchemaJson(table, forceAllFieldsToString, useValidation, excludeComputed)
        schema = StructType.fromJson(schemaJson)
        return schema

    @staticmethod
    def ConvertNanToNull(df):
        '''
        Spark Nan is read in as string Nan.  Convert Nan to null
        '''
        from pyspark.sql.functions import udf  #@UnresolvedImport
        from pyspark.sql.types import StringType  #@UnresolvedImport
        udfConvertNanToNull = udf(lambda name: None if name == "NaN" else name, StringType())

        for fld in df.schema.fields:
            if fld.dataType == StringType():
                df = df.withColumn(fld.name, udfConvertNanToNull(df[fld.name]))
        return df

    @staticmethod
    def ConvertTypesToSchema(df, schema):
        '''
        Spark Nan is read in as string Nan.  Convert Nan to null
        '''
        for fld in schema.fields:
            if fld.dataType != df.schema[fld.name].dataType:
                df = df.withColumn(fld.name, df[fld.name].cast(fld.dataType))
        return df

    @staticmethod
    def ProcessSpecialCharsIfAny(df, tables):
        '''
        if any characters had to be replace use this method in the dataframe
        '''
        from pyspark.sql import functions as F  #@UnresolvedImport
        for field in tables["fields"]:
            if "specialcharacters" in field:
                fieldName = field["name"].lower()
                for symbolValue in field["specialcharacters"]:
                    symbol = symbolValue["symbol"]
                    value = symbolValue["value"]
                    df = df.withColumn(fieldName, F.when(df[fieldName] == symbol, value).otherwise(df[fieldName]))
        return df

    @staticmethod
    def ReadCSVFile(spark, tables, fieldDelimiter, header, path, logger):
        '''
        reads a CSV file and returns a dataframe
        '''
        schema = SparkUtilities.BuildSparkSchema(tables)
        df = (spark.read
              .format("com.databricks.spark.csv")
              .options(header=header, delimiter=fieldDelimiter,
                       ignoreTrailingWhiteSpace=True, ignoreLeadingWhiteSpace=True)
              .schema(schema)
              .load(path)
             )
        df = SparkUtilities.ProcessSpecialCharsIfAny(df, tables)
        logger.info("SparkUtilities -- DONE READING " + str(df.count()) + " ROWS.  Now saving as parquet file...")
        return df

    @staticmethod
    def SaveParquet(df, fileUtilities, fileBaseName="FlushAndFill"):
        fileUtilities.EmptyFolderContents(fileUtilities.parquet)
        df = df.coalesce(1) # Trying to create one big parquet file
        df.write.parquet(fileUtilities.parquet, mode="overwrite")
        
        # Rename files to something more readable
        files = glob.glob(fileUtilities.parquet + "*.parquet")
        if len(files) == 1:
            newFileName = fileUtilities.parquet + fileBaseName + ".snappy.parquet"
            shutil.move(files[0], newFileName)
        else:
            i = 0
            for oldFileName in files:
                newFileName = fileUtilities.parquet + fileBaseName + "-" + str(i) + ".snappy.parquet"
                shutil.move(oldFileName, newFileName)
                i += 1


    @staticmethod
    def GetSqlServerConnectionParams(dbCommon):
        '''
        returns the url and driver
        '''
        url = "jdbc:sqlserver://" + dbCommon["server"] + ":1433;DatabaseName=" + dbCommon["name"] + ";integratedSecurity=true"
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        return url, driver

    @staticmethod
    def RenameColumns(df, oldColumns, newColumns):
        '''
        renames a colum in a dataframe
        '''
        df = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), df)
        return df

    @staticmethod
    def RenameColumnsToSchema(df, schema):
        '''
        renames a column in a dataframe to the original schema name
        '''
        oldColumns = df.schema.names
        newColumns = schema.names
        return SparkUtilities.RenameColumns(df, oldColumns, newColumns)

    @staticmethod
    def RenameColumnsInList(df, renameList):
        '''
        renames all the columns in the schema in the dataframe
        '''
        oldColumns = df.schema.names

        newColumns = []
        for colName in oldColumns:
            newName = colName
            for item in renameList:
                if colName == item[0]:
                    newName = item[1] # Use new name
                    break
            newColumns.append(newName)

        return SparkUtilities.RenameColumns(df, oldColumns, newColumns)

    @staticmethod
    def ReplaceAll(df, pattern, replacement):
        '''
        replaces a pattern in a dataframe
        '''
        from pyspark.sql.functions import regexp_replace #@UnresolvedImport
        columnNames = df.schema.names
        df = reduce(lambda df, idx: df.withColumn(columnNames[idx],
                                                  regexp_replace(columnNames[idx], pattern, replacement)),
                    xrange(len(columnNames)), df)
        return df

    @staticmethod
    def ReadTableUsingJDBC(spark, url, driver, tables, logger):
        '''
        connects to a data source and returns a dataframe
        '''
        tableName = tables["table"]
        if "sourcetable" in tables:
            tableName = tables["sourcetable"] # Use the source table name instead
        df = spark.read.format("jdbc").options(driver=driver, url=url, dbtable=tableName).load()

        # Debug code to get a subset of the data
        #df.createOrReplaceTempView("InputTable")
        #df = spark.sql("Select * from InputTable where lrno = '1000760'")

        # Convert column names to lower case names for consistency
        schema = SparkUtilities.BuildSparkSchema(tables)
        df = SparkUtilities.RenameColumnsToSchema(df, schema)
        df = SparkUtilities.ConvertTypesToSchema(df, schema)

        # Debug code to get a subset of the data
        #print(schema)
        #df.printSchema()
        #df.show()

        logger.info("SparkUtilities -- DONE READING " + str(df.count()) + " ROWS.  Now saving as parquet file...")
        return df

    @staticmethod
    def FormatColumn(df, colName, inFormatString):
        '''
        formats a date value stored in a string column in a dataframe to the date
        then sends it back as a string.
        date formats are considered 11 or less in length mmm/dd/yyyy
        if the length is longer than it is consider to be a timestamp format
        '''
        from pyspark.sql.functions import to_timestamp, to_date  #@UnresolvedImport
        from pyspark.sql.types import StringType  #@UnresolvedImport
        lenFormat = len(inFormatString)
        if lenFormat > 11:
            df = df.withColumn(colName, to_timestamp(colName, inFormatString))
        else:
            df = df.withColumn(colName, to_date(colName, inFormatString))
        df = df.withColumn(colName, df[colName].cast(StringType()))
        return df

    @staticmethod
    def TestCodePrintBadLine(line):
        '''
        Test Code to print out lines that seem to have more columns that expected
        '''
        if len(line) > 220:
            print line

    @staticmethod
    def TestCodeProcessBinaryFile(sc, sqlContext, tables, fileUtilities, rowTerminator, fieldTerminator):
        '''
        Test Code to see if we can process the specially delimited file as binary.
        Findinds: This takes too much memory
        '''
        rdd = sc.binaryFiles(fileUtilities.csvFolder).values() # Read as binary as there are non-text characters in the files
        rdd = rdd.flatMap(lambda contents: [lines.replace("\x00", "") for lines in contents.split(rowTerminator)])
        #rdd = rdd.flatMap(lambda contents: [lines for lines in contents.split(u"{~*]")])
        rdd = rdd.map(lambda line: [column for column in line.split(fieldTerminator)]) # Break the line into columns

        rdd.map(lambda line: SparkUtilities.TestCodePrintBadLine(line)) # Break the line into columns
        rdd = rdd.filter(lambda row: len(row) > 1)
        #x = rdd.take(5)
        #print(x)

        # Load RDD into a dataframe as strings and then do the type conversion based on the schema
        schemaForceAllFieldsToString = SparkUtilities.BuildSparkSchema(tables, True)
        df = sqlContext.createDataFrame(rdd, schemaForceAllFieldsToString)
        schema = SparkUtilities.BuildSparkSchema(tables)
        df = SparkUtilities.ConvertTypesToSchema(df, schema)
        return df
