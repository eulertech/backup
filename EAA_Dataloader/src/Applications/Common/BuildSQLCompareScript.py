'''
Created on Feb 4, 2017
Build script to compare two tables
@author: Christopher Lewis
'''

import sys
import os
import psycopg2  # library to support connection to Redshift (Postgres)
from AACloudTools.RedshiftUtilities import RedshiftUtilities

# pylint: disable=wrong-import-position
sys.path.append(os.getcwd())  # For running in VsCode and Linux we need to add this path
from AACloudTools import ConfigureAWS
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.AthenaUtilities import AthenaUtilities

def ProcessCatalogs(catalog, tableOfInterest):
    for tableSettings in catalog["tables"]:
        if tableSettings["table"].lower() == tableOfInterest.lower():
            return tableSettings
    return None

def ProcessDatabase(databaseSettings, tableOfInterest):
    tableSettings = None
    for catalog in databaseSettings["catalogs"]:
        tableSettings = ProcessCatalogs(catalog, tableOfInterest)
        if tableSettings is not None:
            break
    return tableSettings

def HandleSpecialFields(tableSettings, enableValidation, columnsToExclude):
    '''
    If we have a date field we need to handle as a special case since Athena/Parquet does not support Date
    '''
    sqlSubStatement = ""
    for fld in tableSettings["fields"]:
        # Skip the field if it is for Athena management
        if "athenaOnly" in fld and fld["athenaOnly"]=="Y":
            continue
        fieldName = fld["name"]
        if fieldName in columnsToExclude:
            continue

        if sqlSubStatement: # Subsequent field
            sqlSubStatement = sqlSubStatement + "\n        ,"
        else: # First field
            sqlSubStatement = sqlSubStatement + "\n        "
            
        fieldType = fld["type"]
        if fieldType == "TIMESTAMP":
            sqlSubStatement = sqlSubStatement + "CAST(" + fieldName + " AS VARCHAR(20))"
        elif enableValidation and "validation" in fld and fld["validation"] in fieldName: # Exclude lat/long and NULLIF
            sqlSubStatement = sqlSubStatement + fieldName + " as " + str(fld["validation"])
        elif "validation" in fld and fld["validation"] in fieldName: # Exclude lat/long and NULLIF
            sqlSubStatement = sqlSubStatement + str(fld["validation"])
        else:
            sqlSubStatement = sqlSubStatement + fieldName
    
    sqlSubStatement = sqlSubStatement + "\n"
    return sqlSubStatement


def BuildSqlSubStatement(tableSettings, schema, table, validation, whereClause, columnsToExclude):
    if AthenaUtilities.ContainsSpecialFields(tableSettings) or len(columnsToExclude) > 0:
        fields = HandleSpecialFields(tableSettings, validation, columnsToExclude)
    else:
        fields = " * " # Perform straight compare
    sqlScript = "\n  SELECT" + fields + "  FROM " + schema + '.' + table
    if whereClause is not None:
        sqlScript = sqlScript + "\n  WHERE " + whereClause
    return sqlScript

def SqlToCompareTables(logger, tableSettings, fname, schema1, table1, validation1, schema2, table2, validation2, whereClause, columnsToExclude):
    '''
    Create SQL Script to load data from Athena into the redshift table
    '''
    sqlScript = "SELECT COUNT(*) FROM ("
    sqlScript = sqlScript + BuildSqlSubStatement(tableSettings, schema1, table1, validation1, whereClause, columnsToExclude)
    sqlScript = sqlScript + "\nEXCEPT"
    sqlScript = sqlScript + BuildSqlSubStatement(tableSettings, schema2, table2, validation2, whereClause, columnsToExclude)
    sqlScript = sqlScript + "\n);"
    
    if fname is not None:
        outfile = open(fname, "w")
        FileUtilities.PutLine(sqlScript, outfile)
        outfile.close()
    
    return sqlScript

def BuildSQLCompareScript(logger, fileUtilities, item):
    '''
    Build script to compare results
    '''
    application = item[0]
    tableOfInterest = item[1]
    compareWithSchema = item[2]
    whereClause = item[3]
    
    columnsToExclude = []
    if len(item)>=5:
        columnsToExclude = item[4]
       
    # Get the json file that contains the configuration
    jobConfigFile = "../" + application + "/jobConfig.json"
    job = fileUtilities.LoadJobConfiguration(jobConfigFile)
    
    tableSettings = None
    if "Databases" in job:
        for databaseSettings in job["Databases"]:
            tableSettings = ProcessDatabase(databaseSettings, tableOfInterest)
            if tableSettings is not None:
                break
    elif "catalogs" in job:
        tableSettings = ProcessDatabase(job, tableOfInterest)
    elif "tables" in job:
        tableSettings = ProcessCatalogs(job, tableOfInterest)

    if tableSettings is None:
        logger.info("Cannot find table: " + tableOfInterest)
        return

    awsParams = ConfigureAWS.ConfigureAWS()
    awsParams.LoadAWSConfiguration(logger)
    rsConnect = psycopg2.connect(dbname=awsParams.redshift['Database'], host=awsParams.redshift['Hostname'], port=awsParams.redshift['Port'],
                                 user=awsParams.redshiftCredential['Username'], password=awsParams.redshiftCredential['Password'])

    fname = "Compare.sql"
    # SQL EXCEPT first table with second
    sqlScript = SqlToCompareTables(logger, tableSettings, fname, tableSettings["schemaName"], tableOfInterest, True,
                                   compareWithSchema, tableOfInterest, False, whereClause, columnsToExclude)
    
    count = RedshiftUtilities.GetRecordCountFromSQL(rsConnect, sqlScript)
    if count != 0:
        logger.info("*** FAILED: MISMATCH on first EXCEPT for table: " + tableOfInterest + " by: " + str(count) + " ***")
        return
        
    # SQL EXCEPT second table with first
    sqlScript = SqlToCompareTables(logger, tableSettings, fname, compareWithSchema, tableOfInterest, False,
                                   tableSettings["schemaName"], tableOfInterest, True, whereClause, columnsToExclude)
    count = RedshiftUtilities.GetRecordCountFromSQL(rsConnect, sqlScript)
    if count != 0:
        logger.info("*** FAILED: MISMATCH on second EXCEPT  for table: " + tableOfInterest + " by: " + str(count) + " ***")
        return

    rsConnect.close()
    
    if whereClause:
        logger.info(" SUCCESS: Selected records MATCH for condition: " + whereClause + ". SUCCESS!")
    else:
        logger.info(" SUCCESS: All records MATCH. SUCCESS!")

# Main
compareCriteria = [
    # Application, Table, Schema to compare with, Condition to limit data to compare (or None for all), Option column list to exclude
    #("MaritimeAthenaSpark", "tblcombmovementcalls",  "ra", "callid >= 91000000 and callid < 92000000"),
    #("MaritimeAthenaSpark", "tblallaisfiles",  "ra", "ais_id > 3821000000 and ais_id < 3822000000"), #["Callsign", "Destination"]), Compare all columns
    ("MaritimeAthenaSpark", "tblallorbfiles",  "ra", "orb_id > 2328388902 and orb_id < 2329407732"), #["Callsign", "Destination"])
    #("MaritimeAthenaSpark", "tblPort",  "ra", None),
    #("MaritimeAthenaSpark", "absd_aux_engines_work",  "ra", None),
    #("MaritimeAthenaSpark", "absd_cadi",  "ra", None),
    #("MaritimeAthenaSpark", "absd_fixtures",  "ra", None),
    #("MaritimeAthenaSpark", "absd_himo",  "ra", None),
    #("MaritimeAthenaSpark", "absd_hiop",  "ra", None),
    #("MaritimeAthenaSpark", "absd_hiow",  "ra", None),
    #("MaritimeAthenaSpark", "absd_ncon",  "ra", None),
    #("MaritimeAthenaSpark", "absd_owin",  "ra", None),
    #("MaritimeAthenaSpark", "absd_sale",  "ra", None),
    #("MaritimeAthenaSpark", "absd_ship_search",  "ra", None),
    #("MaritimeAthenaSpark", "production_extras",  "ra", None),
    #("MaritimeAthenaSpark", "supplemental_absd_cadi",  "ra", None),
    #("MaritimeAthenaSpark", "supplemental_absd_ncon",  "ra", None),
    #("MaritimeAthenaSpark", "tblaisliverefportgeo",  "ra", None),
    #("MaritimeAthenaSpark", "tblaisliverefportzoneedges",  "ra", None),
    #("MaritimeAthenaSpark", "tblaisliverefportzonegeo",  "ra", None),
    #("MaritimeAthenaSpark", "tblcombports",  "ra", None),
    #("MaritimeAthenaSpark", "tblcombpositionlist",  "ra", None),
    #("MaritimeAthenaSpark", "tblport",  "ra", None),
    #("MaritimeAthenaSpark", "tblterminal",  "ra", None)
    #("TotemAthenaSpark", "totem", "eaa_prod", None),
    #("HistoricalBrentAthenaSpark", "historicalbrent", "eaa_prod", None),
    #("FHWAAthenaSpark", "fhwa_faf34", "eaa_prod", None),
    #("OilMarketForecastAthenaSpark", "oilmarketforecast", "eaa_prod", None),
    #("LiquidsBalanceAthenaSpark", "liquidsbalance_tightoil", "eaa_prod", None),
    #("AutoParcAthenaSpark", "auto_parc", "eaa_dev", None)
    ]

logger = FileUtilities.CreateLogger("log", 10)
fileUtilities = FileUtilities(logger)
for item in compareCriteria:
    BuildSQLCompareScript(logger, fileUtilities, item)