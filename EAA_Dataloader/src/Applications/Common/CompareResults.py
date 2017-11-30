'''
Created on Feb 4, 2017
Compare results of dev with production
@author: Christopher Lewis
'''

import sys
import os
import psycopg2  # library to support connection to Redshift (Postgres)

# pylint: disable=wrong-import-position
sys.path.append(os.getcwd())  # For running in VsCode and Linux we need to add this path
from AACloudTools import ConfigureAWS
from AACloudTools.RedshiftUtilities import RedshiftUtilities

def CompareResults():
    '''
    Compare results between stage and prod
    '''
    logger = None
    schemas = ["eaa_dev.cfl_", "eaa_prod."]
    schemas = ["eaa_dev.cfl_", "eaa_dev."]
    print "Processing.  Compare results between schemas: " + schemas[0] + " and " + schemas[1]
    tables = ["auto_lv_production", "auto_lv_sales", "auto_parc", "cftc", "eia_pet_imports_series_attributes", "eia_pet_imports_series_data",
              "eia_pet_series_attributes", "eia_pet_series_data", "eia_steo_series_attributes", "eia_steo_series_data",
              "totem", "opis_retail_price", "opis_retail_volume", "chemicals", "enp", "rigcount"]
    tables = ["eia_pet_imports_series_attributes", "eia_pet_imports_series_data",
              "eia_pet_series_attributes", "eia_pet_series_data", "eia_steo_series_attributes", "eia_steo_series_data"]

    awsParams = ConfigureAWS.ConfigureAWS()
    awsParams.LoadAWSConfiguration(logger)
    rsConnect = psycopg2.connect(dbname=awsParams.redshift['Database'], host=awsParams.redshift['Hostname'], port=awsParams.redshift['Port'],
                                 user=awsParams.redshiftCredential['Username'], password=awsParams.redshiftCredential['Password'])

    for table in tables:
        recCount = {}
        for schema in schemas:
            redshiftTableName = schema + table
            try:
                recCount[schema] = RedshiftUtilities.GetRecordCount(
                    rsConnect, redshiftTableName)
            except psycopg2.ProgrammingError:
                recCount[schema] = -1 # Skip this table and mark count as -1

        diff = recCount[schemas[0]] - recCount[schemas[1]]
        flag = " *** Count Different! ***" if diff != 0 else ""
        print("Table: " + table + ", " + schemas[0] + ": " + str(recCount[schemas[0]]) +
              ", " + schemas[1] + ": " + str(recCount[schemas[1]]) + ", diff: " + str(diff) + flag)

    rsConnect.close()


CompareResults()
