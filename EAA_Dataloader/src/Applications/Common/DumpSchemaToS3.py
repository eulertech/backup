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

def GetTableList(rsConnect, schemaName):
    '''
    Utility to get the record count in a table
    '''
    cur = rsConnect.cursor()
    command = "SELECT tablename FROM pg_tables WHERE schemaname = '" + schemaName + "' ORDER BY tablename"
    try:
        cur.execute(command)
    except Exception as ex:
        rsConnect.rollback() # Rollback or else the connection will fail
        cur.close()
        raise ex

    data = cur.fetchall()
    cur.close()
    return data

def DumpSchemaToS3():
    '''
    Compare results between stage and prod
    '''
    logger = None
    schemaName = "ra"

    awsParams = ConfigureAWS.ConfigureAWS()
    awsParams.LoadAWSConfiguration(logger)
    rsConnect = psycopg2.connect(dbname=awsParams.redshift['Database'], host=awsParams.redshift['Hostname'], port=awsParams.redshift['Port'],
                                 user=awsParams.redshiftCredential['Username'], password=awsParams.redshiftCredential['Password'])

    with rsConnect:
        tables = GetTableList(rsConnect, schemaName)
        print("Tables to process: ", len(tables))
        for table in tables:
            tableName = table[0]
            command = "UNLOAD ('SELECT * FROM " + schemaName + "." + tableName + "') TO 's3://ihs-lake-01-redshift/" + schemaName + "/" + tableName + "/'"\
                + " WITH CREDENTIALS AS 'aws_access_key_id=xx;aws_secret_access_key=xx'"\
                + " DELIMITER AS '|' GZIP ALLOWOVERWRITE ADDQUOTES ESCAPE PARALLEL TRUE;"
            print(command)
            with rsConnect.cursor() as curs:
                curs.execute(command)


DumpSchemaToS3()
