'''
Created on Jan, 2017

@author: Christopher Lewis (Work derived from Thomas Coffey)
@summary: Get Parameters for the various AWS configurations
'''

import os
from AACloudTools.FileUtilities import FileUtilities

class ConfigureAWS(object):
    '''
    Get Parameters for the various AWS configurations
    '''
    def __init__(self):
        '''
        Basic initialization of instance variable
        '''
        self.s3 = None
        self.redshift = None
        self.redshiftCredential = None
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def byteify(self, data):
        '''
        By default json load will load string as unicode.  This strings are then loaded into the process environment
        Unfortunately, Spark with Python 2.7 on Windows cannot handle unicode in the environment settings.
        Spark with Python 3.5+ works fine.  So for Python 2.7, we add this conversion.  
        '''
        if isinstance(data, dict):
            return {self.byteify(key): self.byteify(value)
                    for key, value in data.iteritems()}
        elif isinstance(data, list):
            return [self.byteify(element) for element in data]
        elif isinstance(data, unicode):
            return data.encode('utf-8')
        else:
            return data
    
    # Load the AWS configuration for RedShift and S3
    def LoadAWSConfiguration(self, logger):
        '''
        Load the configuration data for Amazon.  E.g. Redshift password and S3 keys etc.
        '''
        import json
        try:
            with open(self.location + '/Config/config.json') as configfile:
                configdata = self.byteify(json.load(configfile))

            self.redshift = configdata["configuration"]["redshift"]
            self.s3 = configdata["configuration"]["S3"]
            
            self.SwitchS3CredentialsTo(self.s3["access_key_id"], self.s3["secret_access_key"])
            
            if "Athena" in configdata["configuration"]:
                self.athena = configdata["configuration"]["Athena"]
                
            os.environ["SPARK_HOME"] = "/opt/spark"  # This is the default on the cloud
            
            if "LocalSettings" in configdata["configuration"]:
                if "LocalSettingsFile" in configdata["configuration"]["LocalSettings"]:
                    localSettingsFilePath = self.location + configdata["configuration"]["LocalSettings"]["LocalSettingsFile"]
                    if os.path.exists(localSettingsFilePath):
                        with open(localSettingsFilePath) as localSettingsFile:
                            localSettings = self.byteify(json.load(localSettingsFile))
                            if "Spark" in localSettings:
                                spark = localSettings["Spark"]
                                if "SPARK_HOME" in spark:
                                    os.environ["SPARK_HOME"] = spark["SPARK_HOME"]
                                if "HADOOP_HOME" in spark:
                                    os.environ["HADOOP_HOME"] = spark["HADOOP_HOME"]

            with open(self.location + self.redshift["UserPasswordFile"]) as userPasswordFile:
                self.redshiftCredential = self.byteify(json.load(userPasswordFile))
                if self.redshiftCredential['Username'] == "your_redshift_username":
                    import sys
                    message = "Edit file: " + self.redshift["UserPasswordFile"] + " and enter your user name and password.  Aborting execution!"
                    sys.exit(message)

                self.SetScriptExecuteEnvironment()
        except:
            logger.exception("we had an error in ConfigAWS during LoadAWSConfiguration")
            raise

    def SetScriptExecuteEnvironment(self):
        '''
        Setup environment variable for accessing various commponents especially Postgres RedShift
        '''
        if self.redshift["Hostname"]:
            os.environ["PGCLIENTENCODING"] = "UTF8"

            os.environ["PGHOSTNAME"] = self.redshift["Hostname"]
            os.environ["PGDBNAME"] = self.redshift["Database"]
            os.environ["PGPORT"] = str(self.redshift["Port"])
            os.environ["PGDRIVERCLASS"] = self.redshift["DriverClass"]

            urlToRedshift = "jdbc:redshift://" + self.redshift["Hostname"] +  ":" + str(self.redshift["Port"]) + "/" + self.redshift["Database"]
            os.environ["PGURLRS"] = urlToRedshift

            if self.redshiftCredential["Username"]:
                os.environ["PGUSER"] = self.redshiftCredential["Username"]
                os.environ["PGPASSWORD"] = self.redshiftCredential["Password"]


    def SwitchS3CredentialsTo(self, key, secret_key):
        os.environ["AWS_ACCESS_KEY_ID"] = key
        os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
    
    def SwitchS3CredentialsToAthena(self):
        '''
        Switch to credentials that will allow write access to Athena
        '''
        old_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        old_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        self.SwitchS3CredentialsTo(self.athena["access_key_id"], self.athena["secret_access_key"])
        return old_key, old_secret_key
