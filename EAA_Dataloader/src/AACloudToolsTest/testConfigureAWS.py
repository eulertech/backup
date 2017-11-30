'''
Created on Feb 27, 2017

@author: ZIK40226
'''
import unittest, os
from AACloudToolsTest import Common
from AACloudTools.ConfigureAWS import ConfigureAWS

class testConfigureAWS(Common.ABCTestCase):
    __moduleName__ = "testConfigureAWS"

    @classmethod
    def setUpClass(self):
        try:
            self.initDependencies()
            self.configureAWS = ConfigureAWS()
        except Exception as err:
            raise Exception("ConfigureAWS class could not be instantiated. Error: " + err.message)                    

    def testLoadAWSConfiguration(self):
        self.configureAWS.LoadAWSConfiguration(self.logger)

        self.assertIsNotNone(self.configureAWS.s3, "AWS s3 configuration not fully created.")
        self.assertIsNotNone(self.configureAWS.redshift, "AWS Redshift configuration not fully created.")
        self.assertIsNotNone(self.configureAWS.redshiftCredential, "AWS Redshift Credentials configuration not fully created.")

    def testsetScriptExecuteEnvironment(self):
        self.configureAWS.LoadAWSConfiguration(self.logger)

        self.assertEqual(os.environ["PGHOSTNAME"], self.configureAWS.redshift["Hostname"], "Environment variables were not properly set.")

def suiteTest():
    return testConfigureAWS.getSuite()

if __name__ == "__main__":
    unittest.TextTestRunner(descriptions=False, verbosity=2, resultclass=Common.TestCaseResult).run(testConfigureAWS.getSuite())
