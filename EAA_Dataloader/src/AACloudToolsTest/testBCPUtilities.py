'''
Created on Feb 27, 2017

@author: ZIK40226
'''
import unittest, os
from AACloudToolsTest import Common
from AACloudTools import FileUtilities
from AACloudTools.BCPUtilities import BCPUtilities
from AACloudTools.ConfigureAWS import ConfigureAWS

class testBCPUtilities(Common.ABCTestCase):
    __moduleName__ = "testBCPUtilities"

    @classmethod
    def setUpClass(self):
        try:
            self.initDependencies()
            self.createTestingFolder(self.__moduleName__)
            self.fileUtilities = FileUtilities.FileUtilities
            self.awsParams = ConfigureAWS()
            self.awsParams.LoadAWSConfiguration(self.logger)
            self.bcpUtilities = BCPUtilities(self.logger, self.fileUtilities, self.awsParams, self.testingFolder)
        except Exception as err:
            raise Exception("BCPUtilities class could not be instantiated. Error: " + err.message)                    

    @classmethod
    def tearDownClass(self):
        self.bcpUtilities = None

    def testGetFileToBeUploaded(self):
        fakeFileName = "testGetFileToBeUploaded.txt"

        evaluating = self.bcpUtilities.GetFileToBeUploaded(fakeFileName, None)
        expected = fakeFileName
        self.assertEqual(expected, evaluating, "File name returned does not match expected without charsToBeReplaced.")

        evaluating = self.bcpUtilities.GetFileToBeUploaded(fakeFileName, "None")
        expected = "scrubbed_" + fakeFileName
        self.assertEqual(expected, evaluating, "File name returned does not match expected with charsToBeReplaced.")

    def testGetFullFilePath(self):
        fakeFileName = "testGetFullFilePath.txt"

        evaluating = self.bcpUtilities.GetFullFilePath(fakeFileName)
        expected = self.fixBackSlash(os.path.join(self.testingFolder, fakeFileName))

        self.assertEqual(evaluating, expected, "File Path returned does not match expected.")

    def testRunBCPJob(self):
        outputFileName = self.fixBackSlash(os.path.join(self.testingFolder, "testRunBCPJob.txt"))

        self.bcpUtilities.RunBCPJob(self.config["BCPUtilities"]["sqlServerloginInfo"], 
                                                      self.config["BCPUtilities"]["bcpUtilityDirOnLinux"], 
                                                      self.config["BCPUtilities"]["inputQuery"], 
                                                      outputFileName, 
                                                      self.config["BCPUtilities"]["fieldTerminator"], 
                                                      self.config["BCPUtilities"]["rowTerminator"])

        self.assertGreater(sum(1 for line in open(outputFileName)), 0, "BCP command statement has not imported any rows.")

def suiteTest():
    return testBCPUtilities.getSuite()

if __name__ == "__main__":
    unittest.TextTestRunner(descriptions=False, verbosity=2, resultclass=Common.TestCaseResult).run(testBCPUtilities.getSuite())
