'''
Created on Feb 27, 2017

@author: Christopher Lewis
'''
import unittest, os
from AACloudToolsTest import Common 
from AACloudTools.ConfigureAWS import ConfigureAWS
from AACloudTools.S3Utilities import S3Utilities

class testS3Utilities(Common.ABCTestCase):
    __moduleName__ = "testS3Utilities"

    @classmethod
    def setUpClass(self):
        self.initDependencies()
        self.createTestingFolder(self.__moduleName__)

        try:
            self.awsParams = ConfigureAWS()
            self.awsParams.LoadAWSConfiguration(self.logger)
        except:
            raise Exception("AWS parameters could not be configured.")

    def testDownloadFileFromS3(self):
        testFile = self.createTestingFile("testDownloadFileFromS3.txt", "Testing DownloadFileFromS3 from S3Utilities...")
        testFileReturned = testFile.replace(".txt", "_returned.txt")

        bucketName, s3TempKey = S3Utilities.UploadFileToS3Temp(self.awsParams.s3, testFile)
        S3Utilities.DownloadFileFromS3(self.awsParams.s3, bucketName, s3TempKey, testFileReturned)
        self.assertTrue(os.path.isfile(testFileReturned), "File could not be downloaded from the cloud.")

    def testGetS3FileName(self):
        fileNameTested = "testGetS3FileName.txt"
        testFile = self.createTestingFile(fileNameTested, "Testing GetS3FileName from S3Utilities...")
        bucketName, s3TempKey = S3Utilities.UploadFileToS3Temp(self.awsParams.s3, testFile)
        s3FileName = S3Utilities.GetS3FileName(bucketName, s3TempKey)
        listToValid = s3FileName.split("/")

        self.assertIn(bucketName, listToValid, "s3 File Name does not contain the bucketName.")
        self.assertIn(fileNameTested, listToValid, "s3 File Name does not contain a valid s3TempKey.")

    def testUploadFileToS3(self):
        testFileName = "testUploadFileToS3.txt"
        testFile = self.createTestingFile(testFileName, "Testing testUploadFileToS3 from S3Utilities...")
        testFileReturned = testFile.replace(".txt", "_returned.txt")

        bucketName = self.config["S3Utilities"]["testBucketName"]
        s3TempKey = self.config["S3Utilities"]["s3TempKeyFolder"] + "/" + testFileName

        S3Utilities.UploadFileToS3(self.awsParams.s3, testFile, bucketName, s3TempKey)

        S3Utilities.DownloadFileFromS3(self.awsParams.s3, bucketName, s3TempKey, testFileReturned)
        self.assertTrue(os.path.isfile(testFileReturned), "File was not uploaded correctly to the cloud bucket.")

    def testUploadFileToS3Temp(self):
        testFile = self.createTestingFile("testUploadFileToS3Temp.txt", "Testing UploadFileToS3Temp from S3Utilities...")
        testFileReturned = testFile.replace(".txt", "_returned.txt")

        bucketName, s3TempKey = S3Utilities.UploadFileToS3Temp(self.awsParams.s3, testFile)

        S3Utilities.DownloadFileFromS3(self.awsParams.s3, bucketName, s3TempKey, testFileReturned)
        self.assertTrue(os.path.isfile(testFileReturned), "File was not found or uploaded at the cloud bucket.")

    def testDeleteFile(self):
        testFile = self.createTestingFile("testDeleteFile.txt", "Testing DeleteFile from S3Utilities...")
        testFileReturned = testFile.replace(".txt", "_returned.txt")

        bucketName, s3TempKey = S3Utilities.UploadFileToS3Temp(self.awsParams.s3, testFile)
        S3Utilities.DeleteFile(self.awsParams.s3, bucketName, s3TempKey)

        try:
            S3Utilities.DownloadFileFromS3(self.awsParams.s3, bucketName, s3TempKey, testFileReturned)
            self.assertFalse(os.path.isfile(testFileReturned), "File was not deleted from the cloud.")
        except Exception as err:
            if err.status != 404:
                self.fail("Error registered while trying to delete a file from the cloud. Error:" + err.message)

def suiteTest():
    return testS3Utilities.getSuite()

if __name__ == "__main__":
    unittest.TextTestRunner(descriptions=False, verbosity=2, resultclass=Common.TestCaseResult).run(testS3Utilities.getSuite())