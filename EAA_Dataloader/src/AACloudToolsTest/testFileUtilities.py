'''
Created on Feb 27, 2017

@author: ZIK40226
'''
import unittest, os, zipfile, logging
from AACloudToolsTest import Common
from AACloudTools import FileUtilities

class testFileUtilities(Common.ABCTestCase):
    __moduleName__ = "testFileUtilities" 

    @classmethod
    def setUpClass(self):
        self.initDependencies()
        self.fileUtilities = FileUtilities.FileUtilities(self.logger)
        self.createTestingFolder(self.__moduleName__)

    def testGzipFile(self):
        gzipFileToTest = self.createTestingFile("testGzipFile.txt", "Testing GzipFile from File Utilities...")        
        gzipFileZippedToTest = gzipFileToTest + ".gz"
        gUnzippedName = self.fixBackSlash(os.path.join(self.testingFolder, "testGzipFileUnzipped.txt"))

        self.fileUtilities.gzipFile(gzipFileToTest, gzipFileZippedToTest)

        if os.path.isfile(gzipFileZippedToTest) == True:
            self.fileUtilities.gunzipFile(gzipFileZippedToTest, gUnzippedName)
            self.assertTrue(os.path.exists(gUnzippedName), "Gzip file was not correctly created.")
        else:
            self.fail("File could not be gzipped.")

    def testGunzipFile(self):
        gUnzipFileToTest = self.createTestingFile("testGunzipFile.txt", "Testing GunzipFile from File Utilities...")
        gUnzipFileZippedToTest = gUnzipFileToTest + ".gz"
        gUnzippedFileName = gUnzipFileToTest.replace(".txt", "Unzipped.txt")

        self.fileUtilities.gzipFile(gUnzipFileToTest, gUnzipFileZippedToTest)
        self.fileUtilities.gunzipFile(gUnzipFileZippedToTest, gUnzippedFileName)
        self.assertTrue(os.path.isfile(gUnzippedFileName), "File could not be gunzipped.")

    def testUnzipFile(self):
        fileToTest = self.createTestingFile("testUnzipFile.txt", "Testing UnzipFile from File Utilities...")
        zipFolderToCheck = os.path.join(self.testingFolder, "testUnzipFile")
        newZipFullName = zipFolderToCheck + ".zip"

        with zipfile.ZipFile(newZipFullName, "w") as newZip:
            newZip.write(fileToTest)

        self.fileUtilities.unzipFile(newZipFullName, newZipFullName.replace(".zip", ""))
        self.assertTrue(os.path.exists(zipFolderToCheck), "File Utilities could not extract zip content.")

    def testCreateActualFileFromTemplate(self):
        templateFile = self.createTestingFile("testCreateActualFileFromTemplate.txt", "select * from {schemaName}.{tableName};")
        actualFile =  self.fixBackSlash(os.path.join(self.testingFolder, "testCreateActualFileFromTemplateActual.txt"))

        self.fileUtilities.CreateActualFileFromTemplate(templateFile, actualFile, "fooSchema", "fooTable")

        with open(actualFile) as toTestFile:
            self.assertIn("select * from fooSchema.fooTable;", toTestFile, "Query statement was not placed properly inside the file created.")

    def testReplaceStringInFile(self): 
        testingFile = self.createTestingFile("testReplaceStringInFile.txt", "Test Case:#MODULE_NAME#")
        outputFile =  self.fixBackSlash(os.path.join(self.testingFolder, "testReplaceStringInFileOut.txt"))

        self.fileUtilities.ReplaceStringInFile(testingFile, outputFile, {'#MODULE_NAME#' : self.__moduleName__})

        if sum(1 for line in open(outputFile)) > 0:
            with open(outputFile) as opFile:
                self.assertIn("Test Case:" + self.__moduleName__, opFile, "String was not replaced correctly.")
        else:
            self.fail("Output string was not written.")

    def testReplaceIterativelyInFile(self):
        testingFile = self.createTestingFile("testReplaceIterativelyInFile.txt", "\nTest Case:#MODULE_NAME#\n")
        outputFile =  self.fixBackSlash(os.path.join(self.testingFolder, "testReplaceIterativelyInFileOut.txt"))

        self.fileUtilities.ReplaceIterativelyInFile(testingFile, outputFile, [{'#MODULE_NAME#' : self.__moduleName__}, {"\n": ""}])

        if sum(1 for line in open(outputFile)) > 0:
            with open(outputFile) as opFile:
                self.assertIn("Test Case:" + self.__moduleName__, opFile, "Strings were not replaced correctly.")
        else:
            self.fail("Output strings were not written.")

    def testLoadJobConfiguration(self):
        testModule = "File Utilities"
        testingConfigFile = self.createTestingFile("testLoadJobConfiguration.txt", '{"testModule": "' + testModule + '"}')
        jsonCfg = self.fileUtilities.LoadJobConfiguration(testingConfigFile)

        if jsonCfg is not None:
            self.assertEqual(jsonCfg["testModule"], testModule, "JSON configuration was not properly loaded.")
        else:
            self.fail("JSON configuration could not be created.")

    def testGetApplicationDirectory(self):
        pathToLook = self.fileUtilities.GetApplicationDirectory(self.config["FileUtilities"]["appToTestDir"])

        self.assertTrue(os.path.exists(pathToLook), "Directory was not returned correctly.")

    def testCleanSQLQuery(self):
        sqlStatementCorrected = "select * from table where field = 1;"
        sqlTestFile = self.createTestingFile("cleanSQLQuery.sql", "select   *        from table      where field = 1 ")

        sqlResulted = self.fileUtilities.CleanSQLQuery(sqlTestFile)[0]
        self.assertEqual(sqlResulted, sqlStatementCorrected, "SQL File was not cleaned.")

    def testLoadSQLQuery(self):
        sqlStatement = "select * from table where field = 1;"
        sqlTestFile = self.createTestingFile("testLoadSQLQuery.sql", sqlStatement)

        sqlResulted = self.fileUtilities.CleanSQLQuery(sqlTestFile)[0]
        self.assertEqual(sqlResulted, sqlStatement, "SQL File was not loaded correctly.")

    def testCreateFolder(self):
        pathToTest = os.path.join(self.testingFolder, "testCreateFolder")

        self.fileUtilities.CreateFolder(pathToTest)
        self.assertTrue(os.path.exists(pathToTest), "Folder could not be created.")

    def testRemoveFileIfItExists(self):
        fileToTestWith = self.createTestingFile("testRemoveFile.txt", "Testing RemoveFileIfItExists from File Utilities...")

        self.fileUtilities.RemoveFileIfItExists(fileToTestWith)
        self.assertFalse(os.path.isfile(fileToTestWith), "File was not removed by File Utilities.")

    def testPathToForwardSlash(self):
        pathExample = self.fileUtilities.PathToForwardSlash(os.path.join(self.location, "testFolder"))
        pathExpected = os.path.join(self.location, "testFolder").replace("\\", "/")

        self.assertEqual(pathExample, pathExpected, "Path returned does not match expected.")

    def testCreateLogger(self):
        loggerFolder = self.fixBackSlash(os.path.join(self.testingFolder, "testingLogger"))
        logger = self.fileUtilities.CreateLogger(loggerFolder)

        self.addCleanup(self.cleanUpLoggerHandlers, logger)
        self.assertIsInstance(logger, logging.Logger, "Logger was not created.")

    def testScanFolder(self):
        fileList = self.fileUtilities.ScanFolder(self.location) 

        self.assertIsInstance(fileList, list, "Folder Scan does not return expected file list.")

    def testScanFolderWithFile(self):
        fileToTestName = "testScanFolderWithFile.txt"
        fileToTest = self.createTestingFile(fileToTestName, "Testing ScanFolder With File parameter from File Utilities...")
        filetoTestDir = fileToTest.replace("/" + fileToTestName, "")
        fileList = self.fileUtilities.ScanFolder(filetoTestDir, fileToTestName)

        self.assertIsInstance(fileList, list, "File was not returned in the list.")

    def testScanFolderWithFileExt(self):
        fileToTestName = "testScanFolderWithFileExt.txt"
        fileToTest = self.createTestingFile(fileToTestName, "Testing ScanFolder with file extension from File Utilities...")
        filetoTestDir = fileToTest.replace("/" + fileToTestName, "")
        fileList = self.fileUtilities.ScanFolder(filetoTestDir, None, "txt")

        self.assertIsInstance(fileList, list, "File extension was not returned in the list.")

    def cleanUpLoggerHandlers(self, logger):
        for h in logger.handlers:
            logger.removeHandler(h)
            del h

def suiteTest():
    return testFileUtilities.getSuite()

if __name__ == "__main__":
    unittest.TextTestRunner(descriptions=False, verbosity=2, resultclass=Common.TestCaseResult).run(testFileUtilities.getSuite())
