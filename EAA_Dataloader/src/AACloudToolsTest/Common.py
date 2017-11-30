'''
Created on Mar 17, 2017

@author: Hector Hernandez
@summary: 
        Class: ABCTestCase is used as a base for all Test Cases distributed in AACloudToolsTest.
        Class: TestCaseResult is used as the text output formatter for every.
'''
import unittest, os, json, time, shutil, logging, platform
from abc import ABCMeta, abstractmethod

class ABCTestCase(unittest.TestCase):
    __metaclass__ = ABCMeta

    @property
    def platform(self):
        current = platform.system().lower()

        if current in self.config["validPlatforms"]:
            return current
        else:
            return None

    @classmethod
    def initDependencies(self):
        try:
            self.config = None
            self.logger = None

            configFile = self.fixBackSlash(os.path.dirname(os.path.abspath(__file__))) + "/suiteTestConfig.json"

            with open(configFile) as cfgFile:
                self.config = json.load(cfgFile)

            if self.config is not None:
                self.location = self.fixBackSlash(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + self.config["suiteTestLocationPrefix"])

                if os.path.exists(self.location) == False:
                    os.mkdir(self.location)

                self.__initLogger__()
                self.logger.info("---- Starting Test Case: " + self.__name__ + " ----")
            else:
                raise "Configuration was not loaded."
        except Exception as err:
            raise Exception("Basic dependencies could not be loaded. Error: " + err.message)

    @classmethod
    @abstractmethod
    def setUpClass(self): 
        """ Initialize objects needed in the whole Test Case. """
        self.initDependencies()

    @classmethod
    @abstractmethod
    def tearDownClass(self):
        """ For cleaning up objects needed in the whole Test Case. """
        pass

    @classmethod
    @abstractmethod
    def setUp(self):
        """ To initialize objects needed within every single test method. """
        pass

    @classmethod
    @abstractmethod
    def tearDown(self):
        """ Use to clean up objects needed within every single test method. """
        pass

    @classmethod
    def getSuite(self):
        selfTestSuite = unittest.TestSuite()
        selfTestSuite.addTest(unittest.makeSuite(self, 'test'))

        return selfTestSuite

    @classmethod
    def removeFiles(self, filesToRemove):
        for item in filesToRemove:
            if os.path.isfile(item) == True:
                os.remove(item)

    @classmethod
    def createTestingFolder(self, folderName):
        self.testingFolder = self.fixBackSlash(os.path.join(self.location, folderName))
        self.removeFolder(self.testingFolder)
        os.mkdir(self.testingFolder)

    @classmethod
    def removeFolder(self, folderPath):
        if os.path.exists(folderPath) == True:
            shutil.rmtree(folderPath)

    @classmethod
    def fixBackSlash(self, s):
        return s.replace("\\", "/")

    @classmethod
    def __initLogger__(self):
        logFolder = self.fixBackSlash(os.path.join(self.location, self.config["loggerPrefix"])) 
        logFile = self.fixBackSlash(os.path.join(logFolder, time.strftime('%Y-%m-%d') + "_unittest.log"))

        if os.path.exists(logFolder) == False:
            os.mkdir(logFolder)

        logging.basicConfig(filename=logFile,
                            filemode='a',
                            format='%(asctime)s %(name)s %(levelname)s: %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

        self.logger = logging.getLogger("[Unit Test]")

    @classmethod
    def createTestingFile(self, testingFileName, fileContentMsg):
        testingFile = self.fixBackSlash(os.path.join(self.testingFolder, testingFileName))

        if os.path.isfile(testingFile) == True:
            os.remove(testingFile)

        with open(testingFile, "w+") as _file:
            _file.write(fileContentMsg)

        return testingFile

class TestCaseResult(unittest.TextTestResult):
    def addError(self, test, err):
        test.logger.error(test._testMethodName + " - ERROR. Message: " + err[1].message)
        return super(TestCaseResult, self).addError(test, err)

    def addFailure(self, test, err):
        test.logger.warning(test._testMethodName + " - FAILED. Message: " + err[1].message)
        return super(TestCaseResult, self).addFailure(test, err)

    def addSuccess(self, test):
        test.logger.info(test._testMethodName + " - OK.")
        return super(TestCaseResult, self).addSuccess(test)

    def addSkip(self, test, reason):
        test.logger.info(test._testMethodName + " - SKIPPED. Message: " + reason)
        return super(TestCaseResult, self).addSkip(test, reason)
