'''
Created on Feb 27, 2017

@author: ZIK40226
'''
import unittest, os
from AACloudToolsTest import Common
from AACloudTools.OSUtilities import OSUtilities

class testOSUtilities(Common.ABCTestCase):
    __moduleName__ = "testOSUtilities" 

    @classmethod
    def setUpClass(self):
        self.initDependencies()
        self.createTestingFolder(self.__moduleName__)

    def testRunCommandAndLogStdOutStdErr(self):
        if self.platform is not None:
            outputFileName = self.fixBackSlash(os.path.join(self.testingFolder, "testRunCommand.txt"))
            commandToTest = self.config["OSUtilities"]["bcpTest_" + self.platform]
            commandToTest = commandToTest.replace("#fileName#", outputFileName)

            OSUtilities.RunCommandAndLogStdOutStdErr(commandToTest, self.logger)
            self.assertGreater(sum(1 for line in open(outputFileName)), 0, "BCP command statement has not imported any rows.")
        else:
            self.fail("This test case is not available for the current platform.")

def suiteTest():
    return testOSUtilities.getSuite()

if __name__ == "__main__":
    unittest.TextTestRunner(descriptions=False, verbosity=2, resultclass=Common.TestCaseResult).run(testOSUtilities.getSuite())
