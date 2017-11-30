'''
Created on Mar 16, 2017

@author: Hector Hernandez
@summary: This module is used as the main point to start the execution of all of the Test Suites within AACloudToolsTest. 
'''
import unittest
from AACloudToolsTest import Common, testBCPUtilities, testOSUtilities, testFileUtilities, testS3Utilities, testConfigureAWS

testSuite = unittest.TestSuite()
testSuite.addTest(testBCPUtilities.suiteTest())
testSuite.addTest(testOSUtilities.suiteTest())
testSuite.addTest(testFileUtilities.suiteTest())
testSuite.addTest(testS3Utilities.suiteTest())
testSuite.addTest(testConfigureAWS.suiteTest())

unittest.TextTestRunner(verbosity=2, resultclass=Common.TestCaseResult).run(testSuite)