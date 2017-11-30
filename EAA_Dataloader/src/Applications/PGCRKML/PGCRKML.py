'''
Created on Jul 25, 2017
@author: Varun
License: IHS - not to be used outside the company
'''

import os
import re
import xml.dom.minidom

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase


class PGCRKML(ApplicationBase):
    '''
    Code to process the PGCR KML files
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(PGCRKML, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))
        self.awsParams = ""
        self.packedFolder = None

    def DownloadFiles(self):
        '''
        Download the XML files
        '''
        fileList = S3Utilities.GetListOfFiles(self.awsParams.s3, self.job["bucketName"], self.job["s3SrcDirectory"][1:])
        downloadedFiles = []
        for fl in fileList:
            fileName = fl.split("/")[-1]
            s3Key = "/" + fl
            outputPath = self.localTempDirectory + "/" + fileName
            S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"], s3Key, outputPath)
            downloadedFiles.append(outputPath)
        return downloadedFiles

    def GetName(self, placemark):
        '''
        Extract Name from the placemark tag
        '''
        try:
            name = placemark.getElementsByTagName("name")[0].childNodes[0].data
        except:
            self.logger.exception("Error retrieving name from placemark: {}".format(placemark))
            raise
        return name

    def GetDescription(self, placemark):
        '''
        Extract Description from the placemark tag
        '''
        try:
            description = placemark.getElementsByTagName("description")[0].childNodes[0].data
            description = description.replace("\n", "").replace("'", "")
            description = re.sub(r"\s{2,}", " ", description)
        except:
            self.logger.exception("Error retrieving description from placemark: {}".format(placemark))
            raise
        return description

    def GetCoordinates(self, placemark):
        '''
        Extract lat/long from the placemark tag
        '''
        try:
            point = placemark.getElementsByTagName("Point")
            coordinates = point[0].getElementsByTagName("coordinates")[0].childNodes[0].data
            latitude = coordinates.split(",")[1].strip()
            longitude = coordinates.split(",")[0].strip()
        except:
            self.logger.exception("Error retrieving coordinates from placemark: {}".format(placemark))
            raise
        return (latitude, longitude)


    def GetSettlementPoint(self, match):
        '''
        Extract SettlementPoint from the match object
        '''
        try:
            settlementPoint = match.group(1).strip()
        except:
            self.logger.exception("Error retrieving SettlementPoint")
            raise
        return settlementPoint

    def GetPlantName(self, match):
        '''
        Extract plantName from the match object
        '''
        try:
            plantName = match.group(2).strip()
        except:
            self.logger.exception("Error retrieving plantName")
            raise
        return plantName

    def GetPlantAddress(self, match):
        '''
        Extract plantAddress from the match object
        '''
        try:
            plantAddress = match.group(3).replace("<br>", ", ").strip()
        except:
            self.logger.exception("Error retrieving plantAddress")
            raise
        return plantAddress

    def GetCounty(self, match):
        '''
        Extract county from the match object
        '''
        try:
            county = match.group(4).strip()
        except:
            self.logger.exception("Error retrieving county")
            raise
        return county

    def GetUtility(self, match):
        '''
        Extract utility from the match object
        '''
        try:
            utility = match.group(5).strip()
        except:
            self.logger.exception("Error retrieving utility")
            raise
        return utility

    def GetPlantDetails(self, placemark):
        '''
        Extract wrapper for SettlementPoint, utility, county, plantAddress, plantName from the placemark tag
        '''
        try:
            plantDetails = {}
            plantDetails["name"] = self.GetName(placemark)
            latitude, longitude = self.GetCoordinates(placemark)
            plantDetails["latitude"] = latitude
            plantDetails["longitude"] = longitude
            match = re.search(self.job["RegexExpr"], self.GetDescription(placemark))
            plantDetails["settlementPoint"] = self.GetSettlementPoint(match)
            plantDetails["plantName"] = self.GetPlantName(match)
            plantDetails["plantAddress"] = self.GetPlantAddress(match)
            plantDetails["county"] = self.GetCounty(match)
            plantDetails["utility"] = self.GetUtility(match)
        except Exception:
            self.logger.exception("Error while processing placemark: {}".format(placemark))
            raise
        return plantDetails


    def ProcessFiles(self, downloadedFiles):
        '''
        Process the XML files
        '''
        counter = 1
        for xmlFile in downloadedFiles:
            collection = xml.dom.minidom.parse(xmlFile).documentElement
            fileFullPath = self.localTempDirectory + "/kml" + str(counter) + ".csv"
            self.fileUtilities.CreateFile(fileFullPath)
            for folder in collection.getElementsByTagName("Folder"):
                if folder.getElementsByTagName("name")[0].childNodes[0].data == "LMP Points": #name tag
                    for placemark in folder.getElementsByTagName("Placemark"):
                        plantDetails = self.GetPlantDetails(placemark)
                        line = "{}|{}|{}|{}|{}|{}|{}|{}\n".format(plantDetails["name"],
                                                                  plantDetails["settlementPoint"],
                                                                  plantDetails["plantName"],
                                                                  plantDetails["plantAddress"],
                                                                  plantDetails["county"],
                                                                  plantDetails["utility"],
                                                                  plantDetails["latitude"],
                                                                  plantDetails["longitude"])
                        FileUtilities.WriteToFile(fileFullPath, line)
            self.PackFile(fileFullPath)
            self.fileUtilities.DeleteFile(xmlFile)
            counter = counter + 1

    def PackFile(self, inputFilePath):
        '''
        Converts the cleaned CSV files into gz.
        Deletes the original CSV file
        '''
        csvFile = inputFilePath.split("/")[-1]
        self.fileUtilities.GzipFile(inputFilePath, self.packedFolder + csvFile + ".gz")
        self.fileUtilities.DeleteFile(inputFilePath)

    def UploadPackedToS3(self):
        '''
        Uploads all files packed to s3.
        '''
        self.logger.info("Uploading GZIP files to s3 folder...")
        S3Utilities.CopyItemsAWSCli(self.packedFolder,
                                    "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"],
                                    "--recursive --quiet")

    def LoadTables(self):
        '''
        Performs the final step to insert multiple files located in s3 into the final table in Redshift.
        '''
        try:
            s3DataFolder = "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"]

            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": self.job["destinationSchema"],
                                                 "tableName": self.job["tableName"] + "plant_details",
                                                 "s3Filename": s3DataFolder,
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": self.job["delimiter"]
                                             },
                                             self.logger, "N")

            self.logger.info("Cleaning s3 data folder...")
            S3Utilities.DeleteFileFromS3TempUsingAWSCLi(s3DataFolder, "--recursive --quiet")
        except Exception:
            self.logger.error("Error while trying to save into Redshift from s3 folder.")
            raise
        finally:
            if rsConnect is not None:
                rsConnect.close()

    def CreatePackedFolder(self):
        '''
        Create the folder to store packed files
        '''
        FileUtilities.CreateFolder2(self.localTempDirectory + "/packedFolder/")
        self.packedFolder = self.localTempDirectory + "/packedFolder/"

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.CreatePackedFolder()
            fileList = self.DownloadFiles()
            self.ProcessFiles(fileList)
            self.UploadPackedToS3()
            self.LoadTables()
        except Exception:
            logger.exception(moduleName + " - Exception!")
            raise
