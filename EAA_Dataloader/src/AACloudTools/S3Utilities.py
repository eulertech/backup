'''
# S3 Utilties
@author: Christopher Lewis
@license: IHS - not to be used outside the company
@change: added new method to get a list of file and modified for coding standards
'''
import boto
from AACloudTools.DatetimeUtilities import DatetimeUtilities

class S3Utilities(object):
    '''
    Download file from S3 to the local directory
    '''
    @staticmethod
    def DownloadFileFromS3(s3, bucketName, s3Key, localFilename):
        '''
        Downloads a file from S3 to a local folder
        '''
        from boto.s3.key import Key
        conn = boto.connect_s3(s3['access_key_id'], s3['secret_access_key'])
        bucket = conn.get_bucket(bucketName)

        #Get the Key object of the given key, in the bucket
        k = Key(bucket, s3Key)

        #Get the contents of the key into a file
        k.get_contents_to_filename(localFilename)

    @staticmethod
    def CopyItemsAWSCli(sourcePath, destinationPath, args="", dbug="N"):
        '''
        Download a specific file or a bucket to a local folder, arguments can be specified with args
        '''
        import os

        cmd = "aws s3 cp " + sourcePath + " " + destinationPath + " " + args
        if dbug == 'Y':
            print(cmd)
        os.system(cmd)

    @staticmethod
    def SyncFolderAWSCli(sourcePath, destinationPath, delete=False, args="", dbug="N"):
        '''
        Syncronize a folder's content, this can be both ways, local to s3 or from s3 to local.

        sourcePath = Can be a local folder or a s3 folder where the files you want to move are.
        destinationPath = Can be a local folder or a s3 folder where you want to move the files to.
        delete = To delete files that does not exist in the sourcePath. Default is false.
        '''
        import os

        delStatement = ""

        if delete is True:
            delStatement = " --delete"
        cmd = "aws s3 sync " + sourcePath + " " + destinationPath + delStatement + " " + args
        if dbug == 'Y':
            print(cmd)
        os.system(cmd)

    @staticmethod
    def S3Copy(src, dst, args=""):
        '''
        Uploads a folder on the local drive to an S3 location
        '''
        import os

        cmd = "aws s3 cp " + src + " " + dst + args
        os.system(cmd)

    @staticmethod
    def S3RecursvieCopy(srcFolder, destFolder, args=""):
        '''
        Uploads a folder on the local drive to an S3 location
        '''
        import os

        cmd = "aws s3 cp " + srcFolder + " " + destFolder + " --recursive " + args
        os.system(cmd)

    @staticmethod
    def DeleteFileFromS3TempUsingAWSCLi(s3FullPath, args=""):
        '''
        Deletes files from a location on S3 using the CLI method, arguments can be specified with args
        '''
        import os

        cmd = "aws s3 rm " + s3FullPath + " " + args
        os.system(cmd)

    @staticmethod
    def GetS3FileName(bucketName, s3TempKey):
        '''
        returns a file name from S3
        '''
        return "s3://" + bucketName + "/" + s3TempKey

    @staticmethod
    def UploadFileToS3(s3, localFilename, bucketName, s3Key):
        '''
        uploads a file to a specific S3 folder
        '''
        from boto.s3.key import Key
        conn = boto.connect_s3(s3['access_key_id'], s3['secret_access_key'])
        bucket = conn.get_bucket(bucketName)

        #Get the Key object of the given key, in the bucket
        k = Key(bucket, s3Key)

        k.set_contents_from_filename(localFilename)

    @staticmethod
    def UploadFileToS3Temp(s3, filepath):
        '''
        uploads a file to the temp area
        '''
        import os
        import ntpath
        user = os.environ.get("USER", "")
        if not user:
            user = os.environ.get("USERNAME", "")

        # Load file to S3 at a temporary location
        bucketName = "ihs-temp"
        s3TempKey = "eaa/" + ntpath.basename(os.getcwd()) + "/temp/" + user + "/" + ntpath.basename(filepath)
        S3Utilities.UploadFileToS3(s3, filepath, bucketName, s3TempKey)

        return bucketName, s3TempKey

    @staticmethod
    def DeleteFile(s3, bucketName, s3Key):
        '''
        deletes a file from S3
        '''
        from boto.s3.key import Key
        conn = boto.connect_s3(s3['access_key_id'], s3['secret_access_key'])
        bucket = conn.get_bucket(bucketName)

        #Get the Key object of the given key, in the bucket
        k = Key(bucket, s3Key)
        bucket.delete_key(k)

    @staticmethod
    def GetListOfFiles(s3, bucketName, s3Folder):
        '''
        get a list of the bucket contents
        '''
        conn = boto.connect_s3(s3['access_key_id'], s3['secret_access_key'])
        bucket = conn.get_bucket(bucketName)
        retVal = []
        for key in bucket.list(prefix=s3Folder):
            retVal.append(key.name)
        return retVal

    @staticmethod
    def GetFilesSinceGivenDatetime(s3Bucket, s3Path, lastModifiedDatetime):
        '''
        Returns the list of files on S3 that has date greater than the parameter lastModifiedDateStr
        s3Bucket to be passed in the format (string): "ihs-bda-data"
        s3Path to be passed in the format (string): "/projects/Pgcr_WindDashboard/ERCOT/"
        '''
        import os
        import re
        if not s3Path.startswith("/"):
            s3Path = "/" + s3Path.strip()

        if not s3Path.endswith("/"):
            s3Path = s3Path + "/"

        cmd = "aws s3 ls s3://" + s3Bucket + s3Path + " --recursive"
        listing = os.popen(cmd).read().split("\n")
        listing = [ls.strip() for ls in listing if len(ls.strip()) > 0] #remove blank lines
        modifiedFiles = []
        for item in listing:
            dtpart = str(item[0:20]) #get the first 19 characters
            dtpart = re.sub(r"\s+", " ", dtpart.strip()) #replace rogue space characters with a single space
            dt = DatetimeUtilities.ConvertToUTC(dtpart)
            if dt > lastModifiedDatetime:
                fileDict = {}
                filename = item.split(" ")[-1]
                fileDict["fileName"] = filename
                fileDict["datetime"] = str(DatetimeUtilities.ConvertToUTC(dtpart))
                modifiedFiles.append(fileDict)
        return modifiedFiles
    
    
    @staticmethod
    def GetFilesNModifiedDatetimeFromS3(s3Bucket, s3Path):
        '''
        Returns the list of files on S3 that has date greater than the parameter lastModifiedDateStr
        lastModifiedDateStr to be passed in the format (string): "2017-06-07 20:33:26"
        s3Bucket to be passed in the format (string): "ihs-bda-data"
        s3Path to be passed in the format (string): "/projects/Pgcr_WindDashboard/ERCOT/"
        '''
        import os
        import re
        if not s3Path.startswith("/"):
            s3Path = "/" + s3Path.strip()

        if not s3Path.endswith("/"):
            s3Path = s3Path + "/"

        cmd = "aws s3 ls s3://" + s3Bucket + s3Path + " --recursive"
        listing = os.popen(cmd).read().split("\n")
        listing = [ls.strip() for ls in listing if len(ls.strip()) > 0] #remove blank lines
        modifiedFiles = []
        for item in listing:
            dtpart = str(item[0:20]) #get the first 19 characters
            dtpart = re.sub(r"\s+", " ", dtpart.strip()) #replace rogue space characters with a single space
            fileDict = {}
            filename = item.split(" ")[-1]
            fileDict["fileName"] = filename
            fileDict["datetime"] = str(DatetimeUtilities.ConvertToUTC(dtpart))
            modifiedFiles.append(fileDict)
        return modifiedFiles

    @staticmethod
    def KeyExist(awsParams, s3Key):
        '''
        Check if a key exists
        '''
        import boto3
        client = boto3.client('s3', aws_access_key_id=awsParams.athena["access_key_id"],
                              aws_secret_access_key=awsParams.athena["secret_access_key"])
        
        pathNoS3 = s3Key.split("//", 1)[1]
        parts = pathNoS3.split("/", 1)
        bucketName = parts[0]
        prefix = parts[1]
        response = client.list_objects_v2(Bucket=bucketName, Prefix=prefix)
        exists = "Contents" in response
        return exists
