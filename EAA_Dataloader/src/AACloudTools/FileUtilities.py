'''
#
@summary: File Utilties
# Author - Christopher Lewis
# License: IHS - not to be used outside the company
#
'''
import re
import os
import sys
import time
import logging
import glob

class FileUtilities(object): # pylint: disable=too-many-public-methods
    '''
    a collection of file utilities
    '''
    def __init__(self, logger):
        '''
        constructor
        '''
        self.logger = logger
        self.moduleName = None
        self.localBaseDirectory = None
        self.sqlFolder = None
        self.csvFolder = None
        self.gzipFolder = None
        self.parquet = None

    def ZipMultipleFiles(self, inputFolder, outputFilename, filetype="*"):
        '''
        zip multiple files into one zip file
        '''
        import zipfile
        try:
            retfileList = []
            fileList = self.GetListOfFiles(inputFolder, filetype)
            zpFile = zipfile.ZipFile(outputFilename, 'a')
            for fl in fileList:
                zpFile.write(inputFolder + fl, fl)
                retfileList.append(fl)
            zpFile.close()
            return retfileList
        except:
            self.logger.exception("we had an error in FileUtilites on gzipFile")
            raise

    def GzipMultipleFiles(self, inputFolder, outputFolder, filetype="*"):
        '''
        gzip multiple files one directory based on file ext
        '''
        try:
            fileList = self.GetListOfFiles(inputFolder, filetype)
            retfileList = []
            for fl in fileList:
                zipLocalFilepath = outputFolder + fl + ".gz"
                retfileList.append(zipLocalFilepath)
                self.GzipFile(inputFolder + fl, zipLocalFilepath)
            return retfileList
        except:
            self.logger.exception("we had an error in FileUtilites on GzipMultipleFiles")
            raise

    def GzipFile(self, inputFilename, outputFilename):
        '''
        zips file using gzip
        '''
        import gzip
        import shutil
        try:
            if os.path.isfile(inputFilename):
                with open(inputFilename, 'rb') as fIn:
                    with gzip.open(outputFilename, 'wb') as fOut:
                        shutil.copyfileobj(fIn, fOut)
        except:
            self.logger.exception("we had an error in FileUtilites on gzipFile")
            raise

    def GunzipFile(self, inputFilename, outputFilename):
        '''
        unzips file using gzip
        '''
        import gzip
        import shutil
        try:
            with gzip.open(inputFilename, 'rb') as fIn:
                with open(outputFilename, 'wb') as fOut:
                    shutil.copyfileobj(fIn, fOut)
        except:
            self.logger.exception("we had an error in FileUtilites on gunzipFile")
            raise

    def UnzipUsing7z(self, inputFilename, outputDirectory):
        '''
        unzips file using 7z
        '''
        import platform
        try:
            cmd = "7z e " + inputFilename + " -o" + outputDirectory.strip()
            if platform.system().lower() == "linux":
                cmd = cmd + " 1>/dev/null"
            elif platform.system().lower() == "windows":
                cmd = cmd + " 1>NUL"
                cmd = cmd.replace("/", "\\")
            else:
                self.logger.exception("OS other than Windows/Linux")
            os.system(cmd)
        except Exception:
            self.logger.exception("we had an error in FileUtilites on UnzipUsing7z")
            self.logger.exception("Exception while unzipping the file: {}".format(inputFilename))
            raise

    def UnzipFile(self, inputFilename, outputDirectory):
        '''
        unzips file
        '''
        import zipfile
        try:
            zipRef = zipfile.ZipFile(inputFilename, 'r')
            zipRef.extractall(outputDirectory)
            zipRef.close()
        except:
            self.logger.exception("we had an error in FileUtilites on unzipFile")
            raise

    # Create the actual file from the template by making a copy and then replacing
    # the template token with the real token
    def CreateActualFileFromTemplate(self, templateFile, actualFile, schemaToReplace, tableToReplace):
        '''
            # Create the actual file from the template by making a copy and then replacing
            # the template token with the real token
        '''
        try:
            FileUtilities.RemoveFileIfItExists(actualFile)
            with open(templateFile) as infile, open(actualFile, 'w') as outfile:
                for line in infile:
                    line = line.replace('{schemaName}', schemaToReplace)
                    if tableToReplace:
                        line = line.replace('{tableName}', tableToReplace)
                    outfile.write(line)
        except:
            self.logger.exception("we had an error in FileUtilites on CreateActualFileFromTemplate")
            raise

    def RemoveNonAscii(self, text):
        '''
        converts unicode to ascii
        '''
        try:
            if sys.version[0] == '2':
                from unidecode import unidecode
                text = unidecode(unicode(text, encoding="utf-8"))
        except:
            self.logger.exception("Error while converting unicode character")
            raise
        return text

    def DiscardNonAsciiCharacters(self, text):
        '''
        discards the non-ascii
        '''
        try:
            text = re.sub(r'[^\x00-\x7F]+', '', text)
        except:
            self.logger.exception("Exception while discarding non-ascii characters")
            raise
        return text

    def ReplaceStringInString(self, text, replacements):
        '''
        replacements = {'^':'Add Begin Tag: ', 'name':'Bob', 'address':'US', '$':" Add End Tag"}
        '''
        try:
            if sys.version[0] == '2':
                for src, target in replacements.iteritems():
                    text = re.sub(re.escape(src), re.escape(target), text)
            elif sys.version[0] == '3':
                for src, target in replacements.items():
                    text = re.sub(re.escape(src), re.escape(target), text)
        except:
            self.logger.exception("we had an error in FileUtilites on ReplaceStringInFile")
            raise
        return text

    def RemoveSpecialCharsFromString(self, recordText):
        '''
        removes special characters for a string
        '''
        try:
            recordText = re.sub("\r\n+", " ", recordText) #removes newline
            recordText = re.sub("\n+", " ", recordText) #removes newline
            recordText = re.sub(r"\n", " ", recordText) #removes newline
            recordText = re.sub(r"\r\n", " ", recordText) #removes newline
            recordText = re.sub(r"\xbf", " ", recordText) #removes hex bf
            recordText = re.sub("\t+", " ", recordText) #removes tab
            recordText = re.sub("\\\\x00+", " ", recordText) #removes hex 00
            recordText = re.sub("\\\\xa0+", " ", recordText) #removes hex a0
            recordText = re.sub("\\\\xe9+", " ", recordText) #removes hex e9
            recordText = re.sub("\\\\xae+", " ", recordText) #removes hex ae
            recordText = re.sub("\\\\xe2+", " ", recordText) #removes hex e2
            recordText = re.sub("\\\\xf1+", " ", recordText) #removes hex f1
            recordText = re.sub("\\\\xd1+", " ", recordText) #removes hex d1
            recordText = re.sub("\\\\xa7+", " ", recordText) #removes hex a7
            recordText = re.sub("\x90+", " ", recordText) #removes hex a7
            recordText = re.sub("r[^\x00-\x7F]+", " ", recordText) #removes Replace non-ASCII characters with a single space

            recordText = re.sub("\\\\u[0-9]+", " ", recordText) #removes unicode characters
            recordText = recordText.strip()
        except:
            self.logger.exception("we had an error in FileUtilites on RemoveSpecialCharsFromString")
            raise
        return recordText

    @staticmethod
    def WriteToFile(fileFullPath, text):
        '''
        write the text to a file
        '''
        with open(fileFullPath, "a+") as outFile:
            outFile.write("{}".format(str(text)))

    def ReplaceStringInFile(self, inputFile, outputFile, replacements):
        '''
            #replacements = {'^':'Add Begin Tag: ', 'name':'Bob', 'address':'US', '$':" Add End Tag"}
        '''
        try:
            with open(inputFile) as infile, open(outputFile, 'w') as outfile:
                for line in infile:

                    if sys.version[0] == '2':
                        for src, target in replacements.iteritems():
                            line = re.sub(src, target, line)
                    elif sys.version[0] == '3':
                        for src, target in replacements.items():
                            line = re.sub(src, target, line)
                    outfile.write(line)
        except:
            self.logger.exception("we had an error in FileUtilites on ReplaceStringInFile")
            raise

    def ReplaceIterativelyInFile(self, inputFile, outputFile, dictArray):
        '''
        #Iteratively replaces the mappings given in a list from each line in an input file.
        #This is used when the replacement has to happen in a particular order.
        #Eg, when the newline character has to be replaced before another set of mappings
        #dict_array = [{"\n":""}, {"[~*}": "|", "{~*]": "\n"}]
        '''
        try:
            with open(inputFile, 'r') as infile, open(outputFile, 'w') as outfile:
                for line in infile:
                    for replacements in dictArray:

                        if sys.version[0] == '2':
                            for src, target in replacements.iteritems():
                                line = re.sub(r'[^\x00-\x7F]+', ' ', line) #Get rid of the nasty non-ascii characters
                                line = re.sub(re.escape(src), target, line)
                        elif sys.version[0] == '3':
                            for src, target in replacements.items():
                                line = re.sub(r'[^\x00-\x7F]+', ' ', line) #Get rid of the nasty non-ascii characters
                                line = re.sub(re.escape(src), target, line)
                    outfile.write(line)
        except:
            self.logger.exception("we had an error in FileUtilites on ReplaceIterativelyInFile")
            raise

    def AppendFiles(self, targetFile, sourceFiles):
        '''
        Appends the files in the dictionary, sourceFiles to the targetFile
        This is how sourceFiles argument would look like {"Name": "<full path here>", "IgnoreLines":[1,2,3]}
        '''
        try:
            currentFile = None
            with open(targetFile, "a") as fout:
                for sf in sourceFiles:
                    currentFile = sf["Name"]
                    with open(sf["Name"], "r") as src:
                        lines = src.readlines()
                        lineCounter = 1
                        for line in lines:
                            if lineCounter not in sf["IgnoreLines"]:
                                fout.write(line)
                            lineCounter = lineCounter + 1
        except Exception as ex:
            self.logger.exception("Exception while appending the file: {}".format(currentFile))
            self.logger.exception("{}".format(str(ex)))
            raise

    def LoadJobConfiguration(self, jobFile):
        '''
        load the configuration into the job object
        '''
        import json
        self.logger.info("FileUtilites in LoadJobConfiguration loaded file %s" % (jobFile))
        try:
            with open(jobFile) as jobDefinition:
                job = json.load(jobDefinition)
            return job
        except:
            self.logger.exception("we had an error in FileUtilites on LoadJobConfiguration")
            raise

    #returns the application directory. Eg D:/Data/Git/EAA_Dataloader_Eclipse/src/Applications/Chemicals/
    def GetApplicationDirectory(self, application):
        '''
        returns the current application folder
        '''
        import inspect
        return self.PathToForwardSlash(os.path.dirname(os.path.dirname(inspect.stack()[0][1]))) + "/Applications/" + application + "/"

    #removes sql comments starting with double hyphen--
    def CleanSQLQuery(self, sqlFile):
        '''
        cleans up the sql file for processing
        '''
        self.logger.info("FileUtilites in CleanSQLQuery loaded file %s" % (sqlFile))
        try:
            with open(sqlFile) as sqlQuery:
                sql = re.sub(r'\-+.*', ' ', sqlQuery.read())
                sql = re.sub(r'\s+', ' ', sql) #replace all continuous spaces with a single space
                sql = [query.strip() + ";" for query in sql.split(";") if query.strip() != ""]
            return sql
        except:
            self.logger.exception("we had an error in FileUtilites on CleanSQLQuery")
            raise

    def LoadSQLQuery(self, sqlFile):
        '''
        gets the sql file ready to process
        '''
        self.logger.info("FileUtilites in LoadSQLQuery loaded file %s" % (sqlFile))
        try:
            sql = self.CleanSQLQuery(sqlFile)[0] #CleanSQLQuery returns a list. Take the first item in the list
            sql = sql.rstrip(";") #removes the trailing semicolon
            return sql
        except:
            self.logger.exception("we had an error in FileUtilites on LoadSQLQuery")
            raise

    @staticmethod
    def CreateFolder(logFolder):
        '''
        creates a new folder if it does not already exists
        '''
        try:
            os.makedirs(logFolder, 0o777)
        except OSError:
            pass

    @staticmethod
    def CreateFolder2(folderPath):
        '''
        creates a new folder if it does not already exists
        '''
        try:
            os.makedirs(folderPath)
        except OSError:
            pass

    @staticmethod
    def RemoveFileIfItExists(fileName):
        '''
        removes a file if it exists
        '''
        if os.path.exists(fileName):
            os.remove(fileName)

    @staticmethod
    def PathToForwardSlash(path):
        '''
        replaces a back slash with a forward slash
        '''
        return path.replace('\\', '/') # Convert all the back slashes to forward slashes

    @staticmethod
    def PathToBackwardSlash(path):
        '''
        replaces the forward slash with a back slash
        '''
        return path.replace('/', '\\') # Convert all the forward slashes to back slashes

    @staticmethod
    def CreateLogger(logFolder, dbLevel=logging.DEBUG):
        '''
        creates/sets up a logger
        '''
        ###
        #  create log file name and save off the current date too
        ###
        today = time.strftime('%Y-%m-%d')
        FileUtilities.CreateFolder(logFolder)
        loggingfilename = logFolder + "/" + today + "_applog.log"

        ###
        #  set up logging
        ###
        # Set up a specific logger with our desired output level
        formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
        logger = logging.getLogger('url stuff')
        logger.setLevel(dbLevel)

        # Create handler that will output to the log file
        logfileHandler = logging.FileHandler(loggingfilename)
        logfileHandler.setFormatter(formatter)
        logger.addHandler(logfileHandler)

        # Create handler that will output to the standard output as well
        stdoutHandler = logging.StreamHandler()
        stdoutHandler.setFormatter(formatter)
        logger.addHandler(stdoutHandler)

        return logger

    @staticmethod
    def GetFileContents(filepath):
        '''
        gets the contents of a file
        '''
        with open(filepath, "r") as dataFile:
            content = dataFile.readlines()
        return content

    @staticmethod
    def GetFileSize(filepath):
        '''
        returns the size of a file
        '''
        return len(FileUtilities.GetFileContents(filepath))

    @staticmethod
    def FilesExistInFolder(pattern):
        '''
        check if file exist
        '''
        import glob
        nFiles = len(glob.glob(pattern))
        return nFiles > 0

    def GetListOfFiles(self, filepath, filetype="*"):
        '''
        Returns a list of files. Default value for filetype is "*", which returns all files
        Other use cases are: self.fileUtilities.GetListOfFiles(filePath, "*.csv") to return the list of CSV files
        '''
        try:
            listOfFiles = [os.path.basename(x) for x in glob.glob(filepath + filetype)]
        except OSError:
            self.logger.exception("Error occurred in FileUtilites.GetListOfFiles")
            raise
        return listOfFiles

    def GetListOfFilesRecursively(self, filepath, filetype="*"):
        '''
        Returns a list of files recursively. Default value for filetype is "*", which returns all files
        Other use cases are: self.fileUtilities.GetListOfFilesRecursively(filePath, "*.csv") to return the list of CSV files
        '''
        import fnmatch
        fileNames = []
        try:
            for root, dirs, files in os.walk(filepath):  # pylint: disable=unused-variable
                root = root.replace("\\", "/")
                for items in fnmatch.filter(files, filetype):
                    fileName = root + "/" + items
                    fileNames.append(fileName)
        except OSError:
            self.logger.exception("Error occurred in FileUtilites.GetListOfFilesRecursively")
            self.logger.exception("Exception while handling the file {}".format(fileName))
            raise
        return fileNames

    @staticmethod
    def EmptyFolderContents(path):
        '''
        Delete and recreate a folder. This is the fastest way to delete all the files & directories in a given path
        '''
        FileUtilities.RemoveFolder2(path)
        FileUtilities.CreateFolder(path)

    @staticmethod
    def RemoveHeader(inputFilepath, outputFilepath, fp):
        '''
        creates a file without a header
        '''
        FileUtilities.CreateFolder(outputFilepath[:-1])
        with open(inputFilepath + fp, "r") as dataFile:
            content = dataFile.readlines()
            with open(outputFilepath + fp, "w") as outFile:
                outFile.writelines(content[1:])

    @staticmethod
    def RemoveFolder2(folderName):
        '''
        remove a folder if it exist
        '''
        import shutil
        if os.path.exists(folderName):
            shutil.rmtree(folderName)

    @staticmethod
    def RemoveFolder(folderName):
        '''
        remove a folder if it exist
        '''
        import shutil
        if os.path.exists(folderName):
            os.chmod(folderName, 0o777)
            shutil.rmtree(folderName)

    def AppendFilesUsingCopyCommand(self, inputFolderPath, outputFilePath, fileType="*.CSV"):
        '''
        Combines CSV or other files into one combined file
        eg: on windows, copy *.csv combined.csv
        on linux, cp *.csv combined.csv
        This method also create a rogue character ("SUB") at the end of the output file.
        That will be removed after the file has been created
        '''
        self.logger.info("Appending {} into: {}".format(outputFilePath, outputFilePath))
        import platform
        try:

            if platform.system().lower() == "linux":
                cmd = "cat \"" + inputFolderPath.strip() + fileType.strip() + "\" > \"" + outputFilePath.strip() + "\""
            elif platform.system().lower() == "windows":
                cmd = "copy \"" + inputFolderPath.strip() + fileType.strip() + "\" \"" + outputFilePath.strip() + "\"" + " 1>NUL"
                cmd = cmd.replace("/", "\\")
            else:
                self.logger.exception("OS other than Windows/Linux")
            os.system(cmd)
            self.RemoveLastLineInFile(outputFilePath)
        except OSError as oe:
            self.logger.exception("Exception in AppendFilesUsingCopyCommand")
            self.logger.exception("Input folder path: {}".format(inputFolderPath))
            self.logger.exception(str(oe))

    def RemoveBlankLines(self, filePath, outputFile):
        '''
        Removes blank lines & empty strings ("") from files
        Returns the number of non empty lines written to the output file
        '''
        self.logger.info("Inside FileUtilities.RemoveBlankLines")
        try:
            lineCounter = 0
            with open(filePath, "r") as fopen, open(outputFile, "w") as fout:
                for line in fopen:
                    line = line.strip()
                    if (line != "") & (line != '""'):
                        fout.write(line + "\n")
                        lineCounter = lineCounter + 1
        except Exception:
            self.logger.exception("Exception in FileUtilities.RemoveBlankLines")
            self.logger.exception("Exception while removing blank lines from file {}".format(filePath))
            raise

    @staticmethod
    def RemoveLastLineInFile(filePath):
        '''
        Removes the last line from a given file.
        A special character "SUB" shows up at the end of the target file that get created by the following command
        copy *.csv combined.csv
        '''
        with open(filePath, "r") as fread:
            lines = fread.readlines()
            lines = lines[:-1]
        with open(filePath, "w") as fwrite:
            fwrite.writelines(lines)

    @staticmethod
    def PutLine(nString, ofile):
        '''
        just puts the line with a cr lf to the file for readability
        '''
        ofile.write(nString)
        ofile.write('\n')
        return ''

    def ScanFolder(self, folder, fileName=None, ext=None):
        '''
        scans a folder for files matching parameters
        '''
        fileList = []
        try:
            if folder is None:
                raise "You must supply a folder to scan"

            if fileName != None:
                self.logger.debug("FileUtilites in scanFolder for file %s" % (fileName))
                for fl in os.listdir(folder):
                    if fileName in fl:
                        fileList.append(fl)
                        return fileList
            elif  ext != None:
                self.logger.debug("FileUtilites in scanFolder for all files with extension %s" % (ext))
                tExt = ""
                if ext.startswith("."):
                    tExt = ext
                else:
                    tExt = ".%s" % (ext)
                for fl in os.listdir(folder):
                    if fl.endswith(tExt):
                        fileList.append(fl)
            else:
                for fl in os.listdir(folder):
                    fileList.append(fl)

            return fileList
        except:
            self.logger.exception("we had an error in FileUtilites on ScanFolder")
            raise

    def SkipLinesInFile(self, inputFileFullPath, outputFileFullPath, ignoreLines):
        '''
        Skip lines and return the content
        '''
        self.logger.info("Skipping lines in file {}".format(inputFileFullPath))
        try:
            content = FileUtilities.GetFileContents(inputFileFullPath)
            with open(outputFileFullPath, "w") as cleanFile:
                lineCounter = 1
                for line in content:
                    if lineCounter not in ignoreLines: #ignore metadata
                        line = line.strip() #removes whitespace from start/end
                        cleanFile.write(line + "\n")
                    lineCounter = lineCounter + 1
        except Exception as ex:
            self.logger.exception(str(ex))
            raise
        return outputFileFullPath

    def SkipLastLine(self, inputFileFullPath):
        '''
        Skip last line, usually the footnote
        '''
        self.logger.info("Skipping the last line in file {}".format(inputFileFullPath))
        try:
            content = FileUtilities.GetFileContents(inputFileFullPath)
            content = content[0:len(content)-1]
            with open(inputFileFullPath, "w") as cleanFile:
                cleanFile.writelines(content)
        except Exception as ex:
            self.logger.exception(str(ex))
            raise

    def CleanFile(self, inputPath, outputPath, **kwargs):
        '''
        inputPath: full path to the input file
        outputPath: full path to the output file
        kwargs: variable length named argument to the function.
        Example call to this function will look like the following:
        CleanFile("D:/data/pgcr/inputFile.csv", "D:/data/pgcr/outputFile.csv", IgnoreLines=[1,2,3], ColumnCount=13, Delimiter="|")
        '''
        ignoreLines = []
        columnCount = None
        delimiter = None

        if kwargs.get("IgnoreLines") is not None:
            ignoreLines = kwargs["IgnoreLines"]
        if kwargs.get("ColumnCount") is not None:
            columnCount = kwargs["ColumnCount"]
        if kwargs.get("Delimiter") is not None:
            delimiter = kwargs["Delimiter"]

        try:
            content = FileUtilities.GetFileContents(inputPath)
            with open(outputPath, "w") as cleanFile:
                lineCounter = 1
                for line in content:
                    if lineCounter not in ignoreLines: #ignore lines
                        line = line.strip() #removes whitespace from start/end

                        #if the last character is a comma and number of columns is not what is expected
                        if (delimiter is not None) and (columnCount is not None):
                            if (line[-1:] == delimiter) and (len(line.split(delimiter)) == int(columnCount)+1):
                                line = line[:-1] #discard the last character, the delimiter
                        cleanFile.write(line + "\n")
                    lineCounter = lineCounter + 1
        except Exception as ex:
            self.logger.exception(str(ex))
            raise

    def GetImmediateSubdirectories(self, directory):
        '''
        Converts comma separated files to pipe separated files
        '''
        try:
            return [name for name in os.listdir(directory)
                    if os.path.isdir(os.path.join(directory, name))]
        except:
            self.logger.exception("Error in finding Subdirectories")
            raise

    def CreateFile(self, filePath):
        '''
        Creates a file in the given file path
        '''
        try:
            fp = open(filePath, "w")
            fp.close()
        except:
            self.logger.exception("Error in CreateFile")
            raise

    def DeleteFile(self, filePath):
        '''
        Deletes the file in the given file path.
        '''
        try:
            os.remove(filePath)
        except:
            self.logger.exception("Error in DeleteFile")
            raise

    def ConvertToPipeDelimitedFile(self, inputPath, outputPath, **kwargs):
        '''
        Converts comma separated files to pipe separated files
        '''
        import pandas as pd

        excludeColumns = []
        delimiter = None

        if kwargs.get("ExcludeColumns") is not None:
            excludeColumns = kwargs["ExcludeColumns"]
        if kwargs.get("Delimiter") is not None:
            delimiter = kwargs["Delimiter"]

        try:
            df = pd.read_csv(inputPath)
            for column in excludeColumns:
                if column in df.columns: #check if the column is present in the dataframe
                    df = df.drop(str(column), 1) #drop the junk columns

            df.to_csv(outputPath,
                      sep=delimiter,
                      na_rep="",
                      header=False,
                      index=False)
        except:
            self.logger.exception("Error converting {} to PipeDelimited File!".format(inputPath))
            raise

    @staticmethod
    def GetLineCount(filePath):
        '''
        Returns the linecount in a given file
        Parameters:
            filePath = The file to work with.
        '''
        lineCount = 0
        with open(filePath, "r") as fread:
            lineCount = len(fread.readlines())
            
#         return sum(1 for line in open(filePath))
        return lineCount

    @staticmethod
    def RemoveLines(filePath, aLines):
        '''
        Removes a list of line numbers from a file.
        Parameters:
            filePath = The file to work with.
            aLines = List of integers which represent the line numbers to be removed.
                     *Note: Use -1 to remove the last one.
        '''
        totalDel = 0

        with open(filePath, "r") as fread:
            lines = fread.readlines()

        for ln in aLines:
            if ln > -1:
                idx = ln-1
                idx = idx - totalDel
                totalDel = totalDel + 1
            else:
                idx = ln

            lines.pop(idx)

        with open(filePath, "w") as fwrite:
            fwrite.writelines(lines)

    @staticmethod
    def ComposeCreateTableSqlFilename(tableSettings, fileLoc):
        '''
        Compose the file name for sql script
        '''
        if fileLoc.endswith('/') is False:
            fileLoc = fileLoc + '/'
        fname = fileLoc + "Create_" + tableSettings["table"] + ".sql"
        return fname

    def CreateTableSql(self, tableSettings, fileLoc):  # pylint: disable=too-many-branches
        '''
        process to create the SQL to create tables if you pass in a valid json nugget to describe what you need
        @param tableSetting  -- json nugget describing the layout of the table:
        @param fileLoc: a json nugget that indicates where files are located and ends with a '/' so it just
                        appends the name of the file created
        Example: this is the ETL logging table
       "tblEtl": {
            "schemaName": "eaa_dev",
            "table": "etl_process_logs",
            "new": "N",
            "fields": [
                { "name": "processid", "type": "IDENTITY", "size": "0,1" },
                { "name": "appname", "type": "VARCHAR", "size": "200" },
                { "name": "startDate", "type": "TIMESTAMP" },
                { "name": "endDate", "type": "TIMESTAMP" },
                { "name": "params", "type": "VARCHAR", "size": "max" },
                { "name": "status", "type": "VARCHAR", "size": "1" },
                { "name": "recsinserted", "type": "BIGINT"},
                { "name": "recsmodified", "type": "BIGINT" },
                { "name": "recsdeleted", "type": "BIGINT" },
                { "name": "etl_run_date", "type": "DATE", "isPartitioned": "Y", "athenaOnly": "Y"} # For Athena partitioning.
            ]
        }
        '''

        fname = FileUtilities.ComposeCreateTableSqlFilename(tableSettings, fileLoc)
        outfile = open(fname, "w")
        try:
            if tableSettings["new"] == "Y":
                outLine = "DROP TABLE IF EXISTS {}.{};".format(tableSettings["schemaName"], tableSettings["table"])
                outLine = FileUtilities.PutLine(outLine, outfile)
                outLine = "CREATE TABLE {}.{} (".format(tableSettings["schemaName"], tableSettings["table"])
                outLine = FileUtilities.PutLine(outLine, outfile)
            else:
                outLine = "CREATE TABLE IF NOT EXISTS {}.{} (".format(tableSettings["schemaName"], tableSettings["table"])
                outLine = FileUtilities.PutLine(outLine, outfile)
            ndx = 0
            for fld in tableSettings["fields"]:
                # Skip the field if it is for Athena management
                if "athenaOnly" in fld and fld["athenaOnly"] == "Y":
                    continue

                if ndx > 0:
                    outLine = ','
                ndx = ndx + 1
                fldName = fld["name"]
                if fldName == 'NEW':
                    fldName = fldName + "_RSWD"
                fieldtype = fld["type"]
                if fld["type"] == "IDENTITY":
                    fieldtype = " bigint " + fld["type"]
                outLine = outLine + fldName + " " + fieldtype
                if fld["type"] == "VARCHAR":
                    outLine = outLine + "(" + fld["size"] + ")  ENCODE ZSTD"
                elif fld["type"] == "IDENTITY":
                    outLine = outLine + "(" + fld["size"] + ")"
                elif fld["type"] == "DECIMAL":
                    outLine = outLine + "(" + fld["size"] + ")"
                elif fld["type"] == "DATE" or fld["type"] == "FLOAT4" or fld["type"] == "CHAR" or fld["type"] == "REAL":
                    outLine = outLine + " ENCODE ZSTD"
                outLine = FileUtilities.PutLine(outLine, outfile)
            outfile.write(")")
            if "distkey" in tableSettings:
                outLine = outLine + " DISTKEY(" + tableSettings["distkey"] + ")"
                outLine = FileUtilities.PutLine(outLine, outfile)
            if "sortkey" in tableSettings:
                outLine = outLine + " SORTKEY(" + tableSettings["sortkey"] + ")"
                outLine = FileUtilities.PutLine(outLine, outfile)
            outfile.write(";")
        except:   # pylint: disable=bare-except
            self.logger.exception("problem creating table SQL for " +
                                  tableSettings["schemaName"] + '.' + tableSettings["table"])
        finally:
            outfile.close()
        return fname

    def CreateLocalFolder(self, folder):
        '''
        if a folder needs to be created it does it and if we want a fresh folder it will do that to
        '''
        try:
            fName = folder["name"]
            tfoldername = self.localBaseDirectory + "/" + folder["folder"] + "/"
            if fName == "sql":
                self.sqlFolder = tfoldername
            elif fName == "csv":
                self.csvFolder = tfoldername
            elif fName == "gzips":
                self.gzipFolder = tfoldername
            elif fName == "parquet":
                self.parquet = tfoldername
            if folder["new"] == "Y":
                self.RemoveFolder(tfoldername)

            self.CreateFolder(tfoldername)
        except:
            self.logger.exception(self.moduleName + " had an issue in CreateFolder for " + folder)
            raise

    def CreateFolders(self, folders):
        '''
        create all the local folders we will need
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "CreateFolders" + " starting ")
            for fld in folders:
                self.CreateLocalFolder(fld)
            self.logger.debug(self.moduleName + " -- " + "CreateFolders" + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in CreateFolders")
            raise
        
    def UnpackFile(self, topfolderName, availableExts):
        '''
        routine to unpack the contents of a zipped file based on the extension
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "UnpackFile" + " starting ")

            ndx = 1
            while ndx != 0:
                ndx = 0
                for fExt in availableExts:
                    flist = self.GetListOfFiles(topfolderName + '/', fExt["name"].encode())
                    for fname in flist:
                        fnameLocation = topfolderName + '/' + fname
                        self.UnzipUsing7z(fnameLocation, topfolderName)
                        self.DeleteFile(fnameLocation)
                        ndx = 1
            self.logger.debug(self.moduleName + " -- " + "UnpackFile" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetFileList")
            raise

    def FindMostCurrentFile(self, folderPath):
        '''
        returns the name of the latest file in a folder
        '''
        retVal = None
        try:
            self.logger.debug(self.moduleName + " -- " + "scan folder " + folderPath + " starting ")
            retVal = max(glob.glob(folderPath + '/*'), key=os.path.getctime)
            self.logger.debug(self.moduleName + " -- " + "scan folder " + folderPath + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in : GetMostRecentFile using folder " + folderPath)
            raise
        return retVal
    
    def MoveFilesFromOneFolderToAnother(self, srcPath, destPath, filePattern):
        '''
        moves all the files form one folder to another folder
        '''   
        import shutil
        fileList = self.GetListOfFiles(srcPath, filePattern)
        for fileName in fileList:
            srcFile = srcPath + fileName
            dstFile = destPath + fileName
            shutil.move(srcFile, dstFile)

    @staticmethod
    def DownloadFromURL(url, localFilepath):
        '''
        Download the data from a URL
        '''
        import urllib
        if sys.version[0] == '2':
            fileDownload = urllib.URLopener()
            fileDownload.retrieve(url, localFilepath)
        elif sys.version[0] == '3':
            fileDownload = urllib.request.urlretrieve(url, localFilepath)  #@UnresolvedImport
            
    @staticmethod
    def DownloadFromURLUserPassword(url, localFilepath, user, pwd):
        '''
        Download the data from a URL given a username and password
        '''
        import urllib2, base64
        request = urllib2.Request(url)
        base64string = base64.encodestring('%s:%s' % (user, pwd)).replace('\n', '')
        request.add_header("Authorization", "Basic %s" % base64string)   
        response = urllib2.urlopen(request)
        contents = response.read()
        with open(localFilepath,'wb') as f:
            f.write(contents)