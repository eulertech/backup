'''
@author: VIU53188
@summary: ETL Logging Utilities -- This is a collection of methods that will be used for tracking run processes
@license: IHS Markit - not to used outside of the company
@change: June 27, 2017 -- Initial creation

'''
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities

class EtlLoggingUtilities(object):
    '''
    A collection of utilities for updating and accessing ETL Processes
    '''
    def __init__(self, logger):
        '''
        Constructor
        '''
        self.logger = logger
        self.awsParams = {}
        self.filelocs = {}
        self.moduleName = None
        self.appschema = None
        self.etlSchema = None

    @staticmethod
    def GetAWSConnection(awsJson):
        '''
        because we do this multiple times it just makes sense to create a method for this
        '''
        rsConnect = RedshiftUtilities.Connect(dbname=awsJson.redshift['Database'],
                                              host=awsJson.redshift['Hostname'],
                                              port=awsJson.redshift['Port'],
                                              user=awsJson.redshiftCredential['Username'],
                                              password=awsJson.redshiftCredential['Password'])
        return rsConnect

    def GetStartInstanceSQL(self, tblEtlTable, appmodule):
        '''
        gets the sql to use to initiate the instance for the app
        '''
        try:
            tname = self.etlSchema + '.' + tblEtlTable
#            bstate = '''begin;
#                        lock {};
#            '''
            bstate = '''begin;
            '''

#            tsql = bstate.format(tname) + \
            tsql = bstate + \
                '''insert into {}
                   ( schemaname, appname, startdate, status)
                     select '{}','{}', getdate(), 'P'
                     where
                     (
                         (
                             select count(*)
                             from {}
                             where schemaname = '{}'
                             and appname = '{}'
                             and status in ('P')
                             and enddate is null
                         ) = 0
                     );
            '''
            sql = tsql.format(tname, self.appschema, appmodule, tname, self.appschema, appmodule) + '''end;'''
            return sql
        except:
            self.logger.exception("problem creating instance sql for " + appmodule)
            raise

    def CreateAppInstanceRecord(self, tblEtlTable, appmodule):
        '''
        Create a new record in the ETL process log table
        '''
        giSql = self.GetStartInstanceSQL(tblEtlTable, appmodule)

        rsConnect = self.GetAWSConnection(self.awsParams)

        try:
            cur = rsConnect.cursor()
            cur.execute(giSql)
            cur.close()
            rsConnect.commit()
        except Exception as ex:
            self.logger.exception("second section")
            rsConnect.rollback()
            raise ex
        finally:
            cur.close()
            rsConnect.close()

    def GetLastGoodRun(self, tblEtlTable, appmodule):
        '''
        returns the record of the last good run for a given module
        '''
        rsConnect = self.GetAWSConnection(self.awsParams)
        tname = self.etlSchema + '.' + tblEtlTable
        retVal = None
        try:
            sql = '''select top 1 * from {}
                    where schemaname = '{}'
                    and appname = '{}'
                    and status in ('C')
                    order by enddate desc
            '''
            sql = sql.format(tname, self.appschema, appmodule)

            cur = rsConnect.cursor()
            cur.execute(sql)

            tretVal = [dict((cur.description[i][0], value) \
               for i, value in enumerate(row)) for row in cur.fetchall()]
            cur.close()
            lentretVal = len(tretVal)
            if lentretVal > 0:
                retVal = tretVal[0]
        except Exception as ex:
            rsConnect.rollback()
            raise ex
        finally:
            cur.close()
            rsConnect.close()
        return retVal

    def GetRunID(self, tblEtlTable, appmodule):
        '''
        gets the run id associated with a current processing application
        '''
        rsConnect = self.GetAWSConnection(self.awsParams)
        runID = 0
        tname = self.etlSchema + '.' + tblEtlTable
        try:
            sql = '''select runid from {}
                    where schemaname = '{}'
                    and appname = '{}'
                    and status in ('P')
                    and enddate is null
            '''
            sql = sql.format(tname, self.appschema, appmodule)

            cur = rsConnect.cursor()
            cur.execute(sql)
            data = cur.fetchall()
            cur.close()
            if data.__len__() == 0:
                runID = -1
            else:
                runID = data[0][0]
        except Exception as ex:
            rsConnect.rollback()
            raise ex
        finally:
            cur.close()
            rsConnect.close()
        return runID

    def SetInstanceParameters(self, tblEtlTable, runid, appParams):
        '''
        gets the process id associated with a current processing application
        '''
        rsConnect = self.GetAWSConnection(self.awsParams)
        tname = self.etlSchema + '.' + tblEtlTable
        successflag = False
        try:
#            bstate = '''begin;
#            lock {};
#            '''
            bstate = '''begin;
            '''

#            sql = bstate.format(tname) + '''update {}
            sql = bstate + '''update {}
                    set params = '{}'
                    where runid = {};
            '''
            sql = sql.format(tname, appParams, runid) + '''end;'''

            cur = rsConnect.cursor()
            cur.execute(sql)
            cur.close()
            rsConnect.commit()
            successflag = True
        except: # pylint: disable=broad-except
            rsConnect.rollback()
            raise
        finally:
            cur.close()
            rsConnect.close()
        return successflag

    def CompleteInstance(self, tblEtlTable, runid, appstatus):
        '''
        marks a process as complete
        '''
        rsConnect = self.GetAWSConnection(self.awsParams)
        tname = self.etlSchema + '.' + tblEtlTable
        successflag = False
        try:
#            bstate = '''begin;
#            lock {};
#            '''
            bstate = '''begin;
            '''
#            sql = bstate.format(tname) + '''update {}
            sql = bstate + '''update {}
                    set status = '{}',
                        enddate = getdate()
                    where schemaname = '{}'  
                    and runid = {}
                    and enddate is null;
            '''
            sql = sql.format(tname, appstatus, self.appschema, runid) + '''end;'''

            cur = rsConnect.cursor()
            cur.execute(sql)
            cur.close()
            rsConnect.commit()
            successflag = True
        except: # pylint: disable=broad-except
            rsConnect.rollback()
            raise
        finally:
            cur.close()
            rsConnect.close()
        return successflag

    def StartEtlLogging(self):
        '''
        routine to make sure that we have the table and create initial record for process
        '''
        try:
            sqllocation = FileUtilities.PathToForwardSlash(self.filelocs["relativeOutputfolder"] + "/")
            fUtil = FileUtilities(self.logger)
            fname = fUtil.CreateTableSql(self.filelocs["tblEtl"], sqllocation)
            RedshiftUtilities.PSqlExecute(fname, self.logger)
            self.appschema = self.filelocs["tblEtl"]["appschema"]
            self.etlSchema = self.filelocs["tblEtl"]["schemaName"]
            self.CreateAppInstanceRecord(self.filelocs["tblEtl"]["table"],
                                         self.moduleName)
        except: # pylint: disable=broad-except
            self.logger(self.moduleName + " -- we have a problem in ")
            raise
