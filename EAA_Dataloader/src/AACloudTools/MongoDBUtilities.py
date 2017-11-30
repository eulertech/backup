'''
# MongoDB Utilties
@author: viu53188
@license: IHS - not to be used outside the company
'''

from pymongo import MongoClient

class MongoDBUtilities(object):
    '''
    A collection of useful methods for working with MongoDB
    '''
    @staticmethod
    def GetMongoConnection(mongoDBConnectionInfo):
        '''
        returns the connection string to MongoDB
        '''
        try:
            client = MongoClient(host=mongoDBConnectionInfo["server"],
                                 port=mongoDBConnectionInfo["port"])
            db = client[mongoDBConnectionInfo["database"]]
            db.authenticate(mongoDBConnectionInfo["user"],
                            mongoDBConnectionInfo["pwd"],
                            mechanism='SCRAM-SHA-1')
        except:
            raise
        return client, db

    @staticmethod
    def GetCollection(db, name, filterCondition=None):
        '''
        returns a collection based on if a filter is needed
        and returns the collection
        '''
        try:
            if filterCondition == None:
                collection = db[name].find()
            else:
                collection = db[name].find(filter=filterCondition, no_cursor_timeout=True)
            return collection
        except:
            raise    
    
        