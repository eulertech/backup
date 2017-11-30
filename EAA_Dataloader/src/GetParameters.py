'''
Get Parameters for the various configurations
Author - Thomas Coffey)
License: IHS - not to be used outside the company
'''

# pylint: disable=too-few-public-methods
class GetParameters(object):
    '''
    Class to get configuration parameters
    '''
    def __init__(self):
        '''
        Initialization
        '''
        self.configfile = None
        self.configdata = None

    def LoadConfigFile(self):
        '''
        Load configuration data
        '''
        import json
        with open(self.configfile) as configfile:
            self.configdata = json.load(configfile)
