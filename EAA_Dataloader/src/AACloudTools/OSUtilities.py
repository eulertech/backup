'''
Utilities for Operatinng sytem operations
Author - Christopher Lewis
License: IHS - not to be used outside the company
'''

import platform

class OSUtilities(object):
    '''
    a collection of os utilities
    '''
    @staticmethod
    def RunCommandAndLogStdOutStdErr(command, logger):
        '''
        Use this function to capture the output from a command
        Capture both the output and the errors
        Send output/errors to the console and log file
        '''
        logger.info(command)

        # Old call that simply outputs to console
        # ret = os.system(command)

        from subprocess import Popen, PIPE
        process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        (output, error) = process.communicate()
        ret = process.wait()
        if output:
            logger.info(output)
        if error:
            if ret == 0:
                logger.info(error)
            else:
                logger.error(error)

        return ret

    @staticmethod
    def GetNullRedirection(logger):
        '''
        Return NULL redirection based on platform
        '''
        nullDev = ""
        if platform.system().lower() == "linux":
            nullDev = " 1>/dev/null "
        elif platform.system().lower() == "windows":
            nullDev = " 1>NUL "
        else:
            logger.error("OS other than Windows/Linux")
        return nullDev
