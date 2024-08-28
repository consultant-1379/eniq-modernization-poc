import sys
import time
from KYO_Tools.DeployMDRInstance import DeployMDRInstance
from KYO_Tools.Environment import PostGreSQL
from KYO_Tools.Utilities import CLI_interface
from datetime import datetime


class SIMTest(object):

    def run(self):
        cli = CLI_interface()
        cli.Green('Starting SIM test')
        overalltime = time.time()
        start = overalltime

        cli.Green('\nCreating MDR container')
        deployment = DeployMDRInstance()
        deployment.run()

        postgres = PostGreSQL()

        cli.White('\t\tCheck for MDR container')
        if not postgres.isonline():
            cli.Red('\t\tNo PostgreSQL container found.')
            sys.exit()

        cli.White('\t\tFound active MDR container. Creating a connection')
        connection = postgres.getconnection()
        self.cursor = connection.cursor()

        self.cursor.execute('CREATE TABLE SIMLOGS ( DATETIME varchar(128), THREAD varchar(128), '
                            'LOG_LEVEL varchar(128), CLASSNAME varchar(128), LOGMESSAGE varchar(4096));')


        self.logline = ''
        sourcePath = 'C:/Users/ebrifol/Desktop/TEST/sim.log'
        with open(sourcePath, 'r') as input:
            for line in input:
                self.handleData(line)

        connection.commit()
        if connection:
            self.cursor.close()
            connection.close()

        cli.Green('\nSIM test complete - ' + str(time.time() - overalltime))

    def handleData(self, data):
        if self.logline == '':
            self.logline = data

        if self.isValidLogEntry(data):
            self.pushToDB(self.logline)
            self.logline = data
        else:
            self.logline = self.logline + data

    def pushToDB(self, logline):
        logparts = logline.split(' ')
        date = '\'' + logparts[0] + ' ' + logparts[1] + '\''
        thread = '\'' + logparts[2] + '\''
        level = '\'' + logparts[3] + '\''
        classname = '\'' + logparts[4] + '\''

        del logparts[:6]
        message = '\'' + ' '.join(logparts) + '\''

        sql = 'INSERT INTO SIMLOGS VALUES (' + date + ',' + thread + ',' + level + ',' + classname + ',' + message + ');'
        self.cursor.execute(sql)

    def isValidLogEntry(self, logline):
        dateStr = logline.split(' ')[0]
        try:
            datetime.strptime(dateStr, '%Y-%m-%d')
            return True
        except:
            return False


if __name__ == '__main__':
    test = SIMTest()
    test.run()
