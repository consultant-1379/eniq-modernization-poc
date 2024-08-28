import sys
import time
from KYO_Tools.DeployMDRInstance import DeployMDRInstance
from KYO_Tools.Environment import PostGreSQL
from KYO_Tools.Model import EntityDef
from KYO_Tools.Utilities import CLI_interface
import KYO_Tools.Utilities as Utils
from datetime import datetime


class GeoRedAnalysis(object):

    def run(self):
        cli = CLI_interface()
        cli.Green('Starting Geo Red test')
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

        self.cursor.execute('CREATE TABLE GEORED ( CUID varchar(128), NAME varchar(128), '
                            'SOURCE_ID varchar(128), TARGET_ID varchar(128), SI_KIND varchar(128), '
                            'DELETED varchar(128), ERROR varchar(128), ERROR_MESSAGE varchar(512));')

        connection.commit()

        errors = Utils.odict()
        errorMessage = ''
        error = False
        self.entities = Utils.odict()
        self.logline = ''
        sourcePath = 'C:/Users/ebrifol/Dev/GeoRed/Vivacell/data/20191007Log.txt'
        with open(sourcePath, 'r') as input:
            for line in input:
                if error:
                    error = False
                    errorID = line.split(':')[-1].strip()
                    errors[errorID] = errorMessage

                if 'Import Error' in line:
                    errorMessage = line.split(':')[-1]
                    error = True

                self.handleData(line)

        self.pushToDB(self.entities, errors)

        connection.commit()
        if connection:
            self.cursor.close()
            connection.close()

        cli.Green('\nGeo Red test complete - ' + str(time.time() - overalltime))

    def handleData(self, data):
        elements = data.split(':')
        if 'Exported SI_CUID' in data:
            CUID = elements[4].split(',')[0].strip()
            SOURCE_ID = elements[5].split(',')[0].strip()
            NAME = elements[6].split(',')[0].strip()
            KIND = elements[7].split(',')[0].strip()

            entity = EntityDef(NAME)
            entity.addProperty('CUID', CUID)
            entity.addProperty('SOURCE_ID', SOURCE_ID)
            entity.addProperty('KIND', KIND)
            entity.addProperty('DELETED', 'False')

            self.entities[CUID] = entity

        elif 'Removing SI_CUID' in data:
            CUID = elements[4].split(',')[0].strip()
            NAME = elements[5].split(',')[0].strip()
            KIND = elements[6].split(',')[0].strip()

            if CUID in self.entities:
                entity = self.entities[CUID]
                entity.addProperty('DELETED', 'True')
            else:
                entity = EntityDef(NAME)
                entity.addProperty('CUID', CUID)
                entity.addProperty('DELETED', 'True')
                entity.addProperty('KIND', KIND)

                self.entities[CUID] = entity

        elif 'Imported SI_CUID' in data:
            CUID = elements[4].split(',')[0].strip()
            TARGET_ID = elements[5].split(',')[0].strip()
            NAME = elements[6].split(',')[0].strip()
            KIND = elements[7].split(',')[0].strip()

            if CUID in self.entities:
                entity = self.entities[CUID]
                entity.addProperty('TARGET_ID', TARGET_ID)
            else:
                entity = EntityDef(NAME)
                entity.addProperty('CUID', CUID)
                entity.addProperty('TARGET_ID', TARGET_ID)
                entity.addProperty('KIND', KIND)
                entity.addProperty('DELETED', 'False')

                self.entities[CUID] = entity

    def pushToDB(self, entities, errors):
        for entity_ID, entity in entities.iteritems():
            CUID = entity_ID
            NAME = entity.getName()
            SOURCE_ID = entity.getProperty('SOURCE_ID')
            TARGET_ID = entity.getProperty('TARGET_ID')
            SI_KIND = entity.getProperty('KIND')
            DELETED = entity.getProperty('DELETED')

            ERROR = 'False'
            ERROR_MESSAGE = 'Null'
            if TARGET_ID in errors:
                ERROR = 'True'
                ERROR_MESSAGE = errors[TARGET_ID]

            sql = 'INSERT INTO GEORED VALUES (\'' + CUID + '\',\'' + NAME + '\',\'' + SOURCE_ID + '\',\'' + TARGET_ID + '\',\'' + \
              SI_KIND + '\',\'' + DELETED + '\',\'' + ERROR + '\',\'' + ERROR_MESSAGE + '\');'

            print(sql)

            self.cursor.execute(sql)


if __name__ == '__main__':
    test = GeoRedAnalysis()
    test.run()