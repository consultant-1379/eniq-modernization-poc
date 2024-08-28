
import os
import psycopg2
from time import sleep


class PostGreSQL(object):

    def isonline(self):
        result = os.popen('docker ps --filter "name=MDR_postgres"').read()
        if len(result.split('\n')) > 2:
            return True
        return False

    def startupinstance(self):
        os.popen(
            'docker run --name MDR_postgres -p 5555:5432 -e POSTGRES_DB=MDR -e POSTGRES_PASSWORD=dba123 -e POSTGRES_USER=MDR_dba -d postgres:10').read()

        sleep(2)


    def stopinstance(self):
        os.popen('docker stop MDR_postgres').read()
        os.popen('docker rm MDR_postgres').read()

    def getconnection(self):
        connection = psycopg2.connect(user="MDR_dba",
                                      password="dba123",
                                      host="localhost",
                                      port="5555",
                                      database="MDR")
        return connection



