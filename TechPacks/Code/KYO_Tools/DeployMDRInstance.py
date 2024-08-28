import os
from KYO_Tools.Parsers import ClassicMIMParser, ModelXMLLoader
from KYO_Tools.Environment import PostGreSQL
from KYO_Tools.Utilities import CLI_interface

class DeployMDRInstance(object):

    def run(self):
        cli = CLI_interface()
        cli.Blue('\tDeploying MDR container')

        postgres = PostGreSQL()

        cli.White('\t\tChecking for existing container')
        if postgres.isonline():
            cli.Yellow('\t\tPostgreSQL container found. Removing old container')
            postgres.stopinstance()
        else:
            cli.White('\t\tNo container found')

        cli.White('\t\tStarting PostgreSQL container')
        postgres.startupinstance()

        cli.Blue('\tMDR container deployed successfully')

if __name__ == '__main__':
    test = DeployMDRInstance()
    test.run()
