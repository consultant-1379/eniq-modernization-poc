import os
import time
from KYO_Tools.Parsers import ClassicMIMParser, ECIMParser
from KYO_Tools.DeployMDRInstance import DeployMDRInstance
from KYO_Tools.ModelConversionToSQL import ModelConversionToSQL
from KYO_Tools.LoadSQLToMDR import LoadSQLToMDR
from KYO_Tools.DesignTP import DesignTP
from KYO_Tools.Utilities import CLI_interface
from KYO_Tools.MDRCreateSQLFromModel import MDRCreateSQLFromModel


class TestMain(object):

    def run(self):
        cli = CLI_interface()
        cli.Green('Starting MDR & Tech Pack Generation test')
        overalltime = time.time()
        start = overalltime

        cli.Green('\nCreating MDR container')
        deployment = DeployMDRInstance()
        deployment.run()

        # createMDRSQL = MDRCreateSQLFromModel()
        # createMDRSQL.run()
        #
        # loader = LoadSQLToMDR()
        # loader.run('C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/MDR_Schema.sql')
        # cli.Green('MDR container created and MDR schema deployed - ' + str(time.time() - start) + '\n')
        #
        #
        #
        #
        # start = time.time()
        # cli.Green('\nDeploying BASE_E_ERBS to the MDR')
        # convertToSQL = ModelConversionToSQL()
        # convertToSQL.run('C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/BASE_E_ERBS.xml')
        #
        # loader.run('C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/BASE_E_ERBS_R1A01.sql')
        # cli.Green('BASE_E_ERBS deployed to the MDR - ' + str(time.time() - start) + '\n')
        #
        #
        #
        #
        # start = time.time()
        # cli.Green('\nDeploying BASE_E_SGSNMME to the MDR')
        # convertToSQL = ModelConversionToSQL()
        # convertToSQL.run('C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/BASE_E_SGSNMME.xml')
        #
        # loader.run('C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/BASE_E_SGSNMME_R1A01.sql')
        # cli.Green('BASE_E_SGSNMME deployed to the MDR - ' + str(time.time() - start) + '\n')
        #
        #
        #
        #
        #
        # start = time.time()
        # cli.Green('\nCreating PM_E_ERBS for 17B node version and deploying to the MDR')
        #
        # params = {}
        # params['filepath'] = 'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/NodeMomLimited_L17B_R27B01.xml'
        # parser = ClassicMIMParser(params)
        # nodemodel = parser.parse()
        #
        # designtp = DesignTP()
        # designtp.run('BASE_E_ERBS_R1A01', nodemodel)
        #
        # convertToSQL.run('C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/PM_E_ERBS_R27B01.xml')
        #
        # loader.run('C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/PM_E_ERBS_R27B01.sql')
        # cli.Green('PM_E_ERBS_L17B created and deployed to the MDR - ' + str(time.time() - start) + '\n')
        #
        #
        #
        #
        #
        #
        #
        # start = time.time()
        # cli.Green('\nCreating PM_E_ERBS for 19.1 node version and deploying to the MDR')
        #
        # params[
        #     'filepath'] = 'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/NodeMomLimited_L19_Q1_R28M01.xml'
        # parser = ClassicMIMParser(params)
        # nodemodel = parser.parse()
        #
        # designtp.run('BASE_E_ERBS_R1A01', nodemodel)
        #
        # convertToSQL.run('C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/PM_E_ERBS_R28M01.xml')
        #
        # loader.run('C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/PM_E_ERBS_R28M01.sql')
        # cli.Green('PM_E_ERBS_L19.Q1 created and deployed to the MDR - ' + str(time.time() - start))
        #
        #
        #
        #
        #
        #
        #
        # start = time.time()
        # cli.Green('\nCreating SGSN MME for 5.38 node version and deploying to the MDR')
        #
        # dir = 'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/NodeModels/SGSNMME/MOMxmls'
        # params['filepath'] = dir + '/SgsnMmePmInstances_mp.xml'
        # parser = ECIMParser(params)
        # nodemodel = parser.parse()
        #
        # designtp.run('BASE_E_SGSNMME_R1A01', nodemodel)
        #
        # convertToSQL.run(
        #     'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/PM_E_SGSNMME_5_38.xml')
        #
        # loader.run('C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/PM_E_SGSNMME_5_38.sql')
        # cli.Green('SGSN MME for 5.38 created and deployed to the MDR - ' + str(time.time() - start))

        cli.Green('\nMDR & Tech Pack Generation test complete - ' + str(time.time() - overalltime))


if __name__ == '__main__':
    test = TestMain()
    test.run()
