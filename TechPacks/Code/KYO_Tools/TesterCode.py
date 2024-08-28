from KYO_Tools.Parsers import ClassicMIMParser, ECIMParser
from KYO_Tools.Model import EntityDef,EntityRef
import os

class TesterCode(object):

    def run(self):
        params = {}
        params['Model'] = EntityDef('SGSN-MME')

        dir = 'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/NodeModels/SGSNMME/MOMxmls'
        params['filepath'] = dir + '/SgsnMmePmInstances_mp.xml'
        parser = ECIMParser(params)
        model = parser.parse()

        outputFile = open(
            'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/' + model.getName() + '.xml', 'w+')
        model.toXML(outputfile=outputFile)
        outputFile.close()


if __name__ == '__main__':
    test = TesterCode()
    test.run()
