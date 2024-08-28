
from KYO_Tools.Parsers import XMLSchema
from KYO_Tools.Model import EntityDef,EntityRef
import os

class DeriveXMLSchema(object):

    def run(self):
        params = {}
        params['Model'] = EntityDef('SGSN-MME')

        dir = 'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/NodeModels/SGSNMME/MOMxmls'

        for r, d, f in os.walk(dir):
            for file in f:
                if ".xml" in file:
                    params['filepath'] = os.path.join(r, file)
                    parser = XMLSchema(params)
                    params['Model'] = parser.parse()

        model = params['Model']

        outputFile = open(
            'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/' + model.getName() + '.xml', 'w+')
        model.toXML(outputfile=outputFile)
        outputFile.close()




if __name__ == '__main__':
    test = DeriveXMLSchema()
    test.run()
