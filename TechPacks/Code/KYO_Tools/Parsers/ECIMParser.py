from KYO_Tools.Utilities import Utils
from KYO_Tools.Model import EntityDef,EntityRef


class ECIMParser(object):
    '''
        This parser should be used to parse classic node inputs
        '''
    Model_Elements = {}
    Tags_To_Parse = ['mib', 'object']
    Tags_for_datatypes = ['long', 'string', 'longlong']
    GeneralParseMethod = 'general_parsing'

    def __init__(self, params):
        self.params = params

    def parse(self):
        if 'filepath' not in self.params:
            raise Exception('No valid file path provided')

        root = Utils.fileToXMLObject(self.params['filepath'])

        mib = root.find('mib')
        model = self.createModel(mib)
        model = self.creatTables(model, mib)
        model = self.createAttributes(model, mib)

        return model

    def createModel(self, xmlElement):
        name = xmlElement.get('name').replace('PmInstances', '')

        model = EntityDef(name)

        model.addProperty('Revision', xmlElement.get('version') + '_' + xmlElement.get('release'))
        model.addProperty('Version', xmlElement.get('version') + '_' + xmlElement.get('release'))
        model.addProperty('Release', xmlElement.get('release'))
        model.addProperty('DESCRIPTION', 'SGSN MME support')

        return model

    def creatTables(self, model, xmlElement):
        for entity in xmlElement.findall('object'):
            if entity.get('parentDn').endswith('Pm=1'):
                table = EntityDef('Temp')
                table = self.handleSlot(table, entity)

                model.addComponent('Measurements', table)
        return model

    def createAttributes(self, model, xmlElement):
        for entity in xmlElement.findall('object'):
            if not entity.get('parentDn').endswith('Pm=1'):
                tablename = entity.get('parentDn').split('=')[-1]
                att = EntityDef('Temp')
                att = self.handleSlot(att, entity)

                table = model.getComponent('Measurements', tablename)
                table.addComponent('Attributes', att)
                model.addComponent('Measurements', table)
        return model

    def handleSlot(self, model, xmlElement):
        for entity in xmlElement.findall('slot'):
            propertyname = entity.get('name')
            if propertyname == 'pmGroupId' or propertyname == 'measurementName':
                name = entity.find('value').text
                model.setName(name)
            else:
                if entity.find('value') is not None:
                    propertyValue = entity.find('value').text

                    if propertyname == 'measurementResult':
                        propertyname = 'dataType'
                    elif propertyname == 'collectionMethod':
                        propertyname = 'counterType'


                    model.addProperty(propertyname, propertyValue)
        return model
