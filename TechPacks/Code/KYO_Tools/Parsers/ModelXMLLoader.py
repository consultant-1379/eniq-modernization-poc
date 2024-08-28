from KYO_Tools.Utilities import Utils
from KYO_Tools.Model import EntityDef, EntityRef

class ModelXMLLoader(object):
    '''
    This parser should be used to parse PMIM model files
    '''

    Tags_To_Parse = {'EntityDef': 'parse_entity'}

    def __init__(self, params):
        self.params = params


    def parse(self):
        if 'filepath' not in self.params:
            raise Exception('No valid file path provided')

        if 'model' not in self.params:
            model = EntityDef('temporary')
        else:
            model = self.params['model']

        XML = Utils.fileToXMLObject(self.params['filepath'])
        #XML = XML.getroot()
        model = self.parseElements(XML, model)

        return model

    def parseElements(self, xmlElement, model):
        if xmlElement.tag in self.Tags_To_Parse:
            model = getattr(self, self.Tags_To_Parse[xmlElement.tag])(xmlElement, model)
            for child in xmlElement:
                if child.tag in self.Tags_To_Parse:
                    model = getattr(self, self.Tags_To_Parse[child.tag])(child, model)
                    self.parseElements(child, model)
                    child.clear()
        else:
            for child in xmlElement:
                if child.tag in self.Tags_To_Parse:
                    model = getattr(self, self.Tags_To_Parse[child.tag])(child, model)
                    self.parseElements(child, model)
                    child.clear()

        return model

    def parse_entity(self, xmlElement, model):
        model.setName(xmlElement.get('name'))

        for child in xmlElement:
            if child.tag == 'Components':
                section = child.get('section')
                for innerchild in child:
                    if innerchild.tag == 'EntityDef':
                        component = EntityDef(innerchild.get('name'))
                        component = self.parse_entity(innerchild, component)
                        model.addComponent(section, component)
                    else:
                        component = EntityRef(innerchild.get('name'))
                        component.setSource(innerchild.get('sourceName'), innerchild.get('sourceType'))
                        component.setEntity(innerchild.get('entityName'), innerchild.get('entityType'))
                        model.addComponent(section, component)
            else:
                model.addProperty(child.tag, child.text)

        return model
