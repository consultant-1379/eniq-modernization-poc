from KYO_Tools.Utilities import Utils
from KYO_Tools.Model import EntityDef,EntityRef
import os


class XMLSchema(object):

    def __init__(self, params):
        self.params = params
        self.structure = []
        self.container = {}

    def parse(self):
        if 'filepath' not in self.params:
            raise Exception('No valid file path provided')

        context = Utils.fileToXMLEvents(self.params['filepath'])

        model = self.parseElements(context)

        return model

    def parseElements(self, context):
        if 'Model' in self.params:
            model = self.params['Model']
        else:
            model = EntityDef(os.path.basename(self.params['filepath']))

        self.container['Model'] = model
        self.structure.append('Model')

        for event, elem in context:
            # print event + ' : ' + elem.tag
            if event == 'start':
                self.startTag(elem, self.cleanTag(elem))
            else:
                self.endTag(elem, self.cleanTag(elem))
                elem.clear()

        return self.container['Model']

    def startTag(self, elem, tag):
        parent = '.'.join(self.structure)

        # Add the new tag to the structure
        self.structure.append(tag)
        name = '.'.join(self.structure)

        entity = self.container[parent].getComponent('Children', tag)

        if entity.hasProperty('Occurrence'):
            entity.addProperty('Occurrence', 'Multiple')
        else:
            entity.addProperty('Occurrence', 'Single')
            if elem.text is not None:
                entity.addProperty('ContainsText', 'True')

        if len(elem.attrib.keys()) > 0:
            if entity.hasProperty('Attributes'):
                attribs = entity.getProperty('Attributes').split(',')
                print()
                attribs = list(set(attribs + list(elem.attrib.keys())))
                entity.addProperty('Attributes', attribs)
            else:
                entity.addProperty('Attributes', list(elem.attrib.keys()))

        self.container[name] = entity

    def endTag(self, elem, tag):
        name = '.'.join(self.structure)
        self.structure.pop()
        parent = '.'.join(self.structure)

        self.container[parent].addComponent('Children', self.container[name])

        del self.container[name]

    def cleanTag(self, elem):
        if '}' in elem.tag:
            tag = elem.tag.split('}')[1]
        else:
            tag = elem.tag

        return tag