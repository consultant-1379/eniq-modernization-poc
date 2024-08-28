from KYO_Tools.Utilities import Utils
from KYO_Tools.Model import EntityDef,EntityRef


class ClassicMIMParser(object):
    '''
        This parser should be used to parse classic node inputs
        '''
    Model_Elements = {}
    Tags_To_Parse = ['mim', 'applicationTag', 'enum', 'enumMember', 'struct', 'structMember', 'class', 'attribute',
                     'relationship']
    Tags_for_datatypes = ['long', 'string', 'longlong']
    GeneralParseMethod = 'general_parsing'

    def __init__(self, params):
        self.params = params

    def parse(self):
        if 'filepath' not in self.params:
            raise Exception('No valid file path provided')

        context = Utils.fileToXMLEvents(self.params['filepath'])

        flag = None
        for event, elem in context:
            method = None
            if elem.tag in self.Tags_To_Parse:
                try:
                    method = getattr(self, elem.tag + '_' + event)
                except:
                    # Suppress the exception is the method doesnt exist. Maybe no handling is required.
                    flag = None

                if method is not None:
                    model, flag = method(elem)

            elif flag is not None:
                method = getattr(self, self.GeneralParseMethod + '_' + event)
                model, flag = method(elem, flag)

            elem.clear()

        return model

    def mim_start(self, xmlElement):
        name = xmlElement.get('name')
        nameparts = name.split('_')
        version = nameparts[-1]
        name = '_'.join(nameparts[:-1])

        model = EntityDef(name)

        model.addProperty('Revision', xmlElement.get('revision'))
        model.addProperty('Version', version + '.' + xmlElement.get('version') + '.' + xmlElement.get('release'))
        model.addProperty('Release', xmlElement.get('release'))
        model.addProperty('Date', xmlElement.get('date'))

        self.Model_Elements['mim'] = model

        return model, 'mim'

    def mim_end(self, xmlElement):
        return self.Model_Elements['mim'], None

    def applicationTag_start(self, xmlElement):
        model = self.Model_Elements['mim']

        model.addProperty('ApplicationTag', xmlElement.text)
        model.addProperty('NodeVersion', xmlElement.text.split(' ')[-1])

        self.Model_Elements['mim'] = model

        return model, None

    def enum_start(self, xmlElement):
        self.Model_Elements['enum'] = EntityDef(xmlElement.get('name'))
        return self.Model_Elements['enum'], 'enum'

    def enum_end(self, xmlElement):
        return self.updateModel('enums', 'enum', 'Enums')

    def enumMember_start(self, xmlElement):
        self.Model_Elements['enumMember'] = EntityDef(xmlElement.get('name'))
        return self.Model_Elements['enumMember'], 'enumMember'

    def enumMember_end(self, xmlElement):
        return self.updateModel('enum', 'enumMember', 'EnumMembers')

    def struct_start(self, xmlElement):
        self.Model_Elements['struct'] = EntityDef(xmlElement.get('name'))
        return self.Model_Elements['struct'], 'struct'

    def struct_end(self, xmlElement):
        return self.updateModel('structs', 'struct', 'Structs')

    def structMember_start(self, xmlElement):
        self.Model_Elements['structMember'] = EntityDef(xmlElement.get('name'))
        return self.Model_Elements['structMember'], 'structMember'

    def structMember_end(self, xmlElement):
        return self.updateModel('struct', 'structMember', 'StructMembers')

    def class_start(self, xmlElement):
        self.Model_Elements['class'] = EntityDef(xmlElement.get('name'))
        return self.Model_Elements['class'], 'class'

    def class_end(self, xmlElement):
        return self.updateModel('mim', 'class', 'Measurements')

    def attribute_start(self, xmlElement):
        self.Model_Elements['attribute'] = EntityDef(xmlElement.get('name'))
        return self.Model_Elements['attribute'], 'attribute'

    def attribute_end(self, xmlElement):
        return self.updateModel('class', 'attribute', 'Attributes')

    def relationship_start(self, xmlElement):
        self.Model_Elements['relationship'] = EntityDef(xmlElement.get('name'))
        return self.Model_Elements['relationship'], 'relationship'

    def relationship_end(self, xmlElement):
        return self.updateModel('mim', 'relationship', 'Relationships')

    def general_parsing_start(self, xmlElement, flag):
        model = self.Model_Elements[flag]
        if flag == 'relationship':
            if xmlElement.tag == 'hasClass':
                model.addProperty(self.relFlag, xmlElement.get('name'))
            elif xmlElement.tag in ['min', 'max']:
                model.addProperty('cardinality_' + xmlElement.tag, str(xmlElement.text).strip())
            self.relFlag = xmlElement.tag

        else:
            if xmlElement.tag in self.Tags_for_datatypes:
                model.addProperty('dataType', xmlElement.tag)
            elif xmlElement.tag == 'enumRef':
                enum = self.Model_Elements['enums'].getComponent('Enums', xmlElement.get('name'))
                model = self.updateReference(model, enum, xmlElement.tag, flag)

            elif xmlElement.tag == 'structRef':
                struct = self.Model_Elements['structs'].getComponent('Structs', xmlElement.get('name'))
                model = self.updateReference(model, struct, xmlElement.tag, flag)

            else:
                value = str(xmlElement.text).strip()

                if value == 'None':
                    value = ''

                model.addProperty(xmlElement.tag, value)
        self.Model_Elements[flag] = model

        return self.Model_Elements['mim'], flag

    def general_parsing_end(self, xmlElement, flag):
        return self.Model_Elements['mim'], flag

    def updateReference(self, parent, child, collectionName, flag):
        # Add the reference to the required entity

        reference_entity = EntityRef(child.getName())
        reference_entity.setSource('self', 'Node')
        reference_entity.setEntity(child.getName(), collectionName)
        parent.addComponent('References', reference_entity)

        if flag == 'attribute':
            # Update the Node object with the required entity
            base = self.Model_Elements['mim']
            base.addComponent(collectionName, child)

            refs = None
            if 'StructMembers' in child.listComponentNames():
                refs = child.getComponents()['StructMembers']
            elif 'EnumMembers' in child.listComponentNames():
                refs = child.getComponents()['EnumMembers']

            for entity in refs.itervalues():
                if 'References' in entity.listComponentNames():
                    references = entity.getComponents()['References']
                    for reference in references.itervalues():
                        if reference.getEntityType() == 'enumRef':
                            ref = self.Model_Elements['enums'].getComponent('Enums', reference.getEntityName())
                        elif reference.getEntityType() == 'structRef':
                            ref = self.Model_Elements['structs'].getComponent('Structs', reference.getEntityName())

                        base.addComponent(reference.getEntityType(), ref)

            self.Model_Elements['mim'] = base

        return parent

    def updateModel(self, parentname, childname, collectionName):
        child = self.Model_Elements[childname]

        if parentname in self.Model_Elements:
            parent = self.Model_Elements[parentname]
        else:
            parent = EntityDef(parentname)

        parent.addComponent(collectionName, child)

        self.Model_Elements[parentname] = parent

        self.Model_Elements[childname] = None

        return parent, None
