import KYO_Tools.Utilities as Utils
import json


class EntityDef(object):
    '''
        This class defines an Entity in the model
    '''

    def __init__(self, name):
        self.name = name
        self.properties = Utils.odict()
        self.components = Utils.odict()

    def setName(self, name):
        self.name = name

    def getName(self):
        return self.name

    def addProperty(self, key, value):
        if isinstance(value, list):
            value = ','.join(value)
        self.properties[key] = Utils.unescape(value)

    def getProperty(self, key):
        try:
            return self.properties[key]
        except:
            return ''

    def getProperties(self):
        return self.properties

    def hasProperty(self, key):
        if key in self.properties:
            return True
        else:
            return False

    def addComponent(self, component, entity):
        components = Utils.odict()
        if component in self.components:
            components = self.components[component]

        components[entity.getName()] = entity
        self.components[component] = components

    def getComponentGroup(self, key):
        if key in self.components:
            return self.components[key]
        else:
            return Utils.odict()

    def getComponent(self, component, name):
        if component in self.components:
            if name in self.components[component]:
                return self.components[component][name]

        return EntityDef(name)

    def removeComponent(self, component, name):
        if component in self.components:
            if name in self.components[component]:
                del self.components[component][name]

    def hasComponent(self, key):
        if key in self.components:
            return True
        else:
            return False

    def getComponents(self):
        return self.components

    def listComponentNames(self):
        return self.components.keys()

    def printToScreen(self):
        print(self.name)

        for key, value in self.properties.iteritems():
            print(key + ' : ' + str(value) )

        for key, value in self.components.iteritems():
            print( '\nComponent: ' + key)
            for name, obj in value.iteritems():
                print('\n')
                obj.printToScreen()

    def toXML(self, outputfile, indent=0):
        newline = '\n'
        offset = '    '
        os = offset * indent
        os2 = os + offset

        outputfile.write(os + '<EntityDef name="' + self.name + '">' + newline)

        for key, value in self.properties.iteritems():
            if value is None:
                value = ''

            value = value.strip()
            # outputfile.write(  os2 + '<' + key + ' value="' + Utils.escape(value) + '"/>' + newline)
            outputfile.write(os2 + '<' + key + '>' + Utils.escape(value) + '</' + key + '>' + newline)

        for section, collection in self.components.iteritems():
            outputfile.write(os2 + '<Components section="' + section + '">' + newline)
            for key, value in collection.iteritems():
                value.toXML(outputfile, indent + 2)
            outputfile.write(os2 + '</Components>' + newline)

        outputfile.write(os + '</EntityDef>' + newline)

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)

    def difference(self, compareEntity):
        deltaObj = EntityDef('Delta--' + compareEntity.name)
        delta = False

        Delta = Utils.DictDiffer(self.properties, compareEntity.properties)
        for item in Delta.changed():
            delta = True
            deltaObj.addProperty('Changed:' + item, compareEntity.properties[item])
            deltaObj.addProperty('Old:' + item, self.properties[item])

        for item in Delta.added():
            delta = True
            deltaObj.addProperty('New:' + item, compareEntity.properties[item])

        for item in Delta.removed():
            delta = True
            deltaObj.addProperty('Removed:' + item, self.properties[item])

        Delta = Utils.DictDiffer(self.components, compareEntity.components)
        for item in Delta.added():
            delta = True
            deltaObj.addComponent('New:' + item, compareEntity.components[item])

        for item in Delta.removed():
            delta = True
            deltaObj.addComponent('Removed:' + item, self.components[item])

        for item in Delta.changed():
            changeddict = Utils.odict()
            innerDelta = Utils.DictDiffer(self.components[item], compareEntity.components[item])
            for inneritem in innerDelta.added():
                changeddict['New:' + inneritem] = compareEntity.components[item][inneritem]

            for inneritem in innerDelta.removed():
                changeddict['Removed:' + inneritem] = self.components[item][inneritem]

            for inneritem in innerDelta.changed():
                childDelta = self.components[item][inneritem].difference(compareEntity.components[item][inneritem])
                if childDelta is not None:
                    changeddict['Changed:' + inneritem] = childDelta

            if len(changeddict) > 0:
                delta = True
                deltaObj.addComponent(item, changeddict)

        if delta is True:
            return deltaObj
        else:
            return None