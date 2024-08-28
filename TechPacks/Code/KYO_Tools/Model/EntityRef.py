import json


class EntityRef(object):
    '''
        This class defines a reference to an instance of EntityDef in the model.
    '''

    def __init__(self, name):
        self.name = name
        self.sourceName = ''
        self.sourceType = ''
        self.entityName = ''
        self.entityType = ''

    def getName(self):
        return self.name

    def getSourceName(self):
        return self.sourceName

    def setSourceName(self, sourceName):
        self.sourceName = sourceName

    def getSourceType(self):
        return self.sourceType

    def setSourceType(self, sourceType):
        self.sourceType = sourceType

    def setSource(self, sourceName, sourceType):
        self.sourceType = sourceType
        self.sourceName = sourceName

    def getEntityName(self):
        return self.entityName

    def setEntityName(self, entityName):
        self.entityName = entityName

    def getEntityType(self):
        return self.entityType

    def setEntityType(self, entityType):
        self.entityType = entityType

    def setEntity(self, entityName, entityType):
        self.entityName = entityName
        self.entityType = entityType

    def printToScreen(self):
        print
        self.name
        print
        self.sourceName
        print
        self.sourceType
        print
        self.entityName
        print
        self.entityType

    def toXML(self, file, indent=0):
        newline = '\n'
        offset = '    '
        os = offset * indent
        os2 = os + offset

        file.write(os + '<EntityRef name="' + self.name + '" ')
        file.write('sourceName="' + self.sourceName + '" ')
        file.write('sourceType="' + self.sourceType + '" ')
        file.write('entityName="' + self.entityName + '" ')
        file.write('entityType="' + self.entityType + '"/>' + newline)

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def difference(self, compareEntity):
        deltaObj = EntityRef('Delta:' + self.name + '::' + compareEntity.name)
        delta = False

        if self.sourceName != compareEntity.sourceName:
            delta = True
            deltaObj.setSourceName('Changed:' + self.sourceName + '::' + compareEntity.sourceName)

        if self.sourceType != compareEntity.sourceType:
            delta = True
            deltaObj.setSourceType('Changed:' + self.sourceType + '::' + compareEntity.sourceType)

        if self.entityName != compareEntity.entityName:
            delta = True
            deltaObj.setEntityName('Changed:' + self.entityName + '::' + compareEntity.entityName)

        if self.entityType != compareEntity.entityType:
            delta = True
            deltaObj.setEntityType('Changed:' + self.entityType + '::' + compareEntity.entityType)

        if delta is True:
            return deltaObj
        else:
            return None
