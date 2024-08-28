import os
import KYO_Tools.Utilities as Utils
from KYO_Tools.Parsers import ClassicMIMParser, ModelXMLLoader
from KYO_Tools.Model import EntityDef
from KYO_Tools.Environment import PostGreSQL
from KYO_Tools.Utilities import CLI_interface
import sys


class DesignTP(object):

    def run(self, BaseTPName, nodemodel):
        self.columnlimit = 5000
        self.tableLimitIndex = {}
        self.countercount = 0

        cli = CLI_interface()
        cli.Blue('\tCreating Tech Pack Model from Node Input: ' + nodemodel.getName())

        postgres = PostGreSQL()

        cli.White('\t\tCheck for MDR container')
        if not postgres.isonline():
            cli.Red('\t\tNo PostgreSQL container found.')
            sys.exit()

        cli.White('\t\tFound active MDR container. Creating a connection')
        connection = postgres.getconnection()
        self.cursor = connection.cursor()


        # outputFile = open(
        #     'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/' + nodemodel.getName() + '.xml', 'w+')
        # nodemodel.toXML(outputfile=outputFile)
        # outputFile.close()

        cli.White('\t\tRetrieving design rules from ' + BaseTPName)
        designrules = self.loadDesignRulesFromMDR(BaseTPName)

        cli.White('\t\tApplying design rules to the Node Input')
        self.modelpackages = Utils.odict()
        for packagename, packageRules in designrules.getComponentGroup('Rules').iteritems():
            self.applyDesignRules(BaseTPName, nodemodel, packagename, packageRules)

        for name, package in self.modelpackages.iteritems():
            package = self.completeTPModel(package, nodemodel)
            package = self.addFlowConfigurations(package, BaseTPName)
            outputFile = open('C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/' + package.getName() + '.xml', 'w+')
            cli.White('\t\tCreating ' + name + ' Model to C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/' + package.getName() + '.xml')
            package.toXML(outputfile=outputFile)
            outputFile.close()
            cli.White('\t\tTech Pack Model created')

        if connection:
            self.cursor.close()
            connection.close()

        cli.Blue('\tTech Pack Model successfully generated from Node Input')

    def addFlowConfigurations(self, TPmodel, BaseTPName):
        schedules = self.queryMDR(self.cursor,
                                  'SELECT * FROM flow_scheduling WHERE techpack_version_id = \'' + BaseTPName + '\';')
        for schedule in schedules:
            sch = TPmodel.getComponent('Flows', schedule[1])
            action = sch.getComponent('Actions', schedule[2])
            action.addProperty('WORKER_NAME', schedule[3])
            action.addProperty('REOCCURING', schedule[4])
            action.addProperty('ROP_INTERVAL', str(schedule[5]))
            action.addProperty('ROP_OFFSET', str(schedule[6]))
            action.addProperty('ENABLED', '1')
            sch.addComponent('Actions', action)
            TPmodel.addComponent('Flows', sch)

        configuration = self.queryMDR(self.cursor,
                                  'SELECT * FROM flow_configuration WHERE techpack_version_id = \'' + BaseTPName + '\';')
        for config in configuration:
            sch = TPmodel.getComponent('Flows', config[1])
            action = sch.getComponent('Actions', config[2])
            action.addProperty('WORKER_NAME', config[3])
            action.addProperty('DEPENDENCY_FLOW_NAME', config[5])
            action.addProperty('DEPENDENCY_ACTION_NAME', config[6])

            for param in config[4].split('::'):
                if param != '' and param is not None:
                    paramparts = param.split('=')
                    configname = 'CONFIG_' + paramparts[0]
                    configvalue = paramparts[1]
                    if '$VERSION' in configvalue:
                        configvalue = configvalue.replace('$VERSION', TPmodel.getProperty('TECHPACK_RELEASE'))
                    elif '$TECHPACK_ID' in configvalue:
                        configvalue = configvalue.replace('$VERSION', TPmodel.getProperty('TECHPACK_NAME')
                                                          + TPmodel.getProperty('TECHPACK_RELEASE'))

                    action.addProperty(configname, configvalue)

            sch.addComponent('Actions', action)
            TPmodel.addComponent('Flows', sch)

        return TPmodel


    def completeTPModel(self, TPmodel, nodemodel):
        snv = EntityDef(nodemodel.getProperty('NodeVersion'))
        snv.addProperty('MODEL_VERSION', nodemodel.getProperty('Version'))
        snv.addProperty('MODEL_REVISION', nodemodel.getProperty('Revision'))
        snv.addProperty('DESCRIPTION', nodemodel.getProperty('ApplicationTag'))
        TPmodel.addComponent('SUPPORTED_NODE_VERSIONS', snv)

        for relname, relEntity in nodemodel.getComponentGroup('Relationships').iteritems():
            if relEntity.hasProperty('parent'):
                relation = EntityDef(relname)
                relation.addProperty('PARENT_MO_NAME', relEntity.getProperty('parent'))
                relation.addProperty('CHILD_MO_NAME', relEntity.getProperty('child'))
                relation.addProperty('CARDINALITY_MIN', relEntity.getProperty('cardinality_min'))
                relation.addProperty('CARDINALITY_MAX', relEntity.getProperty('cardinality_max'))
                TPmodel.addComponent('NODE_MO_STRUCTURE', relation)

        return TPmodel

    def applyDesignRules(self, BaseTPName, nodemodel, packagename, packageRules):
        for MOname, MOEntity in nodemodel.getComponentGroup('Measurements').iteritems():
            listOfRulesPassed = self.isValidMO(MOEntity, packageRules)
            if len(listOfRulesPassed) > 0:
                column_order = 0
                atts = MOEntity.getComponentGroup('Attributes')
                for attname in sorted(atts):
                    attEntity = atts[attname]
                    for MOrule in listOfRulesPassed:
                        targetEntityname = packagename
                        if MOrule.getProperty('TARGET') != '':
                            targetEntityname = targetEntityname + '_' + MOrule.getProperty('TARGET').upper()
                        else:
                            targetEntityname = targetEntityname + '_' + MOname.upper()

                        attrule = self.isValidAtt(attEntity, MOrule)
                        if attrule is not None:
                            if attrule.hasProperty('TARGET'):
                                targetEntityname = targetEntityname + attrule.getProperty('TARGET').upper()

                            packagemodel = self.getPackageModel(packagename, nodemodel, BaseTPName)
                            column_order = self.createReportableEntityColumn(BaseTPName, packagemodel, MOEntity, attEntity,
                                                               column_order, targetEntityname)
                            column_order = column_order + 1

    def createReportableEntityColumn(self, BaseTPName, packagemodel, MOEntity, attEntity, column_order, targetEntityname):
        targetEntityname = targetEntityname.replace('-', '_')
        targetEntityname = targetEntityname.replace('.', '_')

        index = ''
        if targetEntityname in self.tableLimitIndex:
            index = str(self.tableLimitIndex[targetEntityname])

        if index == '':
            repEntity = packagemodel.getComponent('REPORTABLE_ENTITIES', targetEntityname)
        else:
            repEntity = packagemodel.getComponent('REPORTABLE_ENTITIES', targetEntityname + '_' + index)

        if len(repEntity.getComponentGroup('REPORTABLE_ENTITIES_COLUMNS')) >= self.columnlimit:
            if index == '':
                #packagemodel.removeComponent('REPORTABLE_ENTITIES', targetEntityname)
                index = 0

            repEntity.setName(targetEntityname + '_' + str(index))
            packagemodel.addComponent('REPORTABLE_ENTITIES', repEntity)

            index = int(index) + 1
            repEntity = EntityDef(targetEntityname + '_' + str(index))
            packagemodel.addComponent('REPORTABLE_ENTITIES', repEntity)
            self.tableLimitIndex[targetEntityname] = index


        TechPack_versionID = packagemodel.getProperty('TECHPACK_NAME') + packagemodel.getProperty('TECHPACK_RELEASE')
        tablename = targetEntityname + '_' + packagemodel.getProperty('TECHPACK_RELEASE')

        if not repEntity.hasProperty('ENTITY_TYPE'):
            repEntity.addProperty('DESCRIPTION', 'RAW table for ' + TechPack_versionID + ':' + targetEntityname)
            repEntity.addProperty('ENTITY_TYPE', 'RAW')
            repEntity.addProperty('PARTITION_PLAN', 'DAY')  # TEMP
            repEntity.addProperty('RETENTION_PLAN', 'SHORTTERM')  # TEMP
            repEntity.addProperty('MAPPED_MO', MOEntity.getName())

            keys = self.queryMDR(self.cursor, 'SELECT rec.entity_column_order, rec.entity_column_name, '
                                              'rec.entity_column_type, rec.entity_column_description, '
                                              'rec.reportable_entity_id, recs.source_entity_column_name, '
                                              'rec.entity_data_type, rec.entity_counter_type, rec.partition_by '
                                              'FROM reportable_entity_columns as rec, reportable_entity_column_source as recs '
                                              'WHERE rec.entity_column_id = recs.entity_column_id '
                                              'AND rec.techpack_version_id = \'' + BaseTPName + '\' order by entity_column_order;')

            for key in keys:
                repkeycol = EntityDef(key[1])
                repkeycol.addProperty('ENTITY_COLUMN_ORDER', str(key[0]))
                repkeycol.addProperty('SOURCE_ENTITY_NAME', key[4])
                repkeycol.addProperty('SOURCE_ENTITY_COLUMN_NAME', key[1])
                repkeycol.addProperty('ENTITY_COLUMN_TYPE', key[2])
                repkeycol.addProperty('ENTITY_DESCRIPTION', key[3])
                repkeycol.addProperty('ENTITY_DATA_TYPE', key[6])
                repkeycol.addProperty('ENTITY_COUNTER_TYPE', key[7])
                repkeycol.addProperty('PARTITION_BY', str(key[8]))
                repEntity.addComponent('REPORTABLE_ENTITIES_COLUMNS', repkeycol)

                column_order = key[0] + 1

        attName = attEntity.getName().replace('-', '_')
        attName = attName.replace('.', '_').lower()

        repcol = EntityDef(attName)
        repcol.addProperty('ENTITY_COLUMN_ORDER', str(column_order))
        repcol.addProperty('ENTITY_COLUMN_TYPE', 'counter')
        repcol.addProperty('SOURCE_ENTITY_NAME', MOEntity.getName())
        repcol.addProperty('SOURCE_ENTITY_COLUMN_NAME', attEntity.getName())

        data_type = attEntity.getProperty('dataType')
        if data_type == 'long':
            data_type = 'BIGINT'
        elif data_type == 'longlong':
            data_type = 'BIGINT'

        ctr_type = attEntity.getProperty('counterType')
        if ctr_type == 'PDF':
            if 'Compressed: True' in attEntity.getProperty('description'):
                ctr_type = 'COMPRESSED_PDF'

            length = attEntity.getProperty('maxLength')

            data_type = 'ARRAY<'+data_type+'>:' + length

        repcol.addProperty('ENTITY_DATA_TYPE', data_type)
        repcol.addProperty('ENTITY_COUNTER_TYPE', ctr_type)
        repcol.addProperty('PARTITION_BY', '0')
        repcol.addProperty('ENTITY_DESCRIPTION', attEntity.getProperty('description'))

        repEntity.addComponent('REPORTABLE_ENTITIES_COLUMNS', repcol)
        packagemodel.addComponent('REPORTABLE_ENTITIES', repEntity)
        self.modelpackages[packagemodel.getName()] = packagemodel

        return column_order

    def getPackageModel(self, packagename, nodemodel, BaseTPName):
        fullpackagename = packagename + '_' + nodemodel.getProperty('Revision')
        packagemodel = EntityDef(fullpackagename)
        if fullpackagename in self.modelpackages.keys():
            packagemodel = self.modelpackages[fullpackagename]
        else:
            versions = self.queryMDR(self.cursor, 'select product_number, feature_name, license, '
                                               ' vendor, technology from techpack_versions where techpack_version_id = \'' + BaseTPName + '\';')

            packagemodel.addProperty('TECHPACK_RELEASE', nodemodel.getProperty('Revision'))
            packagemodel.addProperty('TECHPACK_NAME', packagename)
            packagemodel.addProperty('DESCRIPTION', nodemodel.getProperty('ApplicationTag'))
            packagemodel.addProperty('DEPENDENCY', BaseTPName)
            packagemodel.addProperty('PRODUCT_NUMBER', versions[0][0])
            packagemodel.addProperty('FEATURE_NAME', versions[0][1])
            packagemodel.addProperty('LICENSE', versions[0][2])
            packagemodel.addProperty('TECHPACK_TYPE', 'PM')
            packagemodel.addProperty('VENDOR', versions[0][3])
            packagemodel.addProperty('TECHNOLOGY', versions[0][4])

        return packagemodel

    def isValidMO(self, MOEntity, packageRules):
        listOfRulesPassed = []
        for morulename, moruleEntity in packageRules.getComponentGroup('Rules').iteritems():
            if getattr(self, moruleEntity.getProperty('MATCH_TYPE'))(MOEntity, moruleEntity):
                listOfRulesPassed.append(moruleEntity)
        return listOfRulesPassed

    def isValidAtt(self, AttEntity, MORuleEntity):
        attRules = MORuleEntity.getComponentGroup('Rules')

        for attrulename, attruleEntity in attRules.iteritems():
            if getattr(self, attruleEntity.getProperty('MATCH_TYPE'))(AttEntity, attruleEntity):
                return attruleEntity
        return None

    def ALL(self, modelentity, rule):
        return True

    def equals(self, modelentity, rule):
        match_argument = rule.getProperty('MATCH_ARGUMENT')
        match_value = rule.getProperty('MATCH_VALUE')

        if match_argument == 'name':
            if modelentity.getName() == match_value:
                return True
        elif modelentity.getProperty(match_argument) == match_value:
            return True

        return False

    def startswith(self, modelentity, rule):
        match_argument = rule.getProperty('MATCH_ARGUMENT')
        match_value = rule.getProperty('MATCH_VALUE')

        if match_argument == 'name':

            if str(modelentity.getName()).startswith(match_value):
                return True
        else:
            if str(modelentity.getProperty(match_argument)).startswith(match_value):
                return True
        return False

    def loadDesignRulesFromMDR(self, BaseTPName):
        result = self.queryMDR(self.cursor, 'select * from designrules where techpack_version_id = \'' +
                       BaseTPName + '\' and parent_rule_name = \'' + BaseTPName.rsplit('_', 1)[0] + '\' order by execution_order;')

        designrules = EntityDef(BaseTPName)
        for row in result:
            package = EntityDef(row[1])

            ruleset = EntityDef(row[2])
            ruleset.addProperty('MATCH_TYPE', row[5])
            ruleset.addProperty('MATCH_ARGUMENT', row[6])
            ruleset.addProperty('MATCH_VALUE', row[7])
            ruleset.addProperty('TARGET', row[8])

            rules = self.queryMDR(self.cursor, 'select * from designrules where techpack_version_id = \'' +
                       BaseTPName + '\' and parent_rule_name = \'' + row[2] + '\' order by execution_order;')
            for rule in rules:
                designrule = EntityDef(rule[2])
                designrule.addProperty('MATCH_TYPE', rule[5])
                designrule.addProperty('MATCH_ARGUMENT', rule[6])
                designrule.addProperty('MATCH_VALUE', rule[7])
                designrule.addProperty('TARGET', rule[8])
                ruleset.addComponent('Rules', designrule)

            package.addComponent('Rules', ruleset)
        designrules.addComponent('Rules', package)
        return designrules

    def queryMDR(self, cursor, query):
        cursor.execute(query)
        return cursor.fetchall()


if __name__ == '__main__':
    test = DesignTP()
    test.run('BASE_E_ERBS', 'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/NodeMomLimited_L17B_R27B01.xml')
