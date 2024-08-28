import os
from KYO_Tools.Parsers import ClassicMIMParser, ModelXMLLoader
from KYO_Tools.Environment import PostGreSQL
from KYO_Tools.Utilities import CLI_interface


class ModelConversionToSQL(object):

    def run(self, modelfilepath):
        cli = CLI_interface()
        cli.Blue('\tConverting ' + modelfilepath + ' Tech Pack Model to SQL')

        #Loading TP model to convert to SQL
        params = {}
        params['filepath'] = modelfilepath
        parser = ModelXMLLoader(params)
        TPModel = parser.parse()

        cli.White('\t\tGenerating SQL from Tech Pack Model')
        sqlfilename = 'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/' + TPModel.getName() + '.sql'
        if os.path.isfile(sqlfilename):
            os.remove(sqlfilename)

        sqlFile = open(sqlfilename, 'w+', encoding="utf8")

        TPModel = self.TECHPACK_VERSIONS(TPModel, sqlFile)
        TPModel = self.SUPPORTED_NODE_VERSIONS(TPModel, sqlFile)
        # TPModel = self.PERSISTENCE_ENTITIES(TPModel, sqlFile)
        TPModel = self.REPORTABLE_ENTITIES(TPModel, sqlFile)
        TPModel = self.ACTIONS_AND_SCHEDULER(TPModel, sqlFile)
        TPModel = self.NODE_MO_STRUCTURE(TPModel, sqlFile)
        TPModel = self.DESIGNRULES(TPModel, sqlFile)
        TPModel = self.RETENTION_PLAN(TPModel, sqlFile)

        sqlFile.close()
        cli.Blue('\tModel succesfully converted to SQL')

    def TECHPACK_VERSIONS(self, TPModel, sqlFile):
        TPModel.addProperty('TECHPACK_VERSION_ID', TPModel.getName())

        sql = 'INSERT INTO techpack_versions VALUES ('
        sql = sql + '\'' + TPModel.getProperty('TECHPACK_VERSION_ID') + '\','
        sql = sql + '\'' + TPModel.getProperty('TECHPACK_NAME') + '\','
        sql = sql + '\'' + TPModel.getProperty('TECHPACK_RELEASE') + '\','
        sql = sql + '\'' + TPModel.getProperty('TECHPACK_TYPE') + '\','
        sql = sql + '\'' + TPModel.getProperty('VENDOR') + '\','
        sql = sql + '\'' + TPModel.getProperty('TECHNOLOGY') + '\','
        sql = sql + '\'' + TPModel.getProperty('PRODUCT_NUMBER') + '\','
        sql = sql + '\'' + TPModel.getProperty('FEATURE_NAME') + '\','
        sql = sql + '\'' + TPModel.getProperty('DESCRIPTION') + '\','
        sql = sql + '\'' + TPModel.getProperty('LICENSE') + '\''

        if TPModel.hasProperty('DEPENDENCY'):
            dependencies = TPModel.getProperty('DEPENDENCY').split(',')
            sql = sql + ',ARRAY ['
            for dependency in dependencies:
                if not sql.endswith('['):
                    sql = sql + ','
                sql = sql + '\'' + dependency + '\''

            sql = sql + ']'

        sql = sql + ');\n'
        sqlFile.write(sql)
        return TPModel

    def SUPPORTED_NODE_VERSIONS(self, TPModel, sqlFile):
        if TPModel.hasComponent('SUPPORTED_NODE_VERSIONS'):
            for entityName, entity in TPModel.getComponentGroup('SUPPORTED_NODE_VERSIONS').iteritems():
                sql = 'INSERT INTO supported_node_versions VALUES ('
                sql = sql + '\'' + TPModel.getProperty('TECHPACK_VERSION_ID') + '\','
                sql = sql + '\'' + entityName + '\','
                sql = sql + '\'' + entity.getProperty('MODEL_VERSION') + '\','
                sql = sql + '\'' + entity.getProperty('DESCRIPTION') + '\''
                sql = sql + ');\n'
                sqlFile.write(sql)
        return TPModel

    def NODE_MO_STRUCTURE(self, TPModel, sqlFile):
        if TPModel.hasComponent('NODE_MO_STRUCTURE'):
            for entityName, entity in TPModel.getComponentGroup('NODE_MO_STRUCTURE').iteritems():
                sql = 'INSERT INTO node_mo_structure VALUES ('
                sql = sql + '\'' + TPModel.getProperty('TECHPACK_VERSION_ID') + '\','
                sql = sql + '\'' + entity.getProperty('PARENT_MO_NAME') + '\','
                sql = sql + '\'' + entity.getProperty('CHILD_MO_NAME') + '\','
                sql = sql + entity.getProperty('CARDINALITY_MIN')

                max = entity.getProperty('CARDINALITY_MAX')
                if max != '' and max != 'None':
                    sql = sql + ',' + entity.getProperty('CARDINALITY_MAX')
                sql = sql + ');\n'
                sqlFile.write(sql)
        return TPModel

    def RETENTION_PLAN(self, TPModel, sqlFile):
        if TPModel.hasComponent('RETENTION_PLAN'):
            for entityName, entity in TPModel.getComponentGroup('RETENTION_PLAN').iteritems():
                sql = 'INSERT INTO retention_plan VALUES ('
                sql = sql + '\'' + entityName + '\','
                sql = sql + '\'' + entity.getProperty('RETENTION_PERIOD') + '\','
                sql = sql + '\'' + entity.getProperty('HOT_STORE_RETENTION_PERIOD') + '\''
                sql = sql + ');\n'
                sqlFile.write(sql)
        return TPModel

    def REPORTABLE_ENTITIES(self, TPModel, sqlFile):
        if TPModel.hasComponent('REPORTABLE_ENTITIES'):
            for repEntityName, repEntity in TPModel.getComponentGroup('REPORTABLE_ENTITIES').iteritems():
                rep_entity_id = TPModel.getProperty('TECHPACK_VERSION_ID') + ':' + repEntityName
                sql = 'INSERT INTO reportable_entity VALUES ('
                sql = sql + '\'' + rep_entity_id + '\','
                sql = sql + '\'' + TPModel.getProperty('TECHPACK_VERSION_ID') + '\','
                sql = sql + '\'' + repEntityName + '\','
                sql = sql + '\'' + repEntity.getProperty('ENTITY_TYPE') + '\','
                sql = sql + '\'' + repEntity.getProperty('PARTITION_PLAN') + '\','
                sql = sql + '\'' + repEntity.getProperty('RETENTION_PLAN') + '\','
                sql = sql + '\'' + repEntity.getProperty('DESCRIPTION') + '\''
                sql = sql + ');\n'
                sqlFile.write(sql)

                if repEntity.hasProperty('MAPPED_MO'):
                    sql = 'INSERT INTO mo_table_mapping VALUES ('
                    sql = sql + '\'' + TPModel.getProperty('TECHPACK_VERSION_ID') + '\','
                    sql = sql + '\'' + repEntity.getProperty('MAPPED_MO') + '\','
                    sql = sql + '\'' + repEntityName + '\');\n'
                    sqlFile.write(sql)


                for repEntityColName, repColEntity in repEntity.getComponentGroup(
                        'REPORTABLE_ENTITIES_COLUMNS').iteritems():
                    sql = 'INSERT INTO reportable_entity_columns VALUES ('
                    sql = sql + '\'' + TPModel.getProperty('TECHPACK_VERSION_ID') + '\','
                    sql = sql + '\'' + rep_entity_id + '\','
                    sql = sql + '\'' + rep_entity_id + ':' + repEntityColName + '\','
                    sql = sql + '\'' + repEntityColName + '\','
                    sql = sql + repColEntity.getProperty('ENTITY_COLUMN_ORDER') + ','
                    sql = sql + '\'' + repColEntity.getProperty('ENTITY_COLUMN_TYPE') + '\','
                    sql = sql + '\'' + repColEntity.getProperty('ENTITY_DATA_TYPE') + '\','
                    sql = sql + '\'' + repColEntity.getProperty('ENTITY_COUNTER_TYPE') + '\','
                    sql = sql + '\'' + repColEntity.getProperty('PARTITION_BY') + '\','
                    sql = sql + '\'' + repColEntity.getProperty('ENTITY_DESCRIPTION').replace('\'', '') + '\''
                    sql = sql + ');\n'
                    sqlFile.write(sql)

                    sql = 'INSERT INTO reportable_entity_column_source VALUES ('
                    sql = sql + '\'' + rep_entity_id + ':' + repEntityColName + '\','
                    sql = sql + '\'' + TPModel.getProperty('TECHPACK_VERSION_ID') + '\','
                    sql = sql + '\'' + rep_entity_id + '\','
                    sql = sql + '\'' + repColEntity.getProperty('SOURCE_ENTITY_NAME') + '\','
                    sql = sql + '\'' + repColEntity.getProperty('SOURCE_ENTITY_COLUMN_NAME') + '\''
                    sql = sql + ');\n'
                    sqlFile.write(sql)

        return TPModel


    def DESIGNRULES(self, TPModel, sqlFile):
        if TPModel.hasComponent('DESIGNRULES'):
            for targetName, targetRulesEntity in TPModel.getComponentGroup('DESIGNRULES').iteritems():
                order = 0
                for Ruleset, RulesetEntity in targetRulesEntity.getComponentGroup('Rules').iteritems():
                    self.createDesignRules(TPModel.getProperty('TECHPACK_VERSION_ID'), targetName, Ruleset,
                                           TPModel.getProperty('TECHPACK_NAME'), order, RulesetEntity, sqlFile)

                    ruleOrder = 0
                    for rule, ruleEntity in RulesetEntity.getComponentGroup('Rules').iteritems():
                        self.createDesignRules(TPModel.getProperty('TECHPACK_VERSION_ID'), targetName,
                                               rule, Ruleset, ruleOrder, ruleEntity,sqlFile)
                        ruleOrder = ruleOrder + 1

                    order = order + 1
        return TPModel

    def createDesignRules(self, tpname, targetpackage, rulename, parentrulename, order, RulesetEntity, sqlFile):
        sql = 'INSERT INTO designrules VALUES ('
        sql = sql + '\'' + tpname + '\','
        sql = sql + '\'' + targetpackage + '\','
        sql = sql + '\'' + rulename + '\','
        sql = sql + '\'' + parentrulename + '\','
        sql = sql + str(order) + ' ,'
        sql = sql + '\'' + RulesetEntity.getProperty('MATCH_TYPE') + '\','
        sql = sql + '\'' + RulesetEntity.getProperty('MATCH_ARGUMENT') + '\','
        sql = sql + '\'' + RulesetEntity.getProperty('MATCH_VALUE') + '\','
        sql = sql + '\'' + RulesetEntity.getProperty('TARGET') + '\''
        sql = sql + ');\n'
        sqlFile.write(sql)

    def ACTIONS_AND_SCHEDULER(self, TPModel, sqlFile):
        if TPModel.hasComponent('Flows'):
            for flowName, flowEntity in TPModel.getComponentGroup('Flows').iteritems():

                for actionName, actionEntity in flowEntity.getComponentGroup('Actions').iteritems():

                    if actionEntity.hasProperty('ROP_INTERVAL'):
                        sql = 'INSERT INTO flow_scheduling VALUES ('
                        sql = sql + '\'' + TPModel.getName() + '\','
                        sql = sql + '\'' + flowName + '\','
                        sql = sql + '\'' + actionEntity.getName() + '\','
                        sql = sql + '\'' + actionEntity.getProperty('WORKER_NAME') + '\','
                        sql = sql + '\'' + actionEntity.getProperty('REOCCURING') + '\','
                        sql = sql + '\'' + actionEntity.getProperty('ROP_INTERVAL') + '\','
                        sql = sql + '\'' + actionEntity.getProperty('ROP_OFFSET') + '\','
                        sql = sql + '\'' + actionEntity.getProperty('ENABLED') + '\''
                        sql = sql + ');\n'
                        sqlFile.write(sql)

                    config = ''
                    for propName, propValue in actionEntity.getProperties().iteritems():
                        if propName.startswith('CONFIG_'):
                            if config != '':
                                config = config + '::'
                            config = config + propName.replace('CONFIG_', '') + '=' + propValue


                    sql = 'INSERT INTO flow_configuration VALUES ('
                    sql = sql + '\'' + TPModel.getName() + '\','
                    sql = sql + '\'' + flowName + '\','
                    sql = sql + '\'' + actionEntity.getName() + '\','
                    sql = sql + '\'' + actionEntity.getProperty('WORKER_NAME') + '\','
                    sql = sql + '\'' + config + '\','
                    sql = sql + '\'' + actionEntity.getProperty('DEPENDENCY_FLOW_NAME') + '\','
                    sql = sql + '\'' + actionEntity.getProperty('DEPENDENCY_ACTION_NAME') + '\''
                    sql = sql + ');\n'
                    sqlFile.write(sql)
        return TPModel

if __name__ == '__main__':
    test = ModelConversionToSQL()
    test.run()
