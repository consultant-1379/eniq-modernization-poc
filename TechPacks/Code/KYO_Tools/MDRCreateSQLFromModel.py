import os
from KYO_Tools.Parsers import ClassicMIMParser, ModelXMLLoader
from KYO_Tools.Utilities import CLI_interface


class MDRCreateSQLFromModel(object):

    def run(self):
        cli = CLI_interface()
        cli.Blue('\tCreating MDR schema')

        cli.White('\t\tLoading MDR data schema : C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/MDR_Schema.xml')
        params = {}
        params['filepath'] = 'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/MDR_Schema.xml'
        parser = ModelXMLLoader(params)
        dbschema = parser.parse()

        cli.White('\t\tGenerating SQL to create MDR schema')
        sqlfilepath = 'C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/' + dbschema.getName() + '.sql'

        if os.path.isfile(sqlfilepath):
            os.remove(sqlfilepath)
        outputFile = open(sqlfilepath, 'w+')
        self.deployschema(dbschema, outputFile)
        outputFile.close()
        cli.White('\t\tSQL file successfully created: ' + sqlfilepath)

        cli.Blue('\tMDR schema created successfully')


    def deployschema(self, dbschema, outputFile):
        tables = dbschema.getComponentGroup('Tables')
        for tablename, table in tables.iteritems():
            outputFile.write('DROP Table if exists ' + tablename + ';\n')

            primary_keys = []

            createsql = 'CREATE TABLE ' + tablename + ' ( '
            for columnname, column in table.getComponentGroup('Columns').iteritems():
                if not createsql.endswith(' ( '):
                    createsql = createsql + ', '

                createsql = createsql + columnname + ' ' + column.getProperty('Column_Type')
                if column.hasProperty('Column_Size'):
                    createsql = createsql + '(' + column.getProperty('Column_Size') + ')'

                if column.hasProperty('Primary_Key'):
                    primary_keys.append(columnname)

            if len(primary_keys) > 0:
                createsql = createsql + ', PRIMARY KEY ( ' + ','.join(primary_keys) + ')'


            if table.hasComponent('Foreign_Keys'):
                for sourceColname, foreignkey in table.getComponentGroup('Foreign_Keys').iteritems():
                    createsql = createsql + ', FOREIGN KEY ( ' + sourceColname + \
                                ' ) REFERENCES ' + foreignkey.getProperty('Table_Name') + \
                                ' ( ' + foreignkey.getProperty('Column_Name') + ' )'

            createsql = createsql + ');\n'
            outputFile.write(createsql)


if __name__ == '__main__':
    test = MDRCreateSQLFromModel()
    test.run()
