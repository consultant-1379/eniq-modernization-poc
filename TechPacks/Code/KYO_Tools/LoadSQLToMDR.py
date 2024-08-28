import sys
from KYO_Tools.Parsers import ClassicMIMParser, ModelXMLLoader
from KYO_Tools.Environment import PostGreSQL
from KYO_Tools.Utilities import CLI_interface


class LoadSQLToMDR(object):

    def run(self, sqlfilepath):
        cli = CLI_interface()
        cli.Blue('\tApplying ' + sqlfilepath + ' to MDR')

        postgres = PostGreSQL()

        cli.White('\t\tCheck for MDR container')
        if not postgres.isonline():
            cli.Red('\t\tNo PostgreSQL container found.')
            sys.exit()

        cli.White('\t\tFound active MDR container. Creating a connection')
        connection = postgres.getconnection()
        cursor = connection.cursor()

        cli.White('\t\tRunning SQL file into MDR')
        sql = ''
        with open(sqlfilepath) as fp:
            for line in fp:
                sql = sql + line
                if sql.endswith(');\n'):
                    #print(sql)
                    cursor.execute(sql)
                    sql = ''

        cli.White('\t\tCommitting changes and closing MDR connection')
        connection.commit()
        if connection:
            cursor.close()
            connection.close()

        cli.Blue('\tSQL file successfully ran into MDR')


if __name__ == '__main__':
    test = LoadSQLToMDR()
    test.run('C:/Users/ebrifol/Dev/NextGen/eniq-modernization-poc/TechPacks/scratchpad/BASE_E_ERBS_R1A01.sql')
