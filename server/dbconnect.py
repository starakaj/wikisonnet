import mysql.connector
import os, yaml

class MySQLDatabaseConnection:
    def __init__(self, dbname, user, host, password, options=None):
        self.dbname = dbname
        self.user = user
        self.host = host
        self.connection = mysql.connector.connect(user=user, password=password, host=host, database=dbname)
        self.statement = None
        self.execution_time = 0
        self.name_cache = {}

        if options is not None:
            for k in options:
                if k == "use_cache":
                    self.connection.cursor().execute("""SET SESSION query_cache_type=%s""", (options[k],))
                else:
                    print 'Ignoring unrecognized option {}'.format(k)

    @staticmethod
    def dbconfigForName(config_name):
        filename = os.path.join(os.path.dirname(__file__), 'dbconfig.yml')
        f = open(filename, 'r')
        databases = yaml.load(f)
        return databases[config_name]

    @staticmethod
    def connectionWithConfiguration(config):
        dbconfig = MySQLDatabaseConnection.dbconfigForName(config)
        return MySQLDatabaseConnection(dbconfig['database'], dbconfig['user'], dbconfig['host'], dbconfig['password'])

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None
