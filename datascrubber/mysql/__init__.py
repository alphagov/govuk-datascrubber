import logging
import mysql.connector
import re
import os
import os.path

logger = logging.getLogger(__name__)


class MysqlScrubber:
    def __init__(self, libdir, workspace, db_suffix='_production'):
        self.libdir = libdir
        self.workspace = workspace
        self.db_suffix = db_suffix
        self.supported_dbs = None
        self.connection = None
        self.supported_dbs = None
        self.available_dbs = None

    def get_supported_dbs(self):
        if self.supported_dbs is None:
            logger.info("Scanning %s for directories containing SQL scripts", self.libdir)

            names = os.listdir(self.libdir)
            dirs = [f for f in names if os.path.isdir(f)]
            self.supported_dbs = dirs

            logging.info("Found: %s", dirs)

        return self.supported_dbs

    def get_connection(self):
        if self.connection is None:
            instance = self.workspace.get_instance()

            logger.info("Connecting to MySQL: %s", {
                "endpoint": "{0}:{1}".format(
                    instance['Endpoint']['Host'],
                    instance['Endpoint']['Port']
                ),
                "user": instance['MasterUsername'],
            })

            self.connection = mysql.connector.connect(
                user=instance['MasterUsername'],
                password=self.workspace.password,
                host=instance['Endpoint']['Host'],
                port=instance['Endpoint']['Port'],
                database='information_schema'
            )

        return self.connection

    def get_available_dbs(self):
        if self.available_dbs is None:
            logger.info("Looking for available database in MySQL")

            cnx = self.get_connection()
            cursor = cnx.cursor()
            cursor.execute(
                "SELECT DISTINCT(table_schema) "
                "FROM TABLES "
                "WHERE table_schema NOT IN ("
                "    'information_schema',"
                "    'innodb', "
                "    'mysql', "
                "    'performance_schema', "
                "    'sys', "
                "    'tmp'"
                ");"
            )
            rows = cursor.fetchall()
            database_names = [r[0] for r in rows]
            logging.info("Database names: %s", database_names)
            self.available_dbs = database_names

        return self.available_dbs

    def get_normalised_available_dbs(self):
        r = re.compile('{0}$'.format(self.db_suffix))
        return [r.sub('', db) for db in self.get_available_dbs()]

    def get_viable_scrub_tasks(self):
        return set(self.get_supported_dbs()) & set(self.get_normalised_available_dbs())
