import logging
import mysql.connector
import re

from .whitehall import scrub_whitehall

logger = logging.getLogger(__name__)


class MysqlScrubber:
    def __init__(self, workspace, db_suffix='_production'):
        self.scrub_functions = {
            'whitehall': scrub_whitehall,
        }
        self.db_realnames = {}

        self.workspace = workspace
        self.db_suffix = db_suffix
        self.viable_tasks = None

        self._discover_available_dbs()

    def _get_connection(self, dbname):
        instance = self.workspace.get_instance()

        logger.info("Connecting to MySQL: %s", {
            "endpoint": "{0}:{1}".format(
                instance['Endpoint']['Address'],
                instance['Endpoint']['Port']
            ),
            "user": instance['MasterUsername'],
            "database": dbname,
        })

        connection = mysql.connector.connect(
            user=instance['MasterUsername'],
            password=self.workspace.password,
            host=instance['Endpoint']['Address'],
            port=instance['Endpoint']['Port'],
            database=dbname,
        )
        return connection

    def _discover_available_dbs(self):
        logger.info("Looking for available database in MySQL")

        cnx = self._get_connection('information_schema')
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
        available_dbs = [r[0] for r in rows]
        logging.info("Databases found: %s", available_dbs)

        r = re.compile('{0}$'.format(self.db_suffix))
        for database_name in available_dbs:
            normalised_name = r.sub('', database_name)
            self.db_realnames[normalised_name] = database_name

    def get_viable_tasks(self):
        if self.viable_tasks is None:
            self.viable_tasks = list(
                set(self.scrub_functions.keys()) &
                set(self.db_realnames.keys())
            )
            logger.info("Viable scrub tasks: %s", self.viable_tasks)

        return self.viable_tasks

    def run_task(self, task):
        if task not in self.get_viable_tasks():
            logger.error(
                "%s is not a viable scrub task for %s",
                task, self.__class__
            )
            return

        logger.info("Running scrub task: %s", task)
        cnx = self._get_connection(self.db_realnames[task])
        cursor = cnx.cursor()
        return self.scrub_functions[task](cursor)
