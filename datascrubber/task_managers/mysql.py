import logging
import mysql.connector
import re
import subprocess
import shlex
import time

import datascrubber.tasks

logger = logging.getLogger(__name__)


class Mysql:
    def __init__(self, workspace, db_suffix='_production', icinga_host=None):
        self.scrub_functions = {
            'whitehall': datascrubber.tasks.scrub_whitehall,
        }
        self.db_realnames = {}

        self.workspace = workspace
        self.db_suffix = db_suffix
        self.viable_tasks = None
        self.icinga_host = icinga_host

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
        logger.info("Databases found: %s", available_dbs)

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
            err = "{0} is not a viable scrub task for {1}".format(
                task, self.__class__
            )
            logger.error(err)
            return (False, err)

        logger.info("Running scrub task: %s", task)
        cnx = self._get_connection(self.db_realnames[task])
        cursor = cnx.cursor()
        try:
            self.scrub_functions[task](cursor)
            cnx.commit()
            cursor.close()
            cnx.close()

            return (True, None)

        except Exception as e:
            logger.error("Error running scrub task %s: %s", task, e)
            cnx.rollback()
            cursor.close()
            cnx.close()

            return (False, e)

    def export_to_s3(self, database, s3_url_prefix):
        endpoint = self.workspace.get_endpoint()

        mysqldump_command = ' '.join(list(map(shlex.quote, [
            'mysqldump',
            '--host={0}'.format(endpoint['Address']),
            '--user={0}'.format(self.workspace.get_username()),
            '--password={0}'.format(self.workspace.get_password()),
            self.db_realnames[database],
        ])))

        s3_url = '{0}/{1}-{2}.sql.gz'.format(
            s3_url_prefix,
            time.strftime("%Y-%m-%dT%H:%M:%S"),
            self.db_realnames[database],
        )
        s3_command = 'aws s3 cp - {0}'.format(shlex.quote(s3_url))

        shell_command = '{0} | gzip | {1}'.format(mysqldump_command, s3_command)

        logger.info("Copying %s to S3 as %s", database, s3_url)
        logger.debug("mysqldump command: %s", mysqldump_command)
        logger.debug("s3 command: %s", s3_command)
        logger.debug("shell: %s", shell_command)

        try:
            output = subprocess.check_output(
                shell_command,
                shell=True,
                stderr=subprocess.STDOUT,
            )
            logger.info(
                "Finished copying %s to S3: %s", database,
                {'command': shell_command,
                 'exitcode': 0,
                 'output': output.decode('utf-8')}
            )

        except subprocess.CalledProcessError as e:
            logger.error(
                "Error copying %s to S3: %s",
                database,
                {'command': shell_command,
                 'exitcode': e.returncode,
                 'output': e.output.decode('utf-8')}
            )
