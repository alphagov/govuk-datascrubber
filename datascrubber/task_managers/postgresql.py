import logging
import psycopg2
import re
import subprocess
import shlex
import os
import time

import datascrubber.tasks

logger = logging.getLogger(__name__)


class Postgresql:
    def __init__(self, workspace, db_suffix='_production'):
        self.scrub_functions = {
            'email-alert-api': datascrubber.tasks.scrub_email_alert_api,
            'publishing_api': datascrubber.tasks.scrub_publishing_api,
        }
        self.db_realnames = {}

        self.workspace = workspace
        self.db_suffix = db_suffix
        self.viable_tasks = None

        self._discover_available_dbs()

    def _get_connection(self, dbname):
        instance = self.workspace.get_instance()

        logger.info("Connecting to Postgres: %s", {
            "endpoint": "{0}:{1}".format(
                instance['Endpoint']['Address'],
                instance['Endpoint']['Port']
            ),
            "user": instance['MasterUsername'],
            "database": dbname,
        })

        connection = psycopg2.connect(
            user=instance['MasterUsername'],
            password=self.workspace.password,
            host=instance['Endpoint']['Address'],
            port=instance['Endpoint']['Port'],
            dbname=dbname,
        )
        connection.autocommit = True
        return connection

    def _discover_available_dbs(self):
        logger.info("Looking for available databases in Postgres")

        cnx = self._get_connection('postgres')
        cursor = cnx.cursor()
        cursor.execute(
            "SELECT datname FROM pg_database "
            "WHERE datname NOT IN ("
            "  'template0', "
            "  'rdsadmin', "
            "  'postgres', "
            "  'template1' "
            ") AND datistemplate IS FALSE"
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
            return (True, None)

        except Exception as e:
            logger.error("Error running scrub task %s: %s", task, e)

            return (False, e)

    def export_to_s3(self, database, s3_url_prefix):
        endpoint = self.workspace.get_endpoint()

        pgdump_command = ' '.join(list(map(shlex.quote, [
            'pg_dump',
            '--host={0}'.format(endpoint['Address']),
            '--username={0}'.format(self.workspace.get_username()),
            '--dbname={0}'.format(self.db_realnames[database]),
        ])))

        s3_url = '{0}/{1}-{2}.sql.gz'.format(
            s3_url_prefix,
            time.strftime("%Y-%m-%dT%H:%M:%S"),
            self.db_realnames[database],
        )
        s3_command = 'aws s3 cp - {0}'.format(shlex.quote(s3_url))

        shell_command = '{0} | gzip | {1}'.format(pgdump_command, s3_command)

        logger.info("Copying %s to S3 as %s", database, s3_url)
        logger.debug("pg_dump command: %s", pgdump_command)
        logger.debug("s3 command: %s", s3_command)
        logger.debug("shell: %s", shell_command)

        try:
            # subprocess.check_output doesn't have an env parameter
            # (but subprocess.Popen does - WHY!)
            os.environ['PGPASSWORD'] = self.workspace.get_password()
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

        finally:
            os.environ.pop('PGPASSWORD')
