import datascrubber
import datascrubber.mysql
import datascrubber.postgresql
import logging
import logging.handlers
import argparse
import os
import sys
import threading
import traceback
import boto3

DEFAULTS = {
    'source-mysql-hostname': 'mysql-primary',
    'source-postgresql-hostname': 'postgresql-primary',
}


def main():
    args = parse_arguments()
    configure_logging(args.log_mode, args.log_level)

    thread = threading.Thread(
        target=worker,
        args=(
            'mysql',
            args.source_mysql_hostname,
            args.source_mysql_instance_identifier,
            args.source_mysql_snapshot_identifier,
        )
    )
    thread.start()

    thread = threading.Thread(
        target=worker,
        args=(
            'postgresql',
            args.source_postgresql_hostname,
            args.source_postgresql_instance_identifier,
            args.source_postgresql_snapshot_identifier,
        )
    )
    thread.start()


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='GOV.UK data scrubber'
    )

    parser.add_argument(
        '--source-mysql-hostname',
        required=False,
        type=str,
        default=os.environ.get(
            'SOURCE_MYSQL_HOSTNAME',
            DEFAULTS['source-mysql-hostname']
        ),
        help="Hostname of source MySQL instance/cluster (default: {0})".format(
            DEFAULTS['source-mysql-hostname']
        ),
    )

    parser.add_argument(
        '--source-mysql-instance-identifier',
        required=False,
        type=str,
        default=os.environ.get('SOURCE_MYSQL_INSTANCE_IDENTIFIER'),
        help="RDS instance identifier of source MySQL RDS instance. "
             "If specified, supercedes --source-mysql-hostname."
    )

    parser.add_argument(
        '--source-mysql-snapshot-identifier',
        required=False,
        type=str,
        default=os.environ.get('SOURCE_MYSQL_SNAPSHOT_IDENTIFIER'),
        help="Snapshot identifier to use for MySQL. Defaults to the most "
             "recent automatic snapshot for the source instance/cluster. If "
             "specified, supercedes --source-mysql-hostname and "
             "--source-mysql-snapshot-identifier."
    )

    parser.add_argument(
        '--source-postgresql-hostname',
        required=False,
        type=str,
        default=os.environ.get(
            'SOURCE_POSTGRESQL_HOSTNAME',
            DEFAULTS['source-postgresql-hostname']
        ),
        help="Hostname of source Postgres instance/cluster (default: {0})".format(
            DEFAULTS['source-postgresql-hostname']
        ),
    )

    parser.add_argument(
        '--source-postgresql-instance-identifier',
        required=False,
        type=str,
        default=os.environ.get('SOURCE_POSTGRESQL_INSTANCE_IDENTIFIER'),
        help="RDS instance identifier of source Postgres RDS instance. If "
             "specified, supercedes --source-postgresql-hostname."
    )

    parser.add_argument(
        '--source-postgresql-snapshot-identifier',
        required=False,
        type=str,
        default=os.environ.get('SOURCE_POSTGRESQL_SNAPSHOT_IDENTIFIER', None),
        help="Snapshot identifier to use for Postgres. Defaults to the most "
             "recent automatic snapshot for the source instance/cluster. If "
             "specified, supercedes --source-postgresql-hostname and "
             "--source-postgresql-instance-identifier."
    )

    parser.add_argument(
        '--log-level',
        required=False,
        type=str,
        default=os.environ.get('LOG_LEVEL', 'INFO'),
        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
        help="Log level (default: INFO)",
    )

    parser.add_argument(
        '--log-mode',
        required=False,
        type=str,
        default=os.environ.get('LOG_MODE'),
        choices=['console', 'syslog'],
    )

    task_selection = parser.add_mutually_exclusive_group()
    task_selection.add_argument(
        '--confine-to-tasks',
        required=False,
        type=str,
        default=os.environ.get('CONFINE_TO_TASKS'),
        nargs='+',
        help="Only run the given tasks",
    )
    task_selection.add_argument(
        '--skip-tasks',
        required=False,
        type=str,
        default=os.environ.get('SKIP_TASKS'),
        nargs='+',
        help="Don't run the given tasks",
    )

    parser.add_argument(
        '--share-with',
        required=False,
        type=str,
        default=os.environ.get('SHARE_WITH'),
        nargs='+',
        help="ARNs to share scrubbed snapshots with",
    )

    parser.add_argument(
        '--icinga-host',
        required=False,
        type=str,
        default=os.environ.get('ICINGA_HOST'),
        help="Icinga host to notify of result",
    )

    return parser.parse_args()


def configure_logging(mode, level_name):
    level = {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG
    }.get(level_name)

    def log_config_console():
        logging.basicConfig(
            format='%(asctime)s | %(levelname)s | %(threadName)s | %(name)s | %(message)s',
            level=level,
        )
        return

    def log_config_syslog():
        logger = logging.getLogger()
        logger.setLevel(level)
        handler = logging.handlers.SysLogHandler(address='/dev/log')
        formatter = logging.Formatter('%(name)s | %(levelname)s | %(threadName)s | %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return

    if mode is not None:
        if mode == 'syslog':
            return log_config_syslog()
        if mode == 'console':
            return log_config_console()

    if sys.stdout.isatty():
        return log_config_console()

    elif os.environ.get('LAMBDA_TASK_ROOT') is None:
        return log_config_console()

    else:
        return log_config_syslog()


def worker(dbms, hostname=None, instance=None, snapshot=None):
    logger = logging.getLogger()
    logger.info("Spawned new worker thread")

    # We need a boto3 session per thread
    # https://boto3.readthedocs.io/en/latest/guide/resources.html#multithreading-multiprocessing
    session = boto3.session.Session()
    workspace = None

    try:
        snapshot_finder = datascrubber.RdsSnapshotFinder(
            dbms,
            boto3_session=session,
            hostname=hostname,
            source_instance_identifier=instance,
            snapshot_identifier=snapshot,
        )

        workspace = datascrubber.ScrubWorkspaceInstance(
            snapshot_finder,
            session,
        )

        if dbms == 'mysql':
            task_manager = datascrubber.mysql.MysqlScrubber(workspace)
        elif dbms == 'postgresql':
            task_manager = datascrubber.postgresql.PostgresqlScrubber(workspace)
        else:
            raise Exception("DBMS not supported: %s" % dbms)

        success = True
        for task in task_manager.get_viable_tasks():
            success = task_manager.run_task(task)
            if not success:
                logger.error(
                    "Task %s failed. A final snapshot will not be generated, "
                    "in case sensitive data remains.",
                    task
                )
                break

        workspace.cleanup(create_final_snapshot=success)

    except Exception as e:
        if workspace is None:
            logger.critical(
                "Worker encountered an unrecoverable error: %s, traceback: %s", e,
                traceback.format_tb(e.__traceback__)
            )
        else:
            logger.critical(
                "Worker encountered an unrecoverable error. Assuming the scrub "
                "was unsuccessful; a final snapshot will not be generated, in case "
                "sensitive data remains. The error was: %s, traceback: %s", e,
                traceback.format_tb(e.__traceback__)
            )
            workspace.cleanup(create_final_snapshot=False)


if __name__ == '__main__':
    main()
