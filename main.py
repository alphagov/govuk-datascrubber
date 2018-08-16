import datascrubber
import datascrubber.mysql
import datascrubber.postgresql
import logging
import logging.handlers
import argparse
import os
import sys

DEFAULTS = {
    'source-mysql-hostname': 'mysql-primary',
    'source-postgresql-hostname': 'postgresql-primary',
}


def main():
    args = parse_arguments()
    configure_logging(args.log_mode, args.log_level)

    mysql_snapshot_finder = datascrubber.RdsSnapshotFinder(
        'mysql',
        hostname=args.source_mysql_hostname,
        source_instance_identifier=args.source_mysql_instance_identifier,
        snapshot_identifier=args.source_mysql_snapshot_identifier,
    )

    postgresql_snapshot_finder = datascrubber.RdsSnapshotFinder(
        'postgresql',
        hostname=args.source_postgresql_hostname,
        source_instance_identifier=args.source_postgresql_instance_identifier,
        snapshot_identifier=args.source_postgresql_snapshot_identifier,
    )

    mysql = datascrubber.mysql.MysqlScrubber(
        datascrubber.ScrubWorkspaceInstance(mysql_snapshot_finder)
    )

    postgresql = datascrubber.postgresql.PostgresqlScrubber(
        datascrubber.ScrubWorkspaceInstance(postgresql_snapshot_finder)
    )

    for task in mysql.get_viable_tasks():
        mysql.run_task(task)

    for task in postgresql.get_viable_tasks():
        postgresql.run_task(task)

    # TODO:
    #  * Run mysql and postgresql in parallel to reduce total runtime
    #  * dispose of workspaces and take final snapshots when done
    #  * set permissions on snapshots
    #  * emit Icinga passive check notification
    #  * catch errors and handle appropriately


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
            format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
            level=level,
        )
        return

    def log_config_syslog():
        logger = logging.getLogger()
        logger.setLevel(level)
        handler = logging.handlers.SysLogHandler(address='/dev/log')
        formatter = logging.Formatter('%(name)s: %(levelname)s: %(message)s')
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


if __name__ == '__main__':
    main()
