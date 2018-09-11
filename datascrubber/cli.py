import logging
import logging.handlers
import argparse
import os
import sys
import threading
import traceback
import boto3

from . import ScrubWorkspaceInstance, RdsSnapshotFinder
from .task_managers import Mysql, Postgresql


def main():
    args = parse_arguments()
    configure_logging(args.log_mode, args.log_level)
    logger = logging.getLogger()
    logger.info('Starting up')

    threads = []

    if args.mysql_snapshots is not None:
        for snap_id in args.mysql_snapshots:
            thread = threading.Thread(
                target=worker,
                kwargs=({
                    'dbms': 'mysql',
                    'snapshot': snap_id,
                    'target_accounts': args.share_with,
                }),
            )
            threads.append(thread)

    elif args.mysql_instances is not None:
        for instance_id in args.mysql_instances:
            thread = threading.Thread(
                target=worker,
                kwargs=({
                    'dbms': 'mysql',
                    'instance': instance_id,
                    'target_accounts': args.share_with,
                })
            )
            threads.append(thread)

    elif args.mysql_hosts is not None:
        for host in args.mysql_hosts:
            thread = threading.Thread(
                target=worker,
                kwargs=({
                    'dbms': 'mysql',
                    'hostname': host,
                    'target_accounts': args.share_with,
                })
            )
            threads.append(thread)

    if args.postgresql_snapshots is not None:
        for snap_id in args.postgresql_snapshots:
            thread = threading.Thread(
                target=worker,
                kwargs=({
                    'dbms': 'postgresql',
                    'snapshot': snap_id,
                    'target_accounts': args.share_with,
                }),
            )
            threads.append(thread)

    elif args.postgresql_instances is not None:
        for instance_id in args.postgresql_instances:
            thread = threading.Thread(
                target=worker,
                kwargs=({
                    'dbms': 'postgresql',
                    'instance': instance_id,
                    'target_accounts': args.share_with,
                })
            )
            threads.append(thread)

    elif args.postgresql_hosts is not None:
        for host in args.postgresql_hosts:
            thread = threading.Thread(
                target=worker,
                kwargs=({
                    'dbms': 'postgresql',
                    'hostname': host,
                    'target_accounts': args.share_with,
                })
            )
            threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    logger.info('All tasks completed')


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='GOV.UK data scrubber'
    )

    mysql_selection = parser.add_mutually_exclusive_group()
    mysql_selection.add_argument(
        '--mysql-hosts',
        required=False,
        nargs='+',
        type=str,
        help='Hostnames of source MySQL instances or clusters'
    )

    mysql_selection.add_argument(
        '--mysql-instances',
        required=False,
        nargs='+',
        type=str,
        help="RDS instance identifiers of source MySQL RDS instances or "
             "clusters. If specified, supercedes --mysql-hosts."
    )

    mysql_selection.add_argument(
        '--mysql-snapshots',
        required=False,
        type=str,
        nargs='+',
        help="Snapshot identifiers to use for MySQL. Defaults to the most "
             "recent automatic snapshot for each instance or cluster. If "
             "specified, supercedes --mysql-hosts and --mysql-instances."
    )

    postgresql_selection = parser.add_mutually_exclusive_group()
    postgresql_selection.add_argument(
        '--postgresql-hosts',
        required=False,
        nargs='+',
        type=str,
        help="Hostname of source Postgres instances or clusters"
    )

    postgresql_selection.add_argument(
        '--postgresql-instances',
        required=False,
        nargs='+',
        type=str,
        help="RDS instance identifiers of source Postgres RDS instances or "
             "clusters. If specified, supercedes --postgresql-hosts."
    )

    postgresql_selection.add_argument(
        '--postgresql-snapshots',
        required=False,
        nargs='+',
        type=str,
        help="Snapshot identifiers to use for Postgres. Defaults to the most "
             "recent automatic snapshot for each instance or cluster. If "
             "specified, supercedes --postgresql-hosts and --postgresql-instances."
    )

    parser.add_argument(
        '--log-level',
        required=False,
        type=str,
        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
        default='INFO',
        help="Log level (default: INFO)",
    )

    parser.add_argument(
        '--log-mode',
        required=False,
        type=str,
        choices=['console', 'syslog'],
    )

    parser.add_argument(
        '--share-with',
        required=False,
        type=str,
        nargs='+',
        help="AWS account IDs to share scrubbed snapshots with",
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
            format='%(asctime)s | %(levelname)s | %(threadName)s | %(name)s %(funcName)s | %(message)s',
            level=level,
        )
        return

    def log_config_syslog():
        logger = logging.getLogger()
        logger.setLevel(level)
        handler = logging.handlers.SysLogHandler(address='/dev/log')
        formatter = logging.Formatter('datascrubber: %(levelname)s: [%(threadName)s] [%(funcName)s] %(message)s')
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

    else:
        return log_config_syslog()


def worker(dbms, hostname=None, instance=None, snapshot=None, target_accounts=[]):
    logger = logging.getLogger()
    logger.info("Spawned new worker thread")

    # We need a boto3 session per thread
    # https://boto3.readthedocs.io/en/latest/guide/resources.html#multithreading-multiprocessing
    session = boto3.session.Session()
    rds_client = session.client('rds')
    workspace = None

    try:
        snapshot_finder = RdsSnapshotFinder(
            dbms,
            boto3_session=session,
            hostname=hostname,
            source_instance_identifier=instance,
            snapshot_identifier=snapshot,
        )

        workspace = ScrubWorkspaceInstance(
            snapshot_finder,
            session,
        )

        if dbms == 'mysql':
            task_manager = Mysql(workspace)
        elif dbms == 'postgresql':
            task_manager = Postgresql(workspace)
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
        if success:
            if target_accounts is not None and len(target_accounts) >= 1:
                logger.info(
                    "Sharing snapshot %s with AWS accounts %s",
                    workspace.final_snapshot_identifier, target_accounts
                )
                rds_client.modify_db_snapshot_attribute(
                    DBSnapshotIdentifier=workspace.final_snapshot_identifier,
                    AttributeName='restore',
                    ValuesToAdd=target_accounts,
                )

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
