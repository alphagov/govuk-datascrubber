import boto3
import random
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ScrubWorkspaceInstance:
    def __init__(self, snapshot_finder, timeout=90, security_groups=None):
        self.rds_client = boto3.client('rds')
        self.snapshot_finder = snapshot_finder
        self.timeout = timeout
        self.instance_identifier = "scrubber-workspace-{0}".format(datetime.now().strftime("%Y%m%d%H%M%S"))
        self.password = "{0:x}".format(random.getrandbits(41 * 4))
        self.instance = None
        self.source_instance = self.snapshot_finder.get_source_instance()

        if type(security_groups) == str:
            self.security_groups = [security_groups]

        elif type(security_groups) == list:
            self.security_groups = security_groups

        else:
            self.security_groups = [
                sg['VpcSecurityGroupId'] for sg
                in self.source_instance['VpcSecurityGroups']
                if sg['Status'] == 'active'
            ]

        logger.info(
            "Initialised scrub workspace instance, DBInstanceIdentifier: %s",
            self.instance_identifier
        )

    def get_endpoint(self):
        i = self.get_instance()
        return i['Endpoint']

    def get_instance(self):
        if self.instance is None:
            logger.info(
                "Instance %s doesn't exist yet, creating",
                self.instance_identifier
            )
            self.__create_instance()
            self.__apply_instance_modifications()

        return self.instance

    def cleanup(self):
        if self.instance is not None:
            rds = self.rds_client
            logger.info("Deleting RDS instance %s", self.instance_identifier)
            rds.delete_db_instance(
                DBInstanceIdentifier=self.instance_identifier(),
                FinalDBSnapshotIdentifier=self.final_snapshot_identifier(),
            )

    # TODO: Return a DSN suitable for configuring the MySQL or Postgres connector lib
    def get_dsn(self):
        return "todo"

    def __create_instance(self):
        rds = self.rds_client
        source_snapshot_id = self.snapshot_finder.get_snapshot_identifier()
        subnet_group_name = self.source_instance['DBSubnetGroup']['DBSubnetGroupName']

        logger.info(
            "Restoring instance %s from snapshot %s in subnet group %s. Timeout: %s minutes",
            self.instance_identifier,
            source_snapshot_id,
            subnet_group_name,
            self.timeout,
        )

        restore_response = rds.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier=self.instance_identifier,
            DBSnapshotIdentifier=source_snapshot_id,
            DBSubnetGroupName=subnet_group_name,
        )
        # TODO check for error

        max_end_time = time.time() + 60 * self.timeout
        while time.time() <= max_end_time:
            poll_response = rds.describe_db_instances(
                DBInstanceIdentifier=self.instance_identifier
            )
            # TODO check for error
            self.instance = poll_response['DBInstances'][0]

            logger.info(
                "Waiting for %s to become available, current status: '%s'",
                self.instance_identifier,
                self.instance['DBInstanceStatus'],
            )

            if self.instance['DBInstanceStatus'] == 'available':
                return
            else:
                time.sleep(5)

        raise TimeoutError(
            "Timed out creating RDS instance {0}".format(
                self.instance_identifier
            )
        )

    def __apply_instance_modifications(self):
        rds = self.rds_client

        logger.info(
            "Applying modifications to %s: %s",
            self.instance_identifier,
            {
                'VpcSecurityGroupIds': self.security_groups,
                'MasterUserPassword': '****'
            },
        )

        rds.modify_db_instance(
            DBInstanceIdentifier=self.instance_identifier,
            VpcSecurityGroupIds=self.security_groups,
            MasterUserPassword=self.password,
        )

        max_end_time = time.time() + 60 * self.timeout
        while time.time() <= max_end_time:
            poll_response = rds.describe_db_instances(
                DBInstanceIdentifier=self.instance_identifier
            )
            pending = list(
                poll_response['DBInstances'][0]['PendingModifiedValues'].keys()
            )

            if len(pending) > 0:
                logger.info(
                    "Modifications to %s still pending: %s",
                    self.instance_identifier,
                    pending,
                )
                time.sleep(5)
            else:
                return

        raise TimeoutError(
            "Timed out applying changes to RDS instance {0}".format(
                self.instance_identifier
            )
        )
