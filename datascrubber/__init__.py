import boto3
import random
import time
import logging
import dns.resolver
import dns.name
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
            Tags=[
                {
                    'Key': 'scrubber',
                    'Value': 'scrubber'
                }
            ]
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
                time.sleep(10)

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
                time.sleep(10)
            else:
                return

        raise TimeoutError(
            "Timed out applying changes to RDS instance {0}".format(
                self.instance_identifier
            )
        )


class RdsSnapshotFinder:
    rds_domain = dns.name.from_text('rds.amazonaws.com.')

    def __init__(self, dbms, hostname=None, source_instance_identifier=None, snapshot_identifier=None):
        if dbms not in ['mysql', 'postgresql']:
            raise Exception('dbms must be one of mysql, postgresql')

        self.dbms = dbms
        self.hostname = hostname
        self.rds_endpoint_address = None
        self.source_instance = None
        self.source_instance_identifier = source_instance_identifier
        self.snapshot_identifier = snapshot_identifier
        self.rds_client = boto3.client('rds')

        logger.info("Initialised RDS Snapshot Finder for {0}".format(dbms))

    def get_snapshot_identifier(self):
        if self.snapshot_identifier is None:
            logger.info("Discovering snapshot identifier...")

            source_instance_id = self.get_source_instance_identifier()
            response = self.rds_client.describe_db_snapshots(
                DBInstanceIdentifier=source_instance_id,
            )
            # TODO check for error

            if len(response['DBSnapshots']) == 0:
                raise Exception("No snapshots found")

            logger.debug(
                "Found %d snapshots for %s",
                len(response['DBSnapshots']),
                source_instance_id,
            )

            response['DBSnapshots'].sort(
                key=lambda x: x.get('SnapshotCreateTime', 0),
                reverse=True
            )
            most_recent = response['DBSnapshots'][0]
            self.snapshot_identifier = most_recent['DBSnapshotIdentifier']

            logger.info("Using snapshot %s", self.snapshot_identifier)

        return self.snapshot_identifier

    def get_source_instance_identifier(self):
        if self.source_instance_identifier is None:
            logger.info("Discovering source RDS instance identifier...")

            i = self.get_source_instance()
            self.source_instance_identifier = i['DBInstanceIdentifier']

            logger.info("Using source RDS instance %s", self.source_instance_identifier)

        return self.source_instance_identifier

    def get_source_instance(self):
        if self.source_instance is None:
            if self.source_instance_identifier is None:
                logger.info("Discovering source RDS instance...")

                rds_instances = self.rds_client.describe_db_instances()
                # TODO: check error

                logger.debug(
                    "Enumerated %d RDS instances",
                    len(rds_instances['DBInstances'])
                )

                for instance in rds_instances['DBInstances']:
                    if 'Endpoint' in instance and instance['Endpoint']['Address'] == self.get_rds_endpoint_address():
                        logger.info(
                            "RDS instance %s matches endpoint address %s:%s",
                            instance['DBInstanceIdentifier'],
                            instance['Endpoint']['Address'],
                            instance['Endpoint']['Port'],
                        )

                        self.source_instance = instance
                        self.source_instance_identifier = instance['DBInstanceIdentifier']
                        break

                if self.source_instance is None:
                    raise Exception("TODO: error text")
            else:
                logger.info("Looking up RDS instance %s ...", self.source_instance_identifier)
                rds_instances = self.rds_client.describe_db_instances(
                    DBInstanceIdentifier=self.source_instance_identifier
                )
                if len(rds_instances['DBInstances']) == 0:
                    raise Exception("TODO: error text")
                else:
                    self.source_instance = rds_instances['DBInstances'][0]

        return self.source_instance

    def get_hostname(self):
        if self.hostname is None:
            logger.info(
                "Using hostname default for %s: '%s'",
                self.dbms,
                self.hostname_defaults[self.dbms],
            )
            self.hostname = self.hostname_defaults[self.dbms]

        return self.hostname

    def get_rds_endpoint_address(self):
        if self.rds_endpoint_address is None:
            logger.info("Discovering RDS endpoint address via DNS...")

            try:
                resolver = dns.resolver.Resolver()
                resolution = resolver.query(self.get_hostname())
                cname = resolution.canonical_name
            except dns.resolver.NXDOMAIN:
                raise Exception("TODO: error text")

            if not cname.is_subdomain(self.rds_domain):
                raise Exception("TODO: error text 2")

            self.rds_endpoint_address = cname.to_text().rstrip('.')
            logger.info(
                "Resolved RDS endpoint address of %s to %s",
                self.get_hostname(),
                self.rds_endpoint_address,
            )

        return self.rds_endpoint_address
