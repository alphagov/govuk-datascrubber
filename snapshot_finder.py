import boto3
import dns.resolver
import dns.name
import logging

logger = logging.getLogger(__name__)


class RdsSnapshotFinder:
    hostname_defaults = {
        'mysql': 'mysql-primary',
        'postgresql': 'postgresql-primary'
    }
    rds_domain = dns.name.from_text('rds.amazonaws.com.')

    def __init__(self, dbms, hostname=None, source_instance_identifier=None, snapshot_identifier=None):
        if dbms not in self.hostname_defaults.keys():
            raise Exception('dbms must be one of {0}'.format(', '.join(self.hostname_defaults.keys())))

        self.dbms = dbms
        self.hostname = hostname
        self.rds_endpoint_address = None
        self.source_instance = None
        self.source_instance_identifier = source_instance_identifier
        self.snapshot_identifier = snapshot_identifier
        self.rds_client = boto3.client('rds')

        logger.info("Initialised RDS Snapshot Finder")

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
                key=lambda x: x['SnapshotCreateTime'],
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
