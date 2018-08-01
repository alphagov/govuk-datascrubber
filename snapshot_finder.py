import boto3
import dns.resolver
import dns.name


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

    def get_snapshot_identifier(self):
        if self.snapshot_identifier is None:
            response = self.rds_client.describe_db_snapshots(
                DBInstanceIdentifier=self.get_source_instance_identifier()
            )

            if len(response['DBSnapshots']) == 0:
                raise Exception("No snapshots found")

            response['DBSnapshots'].sort(
                key=lambda x: x['SnapshotCreateTime'],
                reverse=True
            )
            most_recent = response['DBSnapshots'][0]
            self.snapshot_identifier = most_recent['DBSnapshotIdentifier']

        return self.snapshot_identifier

    def get_source_instance_identifier(self):
        if self.source_instance_identifier is None:
            i = self.get_source_instance()
            self.source_instance_identifier = i['DBInstanceIdentifier']

        return self.source_instance_identifier

    def get_source_instance(self):
        if self.source_instance is None:
            if self.source_instance_identifier is None:
                rds_instances = self.rds_client.describe_db_instances()

                for instance in rds_instances['DBInstances']:
                    if instance['Endpoint']['Address'] == self.get_rds_endpoint_address():
                        self.source_instance = instance
                        self.source_instance_identifier = instance['DBInstanceIdentifier']
                        break

                if self.source_instance is None:
                    raise Exception("TODO: error text")
            else:
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
            self.hostname = self.hostname_defaults[self.dbms]

        return self.hostname

    def get_rds_endpoint_address(self):
        if self.rds_endpoint_address is None:
            try:
                resolver = dns.resolver.Resolver()
                resolution = resolver.query(self.get_hostname())
                cname = resolution.canonical_name
            except dns.resolver.NXDOMAIN:
                raise Exception("TODO: error text")

            if not cname.is_subdomain(self.rds_domain):
                raise Exception("TODO: error text 2")

            self.rds_endpoint_address = cname.to_text().rstrip('.')

        return self.rds_endpoint_address
