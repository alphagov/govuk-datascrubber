import boto3
import random
import time
from datetime import datetime


class ScrubWorkspaceInstance:
    def __init__(self, snapshot_finder):
        self.rds_client = boto3.client('rds')
        self.snapshot_finder = snapshot_finder
        self.instance_identifier = "scrubber-workspace-{0}".format(datetime.now().strftime("%Y%m%d%H%M%S"))
        self.password = "{0:x}".format(random.getrandbits(64 * 4))
        self.instance = None
        self.source_instance = self.snapshot_finder.get_source_instance()

    def get_endpoint(self):
        i = self.get_instance()
        return i['Endpoint']

    def get_instance(self):
        if self.instance is None:
            self.__create_instance()

        return self.instance

    def cleanup(self):
        if self.instance is not None:
            rds = self.rds_client
            rds.delete_db_instance(
                DBInstanceIdentifier=self.instance_identifier(),
                FinalDBSnapshotIdentifier=self.final_snapshot_identifier(),
            )

    # TODO: Return a DSN suitable for configuring the MySQL or Postgres connector lib
    def get_dsn(self):
        return "todo"

    def __create_instance(self):
        rds = self.rds_client

        restore_response = rds.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier=self.instance_identifier,
            DBSnapshotIdentifier=self.snapshot_finder.get_snapshot_identifier(),
            DBSubnetGroupName=self.source_instance['DBSubnetGroup']['DBSubnetGroupName'],
        )
        # TODO check for error

        for i in range(120):
            poll_response = rds.describe_db_instances(
                DBInstanceIdentifier=self.instance_identifier
            )
            # TODO check for error
            self.instance = poll_response['DBInstances'][0]
            print("Waiting for instance to become available, current status == '{0}'".format(self.instance['DBInstanceStatus']))
            if self.instance['DBInstanceStatus'] == 'available':
                return
            else:
                time.sleep(2)

        raise Exception("Timeout creating instance, instance status: '{0}'".format(self.instance['DBInstanceStatus']))
