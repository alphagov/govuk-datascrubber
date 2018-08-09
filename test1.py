# Temporary script for assisting in developing the classes
# Starts an interactive Python shell at the end.

from scrub_workspace_instance import ScrubWorkspaceInstance
from snapshot_finder import RdsSnapshotFinder

import socket
import logging
import IPython

# Logs to stderr with the given format string, excluding DEBUG messages.
# The great thing about using this logging library is that we automatically
# get the logs from boto too.

# If we want to log to syslog or CloudWatch we can just add another logging
# config to do that.
logging.basicConfig(
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    level=logging.INFO,
)

# Create snapshot finders for both mysql and postgresql
mysql_sf = RdsSnapshotFinder('mysql')
postgresql_sf = RdsSnapshotFinder('postgresql')

# Request a scrub workspace instance and exercise the code by calling get_endpoint().
mysql_swi = ScrubWorkspaceInstance(mysql_sf)
endpoint = mysql_swi.get_endpoint()

logging.info("Got a new endpoint", endpoint)

try:
    _ = socket.create_connection((endpoint['Address'], endpoint['Port']))
except ConnectionRefusedError:
    logging.error("Connection refused")
except TimeoutError:
    logging.error("Timed out")

# Launch an IPython shell so we can inspect the program state if we want.
IPython.embed()
