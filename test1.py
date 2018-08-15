# Temporary script for assisting in developing the classes
# Starts an interactive Python shell at the end.

import datascrubber
import datascrubber.mysql
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
mysql_sf = datascrubber.RdsSnapshotFinder('mysql')
postgresql_sf = datascrubber.RdsSnapshotFinder('postgresql')

# Request a scrub workspace instance and exercise the code by calling get_endpoint().
mysql_swi = datascrubber.ScrubWorkspaceInstance(mysql_sf)
endpoint = mysql_swi.get_endpoint()
mysql_scrubber = datascrubber.mysql.MysqlScrubber(mysql_swi)
tasks = mysql_scrubber.get_viable_tasks()
for task in tasks:
    mysql_scrubber.run_task(task)

# Launch an IPython shell so we can inspect the program state if we want.
IPython.embed()
