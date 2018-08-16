# Temporary script for assisting in developing the classes
# Starts an interactive Python shell at the end.

import datascrubber
import datascrubber.mysql
import datascrubber.postgresql
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

mysql = datascrubber.mysql.MysqlScrubber(
    datascrubber.ScrubWorkspaceInstance(
        datascrubber.RdsSnapshotFinder('mysql')
    )
)
postgresql = datascrubber.postgresql.PostgresqlScrubber(
    datascrubber.ScrubWorkspaceInstance(
        datascrubber.RdsSnapshotFinder('postgresql')
    )
)

for task in mysql.get_viable_tasks():
    mysql.run_task(task)

for task in postgresql.get_viable_tasks():
    postgresql.run_task(task)

# Launch an IPython shell so we can inspect the program state if we want.
IPython.embed()
