# Temporary script for assisting in developing the classes
# Starts an interactive Python shell at the end.

import datascrubber
import logging
import IPython
import mysql.connector
import re

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

logging.info("Got a new endpoint: %s", endpoint)

mysql_cnx = mysql.connector.connect(
    user='aws_db_admin',
    password=mysql_swi.password,
    host=endpoint['Address'],
    port=endpoint['Port'],
    database='information_schema'
)

cursor = mysql_cnx.cursor()
cursor.execute("SELECT DISTINCT(table_schema) FROM TABLES WHERE table_schema NOT IN ('information_schema', 'innodb', 'mysql', 'performance_schema', 'sys', 'tmp');")
rows = cursor.fetchall()
database_names = [r[0] for r in rows]
logging.info("Database names: %s", database_names)
normalised_db_names = [re.sub(r'_production$', '', db) for db in database_names]
logging.info("Normalised database names: %s", normalised_db_names)

# Launch an IPython shell so we can inspect the program state if we want.
IPython.embed()
