import logging
import os.path


def scrub_publishing_api(cursor):
    logger = logging.getLogger('scrub_publishing_api')
    sql_script = os.path.join(
        os.path.dirname(__file__),
        '..', 'sql', 'scrub_publishing_api.sql'
    )
    logger.info("Loading SQL from %s", sql_script)
    sql = open(sql_script, 'r').read()
    logger.info("Executing SQL (%d lines) ...", len(sql.split('\n')))
    cursor.execute(sql)
