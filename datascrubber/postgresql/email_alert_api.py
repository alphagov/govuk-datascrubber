import logging
import os.path


def scrub_email_alert_api(cursor):
    logger = logging.getLogger('scrub_email_alert_api')
    sql_script = os.path.join(
        os.path.dirname(__file__),
        'scrub_email_alert_api.sql'
    )
    logger.info("Loading SQL from %s", sql_script)
    sql = open(sql_script, 'r').read()
    logger.info("Executing SQL (%d lines) ...", len(sql.split('\n')))
    cursor.execute(sql)
