import logging


def scrub_email_alert_api(cursor):
    logger = logging.getLogger('scrub_email_alert_api')

    logger.info("Deleting all emails that are older than 1 day old...")
    sql = (
        "DELETE FROM emails "
        "WHERE created_at < current_timestamp - interval '1 day'"
    )
    logger.debug(sql)
    cursor.execute(sql)

    logger.info("Creating a table to store all email addresses...")
    sql = 'CREATE TABLE addresses (id SERIAL, address VARCHAR NOT NULL)'
    logger.debug(sql)
    cursor.execute(sql)

    logger.info("Copying all email addresses into the table, ignoring nulled out subscriber addresses")
    sql = (
        'INSERT INTO addresses (address) '
        'SELECT address FROM subscribers WHERE address IS NOT NULL '
        'UNION DISTINCT '
        'SELECT address FROM emails'
    )
    logging.debug(sql)
    cursor.execute(sql)

    logger.info("Indexing the table so we can efficiently lookup addresses...")
    sql = "CREATE UNIQUE INDEX addresses_index ON addresses (address)"
    logging.debug(sql)
    cursor.execute(sql)

    logger.info("Setting subscribers.address from the auto-incremented id in addresses table...")
    sql = (
        "UPDATE subscribers s "
        "SET address = CONCAT('anonymous-', a.id, '@example.com') "
        "FROM addresses a "
        "WHERE a.address = s.address"
    )
    logging.debug(sql)
    cursor.execute(sql)

    logger.info("Setting emails.address from the auto-incremented id in addresses table...")
    sql = (
        "UPDATE emails e "
        "SET address = CONCAT('anonymous-', a.id, '@example.com'), "
        "subject = REPLACE(e.subject, e.address, CONCAT('anonymous-', a.id, '@example.com')), "
        "body = REPLACE(e.body, e.address, CONCAT('anonymous-', a.id, '@example.com')) "
        "FROM addresses a "
        "WHERE a.address = e.address"
    )
    logging.debug(sql)
    cursor.execute(sql)

    logger.info("Cleaning up - deleting the addresses table and its index...")
    sql = "DROP INDEX addresses_index"
    logging.debug(sql)
    cursor.execute(sql)
    sql = "DROP TABLE addresses"
    logging.debug(sql)
    cursor.execute(sql)
