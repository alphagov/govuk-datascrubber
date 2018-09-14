import logging


def scrub_publishing_api(cursor):
    logger = logging.getLogger('scrub_publishing_api')

    sql = (
        "UPDATE events SET payload = NULL "
        "WHERE action = 'PutContent' "
        "AND content_id IN ("
        "  SELECT content_id"
        "  FROM documents"
        "  INNER JOIN editions ON (documents.id = editions.document_id)"
        "  INNER JOIN access_limits ON (editions.id = access_limits.edition_id)"
        ")"
    )
    logger.info(sql)
    cursor.execute(sql)

    sql = (
        'DELETE FROM change_notes WHERE edition_id IN ('
        '    SELECT edition_id'
        '    FROM access_limits'
        ')'
    )
    logger.info(sql)
    cursor.execute(sql)

    sql = (
        'DELETE FROM editions WHERE id IN ('
        '    SELECT edition_id'
        '    FROM access_limits'
        ')'
    )
    logger.info(sql)
    cursor.execute(sql)
