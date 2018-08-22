import logging
import os.path


# SQL copied over from github.com/alphagov/whitehall/script/scrub-database

def scrub_whitehall(cursor):
    logger = logging.getLogger('scrub_whitehall')

    lorem_ipsum_line = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit'
    lorem_ipsum_slug = 'lorem-ipsum-dolor-sit-amet-elit'
    lorem_ipsum_paragraphs = (
        'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vestibulum '
        'eget metus leo. Integer ac gravida magna. Vestibulum adipiscing '
        'pretium vehicula. Praesent ultrices eros a mi elementum id ultrices '
        'ligula ornare. Vivamus mollis, odio id luctus scelerisque, dui nunc '
        'semper felis, vitae fermentum tortor ante eget erat. Maecenas '
        'eleifend elit nec libero porttitor sodales. Quisque vitae augue ut '
        'justo vulputate tincidunt at pellentesque tortor.\n\n'

        'In bibendum urna sed sem egestas aliquam tempor leo dictum. Lorem '
        'ipsum dolor sit amet, consectetur adipiscing elit. Donec rhoncus '
        'adipiscing ultrices. Morbi gravida, lacus vitae adipiscing tincidunt, '
        'quam tellus consectetur leo, et posuere tortor metus a nulla. Aliquam '
        'erat volutpat. In et ante diam. Nulla laoreet ante ut sem egestas sed '
        'placerat elit viverra. Duis tempor congue est, rutrum mattis neque '
        'aliquam non. Nunc a massa quis nisl blandit elementum a in elit. '
        'Pellentesque sollicitudin, magna nec viverra consectetur, risus ante '
        'bibendum diam, vel volutpat risus neque sed risus. Nullam leo enim, '
        'faucibus eu consequat facilisis, auctor eget velit. In ultricies '
        'lectus in velit commodo tempus. Fusce luctus condimentum mi, eleifend '
        'auctor libero volutpat sed. Quisque tempor viverra mauris, non '
        'blandit ipsum vulputate viverra. In sed enim nibh, eu auctor urna. '
        'Suspendisse potenti.\n\n'

        'Proin elementum varius quam, eu fermentum nulla vestibulum sed. '
        'Integer urna turpis, malesuada sed vehicula vel, vestibulum gravida '
        'purus. Vivamus adipiscing ullamcorper bibendum. Nunc pretium '
        'condimentum nisi, sit amet blandit augue accumsan in. Ut in erat '
        'urna, eget elementum dui. Nam arcu enim, iaculis at interdum at, '
        'viverra non massa. Sed nisl massa, pulvinar in blandit nec, '
        'pretium eleifend quam. Nullam a nisi dolor, ornare sagittis felis. '
        'Aliquam laoreet sodales leo sit amet rutrum.\n\n'

        'Fusce dui ante, ornare a interdum vel, posuere non ipsum. Morbi '
        'placerat est ac quam ultrices eget feugiat tortor rhoncus. Duis '
        'tempor placerat leo sit amet volutpat. Curabitur dignissim pulvinar '
        'sem, non auctor dolor mattis sed. In volutpat volutpat massa quis '
        'convallis. In in cursus tortor. Pellentesque massa sem, rhoncus a '
        'iaculis ac, tincidunt sit amet nibh.\n\n'

        'Etiam eu orci sed massa porttitor volutpat. Maecenas euismod lobortis '
        'risus sed vehicula. Proin luctus fringilla odio, in ullamcorper eros '
        'suscipit ac. Ut consequat vehicula urna nec posuere. Donec vel '
        'dapibus massa. Pellentesque consectetur odio a mauris semper '
        'bibendum. In vitae sem sollicitudin est egestas gravida id non urna.'
    )

    logger.info('Anonymising latest access limited drafts...')
    sql = (
        'UPDATE edition_translations '
        'SET title = %s, summary = %s, body = %s '
        'WHERE edition_id IN ('
        '    SELECT id FROM editions WHERE access_limited = 1 '
        ')'
    )
    logger.debug(sql)
    cursor.execute(
        sql,
        params=(lorem_ipsum_line, lorem_ipsum_line, lorem_ipsum_paragraphs)
    )
    logger.info('Rows affected: %d', cursor.rowcount)

    logger.info('Anonymising slugs for latest access limited drafts...')
    sql = (
        'UPDATE documents '
        'SET slug = CONCAT(%s, id) '
        'WHERE id IN ('
        '    SELECT document_id FROM editions WHERE access_limited = 1'
        ')'
    )
    logger.debug(sql)
    cursor.execute(sql, params=(lorem_ipsum_slug,))
    logger.info('Rows affected: %d', cursor.rowcount)

    logger.info('Anonymising email addresses and comments in fact checks...')
    sql = (
        "UPDATE fact_check_requests SET "
        "  email_address = CONCAT('fact-email-', id, '@example.com'), "
        "  comments = '', "
        "  instructions = '', "
        "  `key` = CONCAT('redacted-', id) "
    )
    logger.debug(sql)
    cursor.execute(sql)
    logger.info('Rows affected: %d', cursor.rowcount)

    logger.info('Anonymising attachment titles for latest access limited drafts...')
    sql = (
        "UPDATE attachments SET title=%s WHERE"
        "  attachable_type = 'Edition' AND"
        "  attachable_id IN ("
        "    SELECT id FROM editions WHERE access_limited = 1"
        "  )"
    )
    logger.debug(sql)
    cursor.execute(sql, params=(lorem_ipsum_line,))
    logger.info('Rows affected: %d', cursor.rowcount)

    logger.info('Anonymising HTML attachment data for latest access limited drafts...')
    sql = (
        "UPDATE attachments"
        "  SET slug = CONCAT(%s, '-', id)"
        "  WHERE"
        "    attachable_type = 'Edition' AND"
        "    attachable_id IN ("
        "      SELECT id FROM editions WHERE access_limited = 1"
        "    )"
    )
    logger.debug(sql)
    cursor.execute(sql, params=(lorem_ipsum_slug,))
    logger.info('Rows affected: %d', cursor.rowcount)

    logger.info('Anonymising file names for attachments on latest access limited drafts...')
    sql = (
        "UPDATE attachment_data"
        "  SET carrierwave_file='redacted.pdf'"
        "  WHERE id IN ("
        "    SELECT attachment_data_id FROM attachments WHERE"
        "      attachments.attachable_type = 'Edition' AND"
        "      attachments.attachable_id IN ("
        "        SELECT id FROM editions WHERE access_limited = 1"
        "      )"
        "  )"
    )
    logger.debug(sql)
    cursor.execute(sql)
    logger.info('Rows affected: %d', cursor.rowcount)

    logger.info('Anonymising govspeak content data for access limited editions...')
    sql = (
        "UPDATE"
        "   govspeak_contents"
        "   JOIN attachments ON attachments.id = govspeak_contents.html_attachment_id"
        "   JOIN editions ON attachments.attachable_id = editions.id "
        "SET"
        "   govspeak_contents.body=%s,"
        "   govspeak_contents.computed_body_html=NULL,"
        "   govspeak_contents.computed_headers_html=NULL "
        "WHERE"
        "   attachments.attachable_type = 'EDITION' AND"
        "   editions.access_limited = 1"
    )
    logger.debug(sql)
    cursor.execute(sql, params=(lorem_ipsum_paragraphs,))
    logger.info('Rows affected: %d', cursor.rowcount)


def scrub_email_alert_api(cursor):
    logger = logging.getLogger('scrub_email_alert_api')
    sql_script = os.path.join(
        os.path.dirname(__file__),
        'sql', 'scrub_email_alert_api.sql'
    )
    logger.info("Loading SQL from %s", sql_script)
    sql = open(sql_script, 'r').read()
    logger.info("Executing SQL (%d lines) ...", len(sql.split('\n')))
    cursor.execute(sql)


def scrub_publishing_api(cursor):
    logger = logging.getLogger('scrub_publishing_api')
    sql_script = os.path.join(
        os.path.dirname(__file__),
        'sql', 'scrub_publishing_api.sql'
    )
    logger.info("Loading SQL from %s", sql_script)
    sql = open(sql_script, 'r').read()
    logger.info("Executing SQL (%d lines) ...", len(sql.split('\n')))
    cursor.execute(sql)
