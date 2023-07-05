import select
import psycopg2.extensions


def psycopg2_keyboard_interrupt():
    """
    the function used to have keyboard interrupt the postgresql
    using way:
    running it before you use psycopg2 query
    sample:
    initialize_postgresql_keyboard_interrupt()
    ...
    conn = psycopg2.connect()
    cursor = conn.cursor()
    cursor.execute(query)

    # Fetch all the rows
    rows = cursor.fetchall()

    ^C
    Traceback (most recent call last):
    cursor.execute(query)
    psycopg2.errors.QueryCanceled: canceling statement due to user request

    https://www.psycopg.org/articles/2014/07/20/cancelling-postgresql-statements-python/
    """

    def _lambda_interrupter(conn):
        while 1:
            try:
                state = conn.poll()
                if state == psycopg2.extensions.POLL_OK:
                    break
                elif state == psycopg2.extensions.POLL_READ:
                    select.select([conn.fileno()], [], [])
                elif state == psycopg2.extensions.POLL_WRITE:
                    select.select([], [conn.fileno()], [])
                else:
                    raise conn.OperationalError(
                        "bad state from poll: %s" % state)
            except KeyboardInterrupt:
                conn.cancel()
                # the loop will be broken by a server error
                continue

    psycopg2.extensions.set_wait_callback(_lambda_interrupter)
