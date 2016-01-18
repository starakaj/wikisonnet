import mysql.connector
import db.dbconnect as dbconnect

def putLaudForPoemAndSession(dbconfig, poem_id, session):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """INSERT IGNORE INTO lauds (session, poem_id) VALUES (%s, %s);"""
    values = (session, poem_id)
    try:
        cursor.execute(query, values)
    except mysql.connector.IntegrityError as e:
        conn.close()
        return 0
    affected_rows = cursor.rowcount
    cursor.execute("""COMMIT;""")
    conn.close()
    return 1 if affected_rows > 0 else 0

def deleteLaudForPoemAndSession(dbconfig, poem_id, session):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor(dictionary=True)
    query = """DELETE FROM lauds WHERE session=%s AND poem_id=%s;"""
    values = (session, poem_id)
    cursor.execute(query, values)
    affected_rows = cursor.rowcount
    cursor.execute("""COMMIT;""")
    conn.close()
    return 1 if affected_rows > 0 else 0
