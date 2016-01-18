import mysql.connector
import db.dbconnect as dbconnect

def createSession(dbconfig):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor()
    query = """INSERT INTO sessions (created_at) VALUES(current_timestamp);"""
    cursor.execute(query)
    cursor.execute("""COMMIT;""");
    query = """SELECT LAST_INSERT_ID();"""
    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()

    return res[0][0]

def addPoemToSession(dbconfig, poem_id, session_id):
    conn = mysql.connector.connect(user=dbconfig['user'],
                                    password=dbconfig['password'],
                                    host=dbconfig['host'],
                                    database=dbconfig['database'])
    cursor = conn.cursor()
    query = """INSERT IGNORE INTO sessions_poems (session_id, poem_id) VALUES(%s, %s);"""
    values=(session_id, poem_id)
    cursor.execute(query, values)
    cursor.execute("""COMMIT;""");
    query = """SELECT LAST_INSERT_ID();"""
    cursor.execute(query)
    res = cursor.fetchall()
    conn.close()

    return res[0][0]
