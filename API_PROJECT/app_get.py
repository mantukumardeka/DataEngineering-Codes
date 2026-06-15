from fastapi import FastAPI
import pymysql

app = FastAPI()

@app.get("/employees")
def get_employee():

    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="Hadoop@123",
        database="mkd"
    )

    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM employee
    """)

    rows = cur.fetchall()

    data = []

    for row in rows:
        data.append({
            "id": row[0],
            "name": row[1],
            "salary": float(row[2]),
            "loc": row[3]

        })

    conn.close()

    return data