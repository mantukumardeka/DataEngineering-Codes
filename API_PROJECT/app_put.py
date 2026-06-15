from fastapi import FastAPI
import pymysql

app = FastAPI()

@app.put("/employee/{emp_id}")
def update_employee(emp_id: int, emp: dict):

    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="Hadoop@123",
        database="mkd"
    )

    cur = conn.cursor()

    cur.execute("""
        UPDATE employee
        SET
            name=%s,
            salary=%s,
            loc=%s
        WHERE emp_id=%s
    """,
    (
        emp["name"],
        emp["salary"],
        emp["loc"],
        emp_id
    ))

    conn.commit()
    conn.close()

    return {"message": "Updated Successfully"}