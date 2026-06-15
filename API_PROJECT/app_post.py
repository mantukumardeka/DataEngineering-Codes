from fastapi import FastAPI
import pymysql

app = FastAPI()

@app.post("/employee")
def add_employee(emp: dict):

    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="Hadoop@123",
        database="mkd"
    )

    cur = conn.cursor()

    cur.execute("""
        INSERT INTO employee
        (
            emp_id,
            name,
            salary,
            loc
        )
        VALUES
        (
            %s,
            %s,
            %s,
            %s
        )
    """,
    (
        emp["emp_id"],
        emp["name"],
        emp["salary"],
        emp["loc"]
    ))

    conn.commit()

    conn.close()

    return {
        "message":"Employee Inserted Successfully"
    }