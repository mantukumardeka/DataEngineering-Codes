from fastapi import FastAPI
import pymysql

app = FastAPI()

@app.delete("/employee/{emp_id}")
def delete_employee(emp_id: int):

    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="Hadoop@123",
        database="mkd"
    )

    cur = conn.cursor()

    cur.execute(
        "DELETE FROM employee WHERE emp_id = %s",
        (emp_id,)
    )

    conn.commit()
    conn.close()

    return {"message": "Employee deleted successfully"}