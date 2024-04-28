import sqlite3

con = sqlite3.connect("student.db")

try:
    cur = con.cursor()

    table_info = """
        create table if not exists student(
            name varchar(25),
            class varchar(25),
            section varchar(25)
        )
    """
    cur.execute(table_info)

    student_data = [
        ("A", "Data Science", "A"),
        ("B", "Data Science", "B"),
        ("C", "Data Science", "A"),
        ("D", "DevOps", "D"),
        ("E", "DevOps", "C"),
    ]
    cur.executemany("insert into student values(?, ?, ?)", student_data)
    con.commit()

    query_info = """
        select * from student
    """
    res = cur.execute(query_info)
    data = res.fetchall()
    for row in data:
        print(row)

except sqlite3.Error as e:
    print("An error occurred:", e)

finally:
    cur.close()
    con.close()
