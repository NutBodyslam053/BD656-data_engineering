import streamlit as st
import os
import sqlite3
import google.generativeai as genai

from dotenv import load_dotenv

load_dotenv()

genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))


def get_gemini_response(question, prompt):
    model = genai.GenerativeModel("gemini-pro")
    response = model.generate_content([question, prompt[0]])
    return response.text


def read_sql_query(sql, db):
    con = sqlite3.connect(db)
    cur = con.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        print(row)
    cur.close()
    con.close()
    return rows


prompt = [
    """
    You are an expert in converting English questions into SQL queries. 
    The SQL database named `student` and has the following columns: name, class, section 
    \nFor example: \nExample 1 - How many entries of records are present?, 
    the SQL command will be something like `select count(*) from student;` 
    \nExample 2 - Tell me all the students studying in Data Science class?, 
    the SQL command will be something like `select * from student where class = "Data Science";` 
    \nAlso the SQL code should not have '```' at the beginning or end of the SQL keyword in the output.
    Always convert SQL query keywords into lowercase and use the LOWER function for each column
    when using the WHERE clause in an SQL statement.
    """
]

st.set_page_config(page_title="I can retrieve any SQL statements")
st.header("Gemini App to retrieve SQL data")

question = st.text_input("Input: ", key="input")

submit = st.button("Ask the question...")

if submit:
    sql = get_gemini_response(question=question, prompt=prompt)
    print("sql: ", sql)
    response = read_sql_query(sql=sql, db="student.db")
    st.header("The answer is:")
    for row in response:
        st.header(row)