import sys
import re
import numpy as np
import pandas as pd
import sqlite3
import psycopg2

def get_sqlite_data(tbl):
    db_file = f"/media/sunveil/Data/header_detection/poddubnyy/postgraduate/squall/tables/db/{tbl}.db"
    conn = sqlite3.connect(db_file)
    df = pd.read_sql_query("SELECT * FROM w", conn)
    del df['id']
    return df

def get_psql_type(type_):
    if type_ == int:
        return 'INTEGER'

    elif type_ == float:
        
        return 'REAL'

    else:
        return 'TEXT'
        
def get_query_execution_plan(table,sql):
    host = '192.168.19.148'
    database = 'tapex'
    user = 'postgres'
    password = '0000'
    port = 5432
    answer = None
    try:
        tab_name = 'w'
        explain_sql = 'EXPLAIN (ANALYZE,FORMAT XML)' + sql
        create_table_query = f'CREATE TABLE IF NOT EXISTS {tab_name} (id SERIAL PRIMARY KEY,'\
                            f'{",".join([f"{key} {get_psql_type(table.dtypes[key])}" for key in table.columns])});'
        #insert_data_query = f'INSERT INTO {tab_name} ({",".join([key for key in data.columns if key !="id"])}) VALUES '\
                            #f'{",".join(["%s" for _,x in data.iterrows()])};'
        insert_data_query = f'INSERT INTO {tab_name} ({",".join([key for key in table.columns])}) VALUES '\
                            f'({",".join(["%s" for key in table.columns ])});'
        drop_table_query = f'DROP TABLE IF EXISTS {tab_name};'
        # Подключение к базе данных
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        cursor = connection.cursor()
        # Получение информации о версии postgres
        
        
        cursor.execute(create_table_query)

def get_sqall_execution_plan(squall_example):
    table = get_sqlite_data(squall_example['tbl'])
    sql = ' '.join([s[1] for s in squall_example['sql']])
    answer = get_query_execution_plan(table,sql)
    return answer[0][0] if answer != None else answer
        
        
    
        
    
        # Используем executemany для вставки множества строк
    
        cursor.executemany(insert_data_query, [x.to_list() for _,x in table.iterrows()])  
        cursor.execute(explain_sql)
        answer = cursor.fetchall()
        cursor.execute(drop_table_query)
        connection.commit()
        #version = cursor.fetchone()[0]
        #print(f"Версия PostgreSQL: {version}")
    except Exception as e:
        print("Ошибка при работе с БД:", e)
    finally:
        if cursor:
            cursor.close()  # Закрытие курсора
        if connection:
            connection.close()  # Закрытие соединения
        return answer    