raw_tb_create = ("""CREATE TABLE IF NOT EXISTS predictraw(
                id SERIAL CONSTRAINT predictraw_pk PRIMARY KEY,
                file_name VARCHAR(255),
                file_contents TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_on DATE DEFAULT CURRENT_DATE
                )
                 """)

raw_tb_insert = ("""INSERT INTO predictraw (file_name, file_contents, created_at, created_on) VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_DATE)""")

table_create_l = [raw_tb_create]