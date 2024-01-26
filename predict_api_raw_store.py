import psycopg2
import boto3
import configparser
from create_tables import raw_tb_insert, table_create_l

def read_config(file_path=".config"):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

def connect_db(password,user='postgres',dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def create_db(dbname,password):
    con = connect_db(password=password,dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))
    else:
        print("A database named {0} already exists".format(dbname))
    
    cur.close()
    con.commit()
    con.close()

def read_config(file_path=".config"):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

def create_tables(con):
    cur = con.cursor()
    for tables in table_create_l:
        cur.execute(tables)
    cur.close()
    con.commit()


def fetch_from_s3_bucket():
    config = read_config()
    aws_access_key = str(config.get('AWS','AWS_ACCESS_KEY_ID'))
    aws_secret_key = str(config.get('AWS','AWS_SECRET_ACCESS_KEY'))
    aws_region = str(config.get('AWS','AWS_REGION'))
    aws_bucket = str(config.get('AWS','BUCKET_NAME'))
    db_pwd = str(config.get('DB','POSTGRES_PWD'))
    db_name = str(config.get('DB','POSTGRES_DB'))
    s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key,
                  aws_secret_access_key=aws_secret_key,
                  region_name=aws_region)
    try:
        response = s3.list_objects_v2(Bucket=aws_bucket)
        create_db(dbname=db_name)
        con = connect_db(dbname=db_name)
        cur = con.cursor()
        create_tables(con)
        for obj in response.get('Contents',[]):
            object_key = obj['Key']
            file_contents = s3.get_object(Bucket=aws_bucket,Key=object_key)['Body'].read().decode('utf-8')
            insert_data = (object_key,file_contents)
            cur.execute(raw_tb_insert,insert_data)
            print(f"File {object_key} inserted into DB")
        cur.close()
        con.commit()
        con.close()
        print("All execution success")
    except Exception as detail:
        print("Error while retriving data from S3 ", detail)


