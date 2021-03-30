import mysql.connector
from mysql.connector import Error
from urllib.parse import unquote_plus
import boto3
import rds_config
import botocore

TEMP_FILE_CONTENT = '/tmp/loadGeneralFile.csv'
s3 = boto3.resource('s3')


def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        table_parse = key.split('/')
        load_data_file(table_parse[1], bucket, key)


def load_data_file(table, bucket, key):
    try:
        print("DESCARGANDO ARCHIVO ...")
        s3.Bucket(bucket).download_file(key, TEMP_FILE_CONTENT)
        print("ESTABLECIENDO CONEXION CON LA BBDD ...")
        try:
            conn = mysql.connector.connect(
                host=rds_config.db_host,
                database=rds_config.db_name,
                user=rds_config.db_username,
                password=rds_config.db_password,
                allow_local_infile=True
            )
            cursor = conn.cursor()
            print("CARGANDO ARCHIVO " + TEMP_FILE_CONTENT + " EN TABLA " + table + "...")
            cursor.execute('''
            LOAD DATA LOCAL INFILE '{filename}' 
            IGNORE INTO TABLE {table}
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            IGNORE 2 LINES;
            '''.format(**{'filename': TEMP_FILE_CONTENT, 'table': table}))
            conn.commit()
            print("LISTO ...")
        except Error as e:
            print("Error reading data from MySQL table", e)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise