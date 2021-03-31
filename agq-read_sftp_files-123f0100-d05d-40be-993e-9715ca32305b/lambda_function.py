import json
import boto3
import pandas as pd
import io
import pymysql.cursors
import xlrd
from datetime import datetime
from datetime import time
import os

db = None
db_host = ""
db_user = ""
db_password = ""
db_schema = ""

def init_vars():
    global db
    global db_host
    global db_user
    global db_password
    global db_schema
    if os.environ['environment'] == "QA":
        db_host = "xxxxxxxxx.xxxxxxxxx.xxxxxxxxx.amazonaws.com"
        db_user = "xxxxxxxxx"
        db_password = "xxxxxxxxx!"
        db_schema = "xxxxxxxxx"
    elif os.environ['environment'] == "PROD":
        db_host = "xxxxxxxxx.amazonaws.com"
        db_user = "xxxxxxxxx"
        db_password = "xxxxxxxxx"
        db_schema = "xxxxxxxxx"

def save_data(data):
    db = pymysql.connect(
                            host=db_host,
                            user=db_user,
                            password=db_password,
                            db=db_schema,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor
                        )
    try:
        with db.cursor() as cursor:
            sql = """INSERT INTO xxxxxxxxx (codigo_estacion, codigo_muestra, fecha_hora_muestreo, matriz_muestra, codigo_laboratorio, id_muestra_laboratorio, tipo_muestra, metodo_anl_laboratorio, cas_rn, nombre_quimico, indicador_deteccion, valor_resultado, unidad_resultado, fecha_analisis, fecha_recibir, limite_de_deteccion_metodo, total_disuelto, codigo_muestra_madre, numero_de_hoja_custodia, comentarios, file_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.executemany(sql, data)
        db.commit()
    except Exception as e:
        print("Error:", e.__class__)
    finally:
        db.close()

def get_file_id(event):
    file_string = "%"+str(event["Records"][0]["s3"]["object"]["key"])
    db = pymysql.connect(
                            host=db_host,
                            user=db_user,
                            password=db_password,
                            db=db_schema,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor
                        )
    try:
        with db.cursor() as cursor:
            sql = """ SELECT id FROM xxxxxxxxx WHERE ruta LIKE '"""+file_string+"""' """
            cursor.execute(sql)
            aux_id = cursor.fetchone()
            return aux_id["id"]
        db.commit()
    except Exception as e:
        print("Error:", e.__class__)
    finally:
        db.close()

def update_file_status(file_id):
    db = pymysql.connect(
                            host=db_host,
                            user=db_user,
                            password=db_password,
                            db=db_schema,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor
                        )
    try:
        with db.cursor() as cursor:
            sql = """ UPDATE xxxxxxxxx SET estado = 1, ruta = REPLACE(ruta,'processing','processed') WHERE id = '"""+str(file_id)+"""' """
            cursor.execute(sql)
        db.commit()
    except Exception as e:
        print("Error:", e.__class__)
    finally:
        db.close()

def retrieve_from_s3(event):
    s3 = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    if event:
        s3_records = event["Records"][0]
        bucket_name = str(s3_records["s3"]["bucket"]["name"])
        file_key = str(s3_records["s3"]["object"]["key"])
        local_file_name = '/tmp/aux.xlsx'
        s3_resource.Bucket(bucket_name).download_file(file_key, local_file_name)

        return local_file_name

def move_s3_object(event):
    s3 = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    if event:
        s3_records = event["Records"][0]
        bucket_name = str(s3_records["s3"]["bucket"]["name"])
        file_key = str(s3_records["s3"]["object"]["key"])
        local_file_name = '/tmp/aux.xlsx'
        copy_source = {
            'Bucket': bucket_name,
            'Key': file_key
        }
        new_file_key = file_key.replace('/processing/', '/processed/')
        s3_resource.meta.client.copy(copy_source, bucket_name, new_file_key)
        s3.delete_object(Bucket=bucket_name,Key=file_key,)

def lambda_handler(event, context):
    print(event)

    init_vars()

    file_id = get_file_id(event)
    file_path = retrieve_from_s3(event)
    wb = xlrd.open_workbook(file_path)
    sheet = wb.sheet_by_index(0)
    sheet.cell_value(0, 0)
    rows = sheet.nrows
    if rows > 1:
        values = []
        for i in range(sheet.nrows):
            if i != 0:
                aux = sheet.row_values(i)
                aux[2] =  str(datetime.fromtimestamp((aux[2] - 25569) * 86400))
                aux[13] =  str(datetime.fromtimestamp((aux[13] - 25569) * 86400))
                aux[14] =  str(datetime.fromtimestamp((aux[14] - 25569) * 86400))
                aux.append(str(file_id))
                values.append(aux)
        save_data(values)
        move_s3_object(event)
        update_file_status(file_id)