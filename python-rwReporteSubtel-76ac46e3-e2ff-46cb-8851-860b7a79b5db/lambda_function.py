import json
import boto3
import sys
import logging
import rds_config
import pymysql
import csv
#Credenciales RDS
rds_host  = "xxxxxxxxxxxxx.xxxxxxxxxxxxx.amazonaws.com"
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name
# Instanciando BOTO3 (S3) para AWS SDK PHYTON
s3 = boto3.client('s3')
bucket = "xxxxxxxxxxxxx"
# Seteando nivel de debugg
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Intentando conexion a DB
try:
	conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except:
	logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
	sys.exit()
	logger.info("SUCCESS: Connection to RDS mysql instance succeeded")

#Funcion principal
def lambda_handler(event, context):
# Parametros recibidos
	dateFrom = event["values"]["from"]
	dateTo = event["values"]["to"]
	# Iniciando consulta
	try:
		query = "SELECT * FROM xxxxxxxxxxxxx WHERE Fecha BETWEEN '"+dateFrom+"' AND '"+dateTo+"';"
		cur = conn.cursor()
		# Ejecutando query y recuperando resultado
		cur.execute(query)
		rows = cur.fetchall()
		# Seteando nombres de columnas
		column_names = [i[0] for i in cur.description]
		# Abriendo archivo en temp para escribir
		fp = open('/tmp/temp.csv', 'w')
		myFile = csv.writer(fp, lineterminator = '\n')
		# Agregando columnas
		myFile.writerow(column_names)
		# Agregando data
		myFile.writerows(rows)
		fp.close()
		# Subiendo archivo a S3
		key = "xxxxxxxxxxxxx/xxxxxxxxxxxxx"+"_"+dateFrom+"-"+dateTo+".csv"
		s3.upload_file(Filename='/tmp/temp.csv', Bucket=bucket, Key=key)
	except pymysql.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))

	conn.commit()