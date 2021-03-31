import sys
import json
import pymysql.cursors
import boto3
from datetime import datetime, date
import pytz
import os

db = None
db_host = ""
db_user = ""
db_password = ""
db_schema = ""
email_subject_qa = ""

def init_vars():
    global db
    global db_host
    global db_user
    global db_password
    global db_schema
    global email_subject_qa
    if os.environ['environment'] == "xxxxxxxxxxx":
        db_host = "xxxxxxxxxxx.amazonaws.com"
        db_user = "xxxxxxxxxxx"
        db_password = "xxxxxxxxxxx!"
        db_schema = "xxxxxxxxxxx"
        email_subject_qa = "xxxxxxxxxxx - "
    elif os.environ['environment'] == "xxxxxxxxxxx":
        db_host = "xxxxxxxxxxx.amazonaws.com"
        db_user = "xxxxxxxxxxx.amazonaws.com"
        db_password = "xxxxxxxxxxx,."
        db_schema = "xxxxxxxxxxx"

def init_db():
    global db
    try:
        db = pymysql.connect(
                                host=db_host,
                                user=db_user,
                                password=db_password,
                                db=db_schema,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor
                            )
    except Exception as e:
        print("No se pudo conectar a la DB!", "Error:", e)

def end_db():
    global db
    try:
        db.close()
    except Exception as e:
        print("No se pudo cerrar la conexión a la DB!", "Error:", e)




def retrieve_unprocessed_data():
    try:
        with db.cursor() as cursor:
            sql = "SELECT * FROM xxxxxxxxxxx WHERE estado IN (0,2) ORDER BY id ASC;"
            cursor.execute(sql)
            return cursor.fetchall()
    except Exception as e:
        print("Error 5:", e)

def process_data(data):
    global_pass = True
    main_data_iteration = -1
    iteration = -1
    aux_valid_records = 0
    aux_flag_values = 0
    aux_max_alert_value = 0
    errors = {}
    to_insert = {}
    to_insert["reporte_monitoreo"] = {}
    to_insert["reporte_datos_monitoreo"] = {}
    to_insert["alerta_dato_monitoreo"] = {}
    to_insert["archivos_nuevo"] = {}
    to_insert["archivos_nuevo"][0] = {}
    to_insert["archivos_nuevo"][1] = {}
    for reg in data:
        iteration += 1
        errors[reg["id"]] = []
        aux_reg_id = str(reg["id"])

        print("Reg "+aux_reg_id)

        # Procesing "reporte_datos_monitoreo" data
        to_insert["reporte_datos_monitoreo"][iteration] = {}
        # Check and insert "punto_muestreo_id"
        try:
            with db.cursor() as cursor:
                sql__punto_muestreo_id = "SELECT id_punto_muestreo as id FROM xxxxxxxxxxx WHERE TRIM(valor) = TRIM('"+str(reg["codigo_muestra"])+"');"
                cursor.execute(sql__punto_muestreo_id)
                aux_id = cursor.fetchone()
                aux_pass = (bool(aux_id) == True)
        except Exception as e:
            aux_pass = False
            print("Error 1:", e)
        if(aux_pass):
            to_insert["reporte_datos_monitoreo"][iteration]["punto_muestreo_id"] = aux_id["id"]
        else:
            global_pass = False
            errors[reg["id"]].append("Campo punto_muestreo_id no existe")
            to_insert["reporte_datos_monitoreo"][iteration] = None
            continue
        # Check and insert "parametro_id"
        try:
            with db.cursor() as cursor:
                sql__parametro_id = "SELECT trad.id_parametro as id FROM xxxxxxxxxxx as trad LEFT JOIN ambito_parametro as amb ON trad.id_parametro = amb.parametro_id WHERE amb.ambito_id = 1 AND TRIM(trad.valor) = TRIM(CONCAT( '"+str(reg["nombre_quimico"])+"', ' ("+str(reg["unidad_resultado"])+")' ));"
                cursor.execute(sql__parametro_id)
                aux_id = cursor.fetchone()
                aux_pass = (bool(aux_id) == True)
        except Exception as e:
            aux_pass = False
            print("Error 2:", e)
        if(aux_pass):
            to_insert["reporte_datos_monitoreo"][iteration]["parametro_id"] = aux_id["id"]
        else:
            global_pass = False
            errors[reg["id"]].append("Campo parametro_id no existe")
            to_insert["reporte_datos_monitoreo"][iteration] = None
            continue
        # Check value "total_disuelto"
        if(str(reg["total_disuelto"]) == "D"):
            aux_pass = False
            errors[reg["id"]].append("Campo total_disuelto igual a 'D'")
            to_insert["reporte_datos_monitoreo"][iteration] = None
            continue
        # Check and insert "valor"
        aux_value = extract_nums_from_string(str(reg["valor_resultado"]))
        if aux_value != None:
            aux_value_check = check_positive_values(aux_value)
            if aux_value_check:
                aux_pass = True
                aux_valid_records += 1
                to_insert["reporte_datos_monitoreo"][iteration]["valor"] = aux_value
            else:
                aux_pass = False
                errors[reg["id"]].append("Campo valor_resultado <= 0")
                to_insert["reporte_datos_monitoreo"][iteration] = None
                continue
        else:
            aux_pass = False
            errors[reg["id"]].append("Campo valor_resultado no contiene un número")
            to_insert["reporte_datos_monitoreo"][iteration] = None
            continue

        # Insert others values
        if aux_pass:
            # Calculate and insert "indicador_deteccion"
            if reg["indicador_deteccion"] == "Y":
                aux_flag_values += 1
                to_insert["reporte_datos_monitoreo"][iteration]["estado_alerta_id"] = 3
            else:
                to_insert["reporte_datos_monitoreo"][iteration]["estado_alerta_id"] = 1

            if reg["indicador_deteccion"] == "N":
                to_insert["reporte_datos_monitoreo"][iteration]["simbolo"] = "<"
            else:
                to_insert["reporte_datos_monitoreo"][iteration]["simbolo"] = extract_numeric_symbols_from_string(str(reg["valor_resultado"]))

            to_insert["reporte_datos_monitoreo"][iteration]["fecha_recepcion"] = reg["created_at"]
            to_insert["reporte_datos_monitoreo"][iteration]["fecha_medicion"] = reg["fecha_hora_muestreo"].date()
            to_insert["reporte_datos_monitoreo"][iteration]["hora_medicion"] = reg["fecha_hora_muestreo"].time()
            # Null values
            to_insert["reporte_datos_monitoreo"][iteration]["tipo_valor_id"] = None
            to_insert["reporte_datos_monitoreo"][iteration]["unidad_medida_id"] = None
            to_insert["reporte_datos_monitoreo"][iteration]["codigo_cretificado"] = None
            to_insert["reporte_datos_monitoreo"][iteration]["valor_string"] = None
            # TDB values
            to_insert["reporte_datos_monitoreo"][iteration]["usuario_creador_id"] = None

            # Procesing "alerta_dato_monitoreo" data
            if reg["limite_de_deteccion_metodo"] != None:
                to_insert["alerta_dato_monitoreo"][iteration] = {}
                to_insert["alerta_dato_monitoreo"][iteration]["valor_superacion_maximo"] = reg["limite_de_deteccion_metodo"]
                to_insert["alerta_dato_monitoreo"][iteration]["estado_alerta_id"] = 3 if reg["indicador_deteccion"] == "Y"  else 1
                # Null values
                to_insert["alerta_dato_monitoreo"][iteration]["valor_superacion_minimo"] = None
                to_insert["alerta_dato_monitoreo"][iteration]["valor_alerta_minimo"] = None
                to_insert["alerta_dato_monitoreo"][iteration]["valor_alerta_maximo"] = None
                to_insert["alerta_dato_monitoreo"][iteration]["programa_id"] = None
                # TDB values
                to_insert["alerta_dato_monitoreo"][iteration]["usuario_creador_id"] = None

        # Procesing main tables
        if main_data_iteration == -1:
            main_data_iteration = iteration
            # Procesing "archivos_nuevo" data
            # Get and insert "nombre", "url" for xlsx file
            aux_xlsx_file = None
            try:
                with db.cursor() as cursor:
                    sql__aux_xlsx_file = "SELECT nombre, ruta FROM xxxxxxxxxxx WHERE id = "+str(reg["file_id"])+";"
                    cursor.execute(sql__aux_xlsx_file)
                    aux_xlsx_file = cursor.fetchone()
                    aux_pass = (bool(aux_xlsx_file) == True)
            except Exception as e:
                aux_pass = False
                global_pass = False
                print("Error 3:", e)
            if(aux_pass):
                to_insert["archivos_nuevo"][0]["descripcion"] = aux_xlsx_file["nombre"]
                to_insert["archivos_nuevo"][0]["url"] = aux_xlsx_file["ruta"]
            else:
                errors[reg["id"]].append("No se encontró el archivo .xlsx asociado")
            # Get and insert "nombre", "url" for pdf file
            if aux_pass:
                aux_pdf_file = None
                try:
                    with db.cursor() as cursor:
                        # sql__aux_pdf_file = "SELECT nombre, ruta FROM xxxxxxxxxxx WHERE REPLACE(REPLACE(nombre,'-',''),'.pdf','') = REPLACE(REPLACE ('"+str(reg["id_muestra_laboratorio"])+"', '-',''),'/','') AND tipo = 2;"
                        sql__aux_pdf_file = "SELECT nombre, ruta FROM xxxxxxxxxxx WHERE REPLACE(REPLACE(REPLACE(nombre,'_',''),'.pdf',''), '-', '') = REPLACE(REPLACE ('"+str(reg["id_muestra_laboratorio"])+"', '-',''),'/','') AND tipo = 2;"
                        # print("sql__aux_pdf_file: ", sql__aux_pdf_file)
                        cursor.execute(sql__aux_pdf_file)
                        # print("pass : cursor.execute(sql__aux_pdf_file)")
                        aux_pdf_file = cursor.fetchone()
                        # print("aux_pdf_file: ", aux_pdf_file)
                        aux_pass = (bool(aux_pdf_file) == True)
                        # print("aux_pass: ", aux_pass)
                except Exception as e:
                    aux_pass = False
                    global_pass = False
                    print("Error 4:", e)
                if(aux_pass):
                    to_insert["archivos_nuevo"][1]["descripcion"] = aux_pdf_file["nombre"]
                    to_insert["archivos_nuevo"][1]["url"] = aux_pdf_file["ruta"]
                else:
                    errors[reg["id"]].append("No se encontró el archivo .pdf asociado")
                    break

                if aux_pass:
                    # Null values
                    to_insert["archivos_nuevo"][0]["tipo_documento"] = to_insert["archivos_nuevo"][1]["tipo_documento"] = None
                    to_insert["archivos_nuevo"][0]["fecha_vencimiento"] = to_insert["archivos_nuevo"][1]["fecha_vencimiento"] = None
                    to_insert["archivos_nuevo"][0]["rut_empresa"] = to_insert["archivos_nuevo"][1]["rut_empresa"] = None
                    to_insert["archivos_nuevo"][0]["rut_representante_legal"] = to_insert["archivos_nuevo"][1]["rut_representante_legal"] = None
                    to_insert["archivos_nuevo"][0]["rut_persona_autorizada"] = to_insert["archivos_nuevo"][1]["rut_persona_autorizada"] = None
                    to_insert["archivos_nuevo"][0]["tipo_mod_estatuto_id"] = to_insert["archivos_nuevo"][1]["tipo_mod_estatuto_id"] = None

                    # Procesing "reporte_monitoreo" data
                    to_insert["reporte_monitoreo"][iteration] = {}
                    to_insert["reporte_monitoreo"][iteration]["punto_muestreo_id"] = to_insert["reporte_datos_monitoreo"][iteration]["punto_muestreo_id"]
                    to_insert["reporte_monitoreo"][iteration]["descripcion"] = reg["codigo_muestra"]
                    to_insert["reporte_monitoreo"][iteration]["fecha_muestreo"] = reg["fecha_hora_muestreo"].date()
                    to_insert["reporte_monitoreo"][iteration]["codigo_muestra"] = reg["id_muestra_laboratorio"]
                    # Fix values
                    to_insert["reporte_monitoreo"][iteration]["estado_reporte_id"] = 2
                    to_insert["reporte_monitoreo"][iteration]["created_at"] = datetime.now()
                    # Null values
                    to_insert["reporte_monitoreo"][iteration]["tipo_reporte_id"] = None
                    to_insert["reporte_monitoreo"][iteration]["usuario_validador_id"] = None
                    to_insert["reporte_monitoreo"][iteration]["ruta_tmp"] = None
                    # TDB values
                    to_insert["reporte_monitoreo"][iteration]["usuario_creador_id"] = 1226

    if global_pass and aux_valid_records > 0 and aux_pass: #aux_pass (pdf file)
        # Inserting to "archivos_nuevo"
        try:
            with db.cursor() as cursor:
                sql__insert_to_archivos_nuevo = set_insert_query("archivos_nuevo", to_insert["archivos_nuevo"][0])

                cursor.execute(sql__insert_to_archivos_nuevo, to_insert["archivos_nuevo"][0])
                aux_xlsx_file_id = cursor.lastrowid
                # print(aux_xlsx_file_id)

                cursor.execute(sql__insert_to_archivos_nuevo, to_insert["archivos_nuevo"][1])
                aux_pdf_file_id = cursor.lastrowid
                # print(aux_pdf_file_id)

            db.commit()
        except Exception as e:
            print("Error sql__insert_to_archivos_nuevo:", e)

        # Inserting to "reporte_monitoreo"
        to_insert["reporte_monitoreo"][main_data_iteration]["origen_archivos"] = 2 # Source for "archivos_nuevo" table
        to_insert["reporte_monitoreo"][main_data_iteration]["archivo_id"] = aux_xlsx_file_id
        to_insert["reporte_monitoreo"][main_data_iteration]["archivo_certificado_id"] = aux_pdf_file_id
        to_insert["reporte_monitoreo"][main_data_iteration]["datos_conflictivos"] = aux_flag_values

        try:
            with db.cursor() as cursor:
                sql__insert_to_reporte_monitoreo = set_insert_query("reporte_monitoreo", to_insert["reporte_monitoreo"][main_data_iteration])
                cursor.execute(sql__insert_to_reporte_monitoreo, to_insert["reporte_monitoreo"][main_data_iteration])
                aux_reporte_monitoreo_id = cursor.lastrowid
            db.commit()
        except Exception as e:
            print("Error sql__insert_to_reporte_monitoreo:", e)

        # Inserting to "reporte_datos_monitoreo"
        aux_reporte_datos_monitoreo_ids = {}
        try:
            with db.cursor() as cursor:
                for key in to_insert["reporte_datos_monitoreo"]:
                    if to_insert["reporte_datos_monitoreo"][key] != None:
                        to_insert["reporte_datos_monitoreo"][key]["reporte_monitoreo_id"] = aux_reporte_monitoreo_id
                        sql__insert_to_reporte_datos_monitoreo = set_insert_query("reporte_datos_monitoreo", to_insert["reporte_datos_monitoreo"][key])
                        cursor.execute(sql__insert_to_reporte_datos_monitoreo, to_insert["reporte_datos_monitoreo"][key])
                        aux_reporte_datos_monitoreo_ids[key] = cursor.lastrowid
                        # Updating table "reporte_monitoreo_agq"
                        try:
                            sql__update_reporte_monitoreo_agq = "UPDATE reporte_monitoreo_agq SET estado = 1 WHERE id = "+str(data[key]["id"])+";"
                            cursor.execute(sql__update_reporte_monitoreo_agq)
                            db.commit()
                        except Exception as e:
                            print("Error sql__update_reporte_monitoreo_agq 1:", e)
                    else:
                        # Updating table "reporte_monitoreo_agq"
                        try:
                            sql__update_reporte_monitoreo_agq = "UPDATE reporte_monitoreo_agq SET estado = 2 WHERE id = "+str(data[key]["id"])+";"
                            cursor.execute(sql__update_reporte_monitoreo_agq)
                            db.commit()
                        except Exception as e:
                            print("Error sql__update_reporte_monitoreo_agq 1:", e)
            db.commit()
        except Exception as e:
            print("Error sql__insert_to_reporte_datos_monitoreo:", e)

        # Inserting to "alerta_dato_monitoreo"
        try:
            with db.cursor() as cursor:
                for key in to_insert["alerta_dato_monitoreo"]:
                    if to_insert["alerta_dato_monitoreo"][key]["estado_alerta_id"] > aux_max_alert_value:
                        aux_max_alert_value = to_insert["alerta_dato_monitoreo"][key]["estado_alerta_id"]
                    to_insert["alerta_dato_monitoreo"][key]["reporte_datos_monitoreo_id"] = aux_reporte_datos_monitoreo_ids[key]
                    sql__insert_to_alerta_dato_monitoreo = set_insert_query("alerta_dato_monitoreo", to_insert["alerta_dato_monitoreo"][key])
                    cursor.execute(sql__insert_to_alerta_dato_monitoreo, to_insert["alerta_dato_monitoreo"][key])
            db.commit()
        except Exception as e:
            print("Error sql__insert_to_alerta_dato_monitoreo:", e)

        # Updating "estado de alertas" on tables "punto_muestreo" and "plan_monitoreo"
        if aux_flag_values > 0 and aux_max_alert_value > 0:
            # Updating table "punto_muestreo"
            aux_punto_muestreo_id = to_insert["reporte_datos_monitoreo"][main_data_iteration]["punto_muestreo_id"]
            try:
                with db.cursor() as cursor:
                    sql__update_muestreo = "UPDATE punto_muestreo SET estado_alerta_id = "+str(aux_max_alert_value)+" WHERE id = "+str(aux_punto_muestreo_id)+";"
                    cursor.execute(sql__update_muestreo)
                db.commit()
            except Exception as e:
                print("Error sql__update_muestreo:", e)
            try:
                with db.cursor() as cursor:
                    sql__get_values_to_update = "SELECT ppnto.plan_monitoreo_id, MAX(pto.estado_alerta_id) as new_status FROM xxxxxxxxxxx ppnto LEFT JOIN xxxxxxxxxxx ppnto2 on ppnto.plan_monitoreo_id = ppnto2.plan_monitoreo_id LEFT JOIN punto_muestreo pto on ppnto2.punto_muestreo_id = pto.id WHERE ppnto.punto_muestreo_id = "+str(aux_punto_muestreo_id)+" GROUP BY ppnto.plan_monitoreo_id;"
                    cursor.execute(sql__get_values_to_update)
                    aux__get_values_to_update = cursor.fetchall()
            except Exception as e:
                print("Error sql__select_xxxxxxxxxxx:", e)
            # Updating table "plan_monitoreo"
            for key in aux__get_values_to_update:
                try:
                    with db.cursor() as cursor:
                        sql__update_plan_monitoreo = "UPDATE plan_monitoreo SET estado_alerta_id = "+str(key["new_status"])+" WHERE id = "+str(key["plan_monitoreo_id"])+";"
                        cursor.execute(sql__update_plan_monitoreo)
                    db.commit()
                except Exception as e:
                    print("Error sql__update_plan_monitoreo:", e)
    else:
        # Updating table "reporte_monitoreo_agq"
        try:
            with db.cursor() as cursor:
                for key in data:
                    sql__update_reporte_monitoreo_agq = "UPDATE xxxxxxxxxxx SET estado = 2 WHERE id = "+str(key["id"])+";"
                    cursor.execute(sql__update_reporte_monitoreo_agq)
            db.commit()
        except Exception as e:
            print("Error sql__update_reporte_monitoreo_agq 3:", e)

    # Errors
    # print("==== ERRORS ====")
    # print(errors)
    # print("================")
    return errors;

def set_insert_query(aux_tablename, aux_dict):
    aux_names = list(aux_dict)
    aux_cols = ', '.join(map(escape_name, aux_names))
    aux_placeholders = ', '.join(['%({})s'.format(name) for name in aux_names])
    aux_query = 'INSERT INTO '+aux_tablename+' ({}) VALUES ({})'.format(aux_cols, aux_placeholders)
    return aux_query

def escape_name(s):
    return '`{}`'.format(s.replace('`', '``'))

# def extract_nums_from_string(s):
#     aux_ret = ''.join([n for n in s if (n.isdigit() or n=="." or n=="," or n=="-") ])
#     return aux_ret if aux_ret != '' else None

def extract_nums_from_string(s):
    s = s.replace(",", ".")

    countDecimalPoint = 0
    countNegativeSymbol = 0
    aux_ret = ""


    for n in range(len(s)):
        if s[n].isdigit():
            aux_ret += str(s[n])
        if s[n]==".":
            aux_ret += str(s[n])
            countDecimalPoint += 1
        if s[n]=="-":
            aux_ret += str(s[n])
            countNegativeSymbol += 1
        if (s[n]==">" or s[n]=="<" or s[n]=="="):
            continue
        if s[n].isalpha():
            return None

    return aux_ret if (aux_ret != '' and countDecimalPoint <= 1 and countNegativeSymbol <= 1) else None

# def extract_str_from_string(s):
#     return ''.join([n for n in s if (n.isalpha() or n==">" or n=="<" or n=="=") ])

def extract_numeric_symbols_from_string(s):
    aux_ret = ''.join([n for n in s if (n==">" or n=="<" or n=="=") ])
    return aux_ret if aux_ret != '' else None

def check_positive_values(strnum):
    return float(strnum)>0

# def format_number_string(strnum):
#     strnum = strnum.replace('.','')
#     strnum = strnum.replace(',','.')
#     return strnum

def group_data(data):
    aux_dict = {}
    c = 0
    k = 0
    aux_estado = None
    aux_codigo_muestra = None
    aux_fecha_hora_muestreo = None

    for key in data:
        if c == 0:
            aux_list = []
            aux_list.append(key)
            aux_dict[k] = aux_list
        elif aux_estado == key["estado"] and aux_codigo_muestra == key["codigo_muestra"] and aux_fecha_hora_muestreo == key["fecha_hora_muestreo"]:
            aux_dict[k].append(key)
        else:
            k += 1
            aux_list = []
            aux_list.append(key)
            aux_dict[k] = aux_list
        c += 1
        aux_estado = key["estado"]
        aux_codigo_muestra = key["codigo_muestra"]
        aux_fecha_hora_muestreo = key["fecha_hora_muestreo"]

    return aux_dict

def send_email(errors):

    body = "Estimados xxxxxxxxxxx:"
    body += "<br>";
    body += "<br>";
    body += "A continuación, se adjunta el log correspondiente al procesamiento de datos de monitoreo xxxxxxxxxxx, según:";
    body += "<br>";
    body += "<ul>";

    for key in errors:
        body += "<li>"+str(errors[key])+"</li>"

    body += "</ul>";
    body += "<br>";
    body += "Mensaje automático enviado por xxxxxxxxxxx. Favor no responder a este correo."
    body += "<br>";
    body += "Saludos cordiales.";

    tz = pytz.timezone('America/Santiago')
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    client = boto3.client('ses', region_name="us-east-1")

    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    "xxxxxxxxxxx@xxxxxxxxxxx.cl", "xxxxxxxxxxx@xxxxxxxxxxx.cl", "xxxxxxxxxxx@xxxxxxxxxxx.cl"
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': 'UTF-8',
                        'Data': body,
                    }
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': email_subject_qa+"Notificación - Log procesamiento de datos monitoreo SFTP xxxxxxxxxxx "+now,
                },
            },
            Source="xxxxxxxxxxx <xxxxxxxxxxx@xxxxxxxxxxx.com>"
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

def lambda_handler(event, context):

    # Initialize vars
    init_vars()

    # Initialize DB
    init_db()

    # Retrieve data to process
    unprocessed_data = retrieve_unprocessed_data()

    if len(unprocessed_data) > 0:
        errors = {}
        # Group data
        grouped_data = group_data(unprocessed_data)
        # Process data
        for key in grouped_data:
            errors[key] = (process_data(grouped_data[key]))
        # Send execution log
        send_email(errors)
    else:
        print("Sin registros para procesar!")

    # Dispose DB
    end_db()

# lambda_handler(1,2)