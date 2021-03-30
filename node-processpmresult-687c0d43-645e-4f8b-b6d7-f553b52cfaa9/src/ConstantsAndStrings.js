exports.DYNAMODB_CONSTANTS = {
    TABLE_NAME: "PmresultStructures",
    TABLE_KEY_NAME: "Name"
};

exports.S3_CONSTANTS = {
    BUCKET: "u2000"
};

exports.SES_CONSTANTS = {
    PARAMS: (pmresult, notifications) => {
        const ToAddresses = ['xxxxxxxx@wom.cl'];
        let template = '' +
            '<h3>Notificación PMRESULT: ' + pmresult + '</h3>'+
            '<p>El pmresult ' + pmresult + ' contiene los siguientes cambios</p>'+
            '<br>'
        notifications.map(value => {
            template += '<li>' + value + '</li>';
        });
        return {
            Destination: {ToAddresses},
            Message: {
                Body: {
                    Html: {
                        Charset: "UTF-8",
                        Data: template
                    }
                },
                Subject: {
                    Charset: 'UTF-8',
                    Data: 'Notificación PMRESULT'
                }
            },
            Source: 'etlpmresult@xxxxxx.xxxxxxx',
        }
    }
};

exports.MESSAGES = {
    ERROR: {
        CSV_NOT_CONTAIN_FIELDS: "EL CSV NO CONTIENE CAMPOS",
        TABLE_NOT_FOUND: "ER_NO_SUCH_TABLE",
        S3_NOT_COPIED: (err) => {
            return "ERROR AL COPIAR EL ARCHIVO: " + err;
        },
        FILE_NOT_PROCESSED: (err) => {
            return "ERROR AL PROCESAR EL ARCHIVO: " + err;
        }
    },
    SUCCESS: {
        FILE_PROCESSED: (fileName) => {
            return "EL ARCHIVO " + fileName + " FUE PROCESADO DE FORMA EXITOSA";
        }
    },
    INFO: {
        PMRESULT_CREATED_IN_DYNAMODB: (pmresultName, hasStructureChange) => {
            return 'PMRESULT ' + pmresultName + ' CREADO EN DYNAMODB: TABLA ' + pmresultName + ((hasStructureChange) ? ' ACTUALIZADA ' : ' CREADA');
        },
        STRUCTURE_CHANGE_HEADER: (tableName, lastCount, newCount) => {
            return 'EL PMRESULT ' + tableName + ' CONTIENE ' + newCount + ' CAMPOS Y LA TABLA CONTIENE ' + lastCount + ' SE ACTUALIZARÁ'
        },
        ADD_NEW_COLUMN: (inCsv, inTable) => {
            return 'SE HA AGREGADO UN NUEVO CAMPO EN LA TABLA, NOMBRE DEL CAMPO : ' + inTable + ' NOMBRE EN CSV ' + inCsv;
        },
        CHANGE_COLUMN_POSITION: (inCsv, inTable, lastPosition, newPosition) => {
            return 'EL CAMPO ' + inTable + ' (' + inCsv + ' EN ARCHIVO CSV) FUE CAMBIADO DE LA POSICION ' + lastPosition + ' A LA ' + newPosition;
        }
    }
};

exports.MYSQL_CONSTANTS = {
    HOST: "xxxxxxxxxx.xxxxxxxxx.amazonaws.com",
    USER: "XXXXXX",
    PASSWORD: "XXXXXXXX",
    DATABASE: "XXXXXXXXXX",
    CHARSET: "utf8mb4",
    SQL: {
        SHOW_COLUMNS: (table) => {
            return 'SHOW COLUMNS FROM ' + table + ';';
        },
        LOAD_DATA_LOCAL_INFILE: (file, table) => {
            return "LOAD DATA LOCAL INFILE '" + file + "' INTO TABLE " + table +
                " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 2 LINES ;";
        },
        CREATE_TABLE: (table, fields) => {
            let hasSdate = false;
            let hasobjectName = false;
            let query = 'CREATE TABLE `' + table + '` (';
            fields.map((fieldName, i) => {
                if(i <= 3){
                    let type = '';
                    switch (fieldName){
                        case 'SDATE':
                            hasSdate = true;
                            type = ' datetime NOT NULL';
                            break;
                        case 'Granularity Period':
                            type = ' smallint(6) DEFAULT NULL';
                            break;
                        case 'Object Name':
                            hasobjectName = true;
                            type = ' varchar(200) NOT NULL';
                            break;
                        case 'Reliability':
                            type = ' varchar(8) DEFAULT NULL';
                            break;
                        default:
                            type = ' varchar(200) DEFAULT NULL';
                            break;
                    }
                    query += '`' + fieldName + '` ' + type;
                }else{
                    query += '`' + fieldName + '` decimal(18,3) DEFAULT NULL';
                }
                query += (i < (fields.length -1) ? ', ' : '');
            });
            if(hasSdate && hasobjectName){
                query += ', PRIMARY KEY (`SDATE`,`Object Name`), KEY `SDATE` (`SDATE`)';
            }else if(hasSdate){
                query += ', KEY `SDATE` (`SDATE`)';
            }
            query += ') ENGINE=InnoDB DEFAULT CHARSET=latin1;';
            return query;
        },
        RENAME_TABLE: (name, newName) => {
            return 'RENAME TABLE ' + name + ' TO ' + newName +';'
        },
        SPLIT_COLUMN: (name) => {
            return name.substring(0, 60).split(' ').join('')
        }
    }
};
