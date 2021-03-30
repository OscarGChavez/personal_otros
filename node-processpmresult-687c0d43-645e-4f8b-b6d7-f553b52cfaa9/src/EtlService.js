const {AwsService} = require('./AwsService');
const {MysqlService} = require('./MysqlService');
const {CsvService} = require('./CsvService');
const _localFolder = '/tmp';
const {MESSAGES} = require('./ConstantsAndStrings');

const splitS3fromEvent = (s3ObjectKey) => {
    let cleanFileName = s3ObjectKey.split('/');
    let splitted = cleanFileName[cleanFileName.length-1].split('_');
    let pmresult = splitted[0] + '_' + splitted[1] + '_' + splitted[2];
    return pmresult;
};

const createTable = (item) => {
    return new Promise(async (resolve, reject) => {
        try {
            let status = {rename: null, create: null};
            if(item.Version > 0){
                status.rename = await MysqlService.renameTable(item.TableName, item.TableName + '_' + item.Version);
            }
            let tableName = item.TableName;
            let fields = item.FieldMap.map(el => {
                return el.inTable
            });
            status.create = await MysqlService.createTable(tableName, fields);
            resolve(status);
        }catch (e) {
            reject(e);
        }
    })
};

const resolveNotPresentInDynamoDB = async (item, tableHeader, csvHeader) => {
    return new Promise(async (resolve, reject) => {
        try {
            let changeStructure = false;
            //si la tabla existe y el archivo contiene la misma cantidad de campos, se mapean los campos de la tabla junto a los del archivo y se cambia la version del elemento
            if(tableHeader.length > 0 && csvHeader.length === tableHeader.length){
                item.Version = 0;
                item.FieldMap = tableHeader.map((el, i) => {
                    return {
                        inTable: el,
                        inCsv: csvHeader[i]
                    };
                });
                //Si tienen diferente cantidad de campos
            }else if(csvHeader.length !== tableHeader.length){
                changeStructure = true;
                //y la tabla existe se actualiza la versión, se invoca el método de crear tabla (si la tabla no existe se crea con la version 0 y no reemplaza nombres)
                if(tableHeader.length > 0){
                    item.Version++;
                }
                item.FieldCount = csvHeader.length;
                item.FieldMap = csvHeader.map((el, i) => {
                    return {
                        inTable: el.substring(0, 60).split(' ').join(''),
                        inCsv: el
                    };
                });
                await createTable(item);
            }
            resolve({item, changeStructure});
        }catch (e) {
            reject(e);
        }
    });
};

const checkStructures = (s3ObjectKey) => {
    return new Promise(async (resolve, reject) => {
        try{
            let notifications = [];
            const {pmresult, csvHeader, tableHeader, localFile, dynamoElement} = await getStructures(s3ObjectKey);
            let hasStructureChange = false;
            if(csvHeader.length === 0) reject(MESSAGES.ERROR.CSV_NOT_CONTAIN_FIELDS);
            let element = AwsService.newDynamoElement(pmresult, tableHeader.length);
            if(dynamoElement != null){
                element = dynamoElement;
                if(csvHeader.length != element.FieldMap.length){
                    notifications.push(MESSAGES.INFO.STRUCTURE_CHANGE_HEADER(pmresult, element.FieldMap.length, csvHeader.length));
                }
                let auxMap = csvHeader.map(el => ({ inTable: MysqlService.splitColumn(el), inCsv: el }));
                //Se evalúan todos los ítems del documento entrante
                for(let i = 0; i < auxMap.length; i++){
                    //se asume que hubo cambio de estructura
                    let hasChangeInFields = true;
                    //si la estructura ya guardada tiene al menos la cantidad de campos de la posición del campo del documento entrante
                    //y además este campo se encuentra en la misma posición quiere decir que no ha sido cambiado
                    if((element.FieldMap.length > i) && auxMap[i].inCsv === element.FieldMap[i].inCsv){
                        auxMap[i].inTable = element.FieldMap[i].inTable;
                        hasChangeInFields = false;
                    }else{
                        let fieldNotification = MESSAGES.INFO.ADD_NEW_COLUMN(auxMap[i].inCsv, auxMap[i].inTable);
                        //De lo contrario se busca el mapea del campo enytrante en todos los ya guardados
                        //Si encuentra este campo le otorga el nombre del que está mapeado
                        //Sin embargo porque ya no está en la misma posición hubo cambio de estructura
                        for(let j = 0; j < element.FieldMap.length; j++){
                            if(auxMap[i].inCsv === element.FieldMap[j].inCsv){
                                auxMap[i].inTable = element.FieldMap[j].inTable;
                                fieldNotification = MESSAGES.INFO.CHANGE_COLUMN_POSITION(element.FieldMap[j].inCsv, element.FieldMap[j].inTable, j, i);
                                break;
                            }
                        }
                        notifications.push(fieldNotification);
                    }
                    if(hasChangeInFields){
                        hasStructureChange = true;
                    }
                }

                if(hasStructureChange || csvHeader.length !== element.FieldMap.length){
                    hasStructureChange = true;
                    element.FieldMap = auxMap;
                    element.Version++;
                    await createTable(element);
                }
            }else{
                let resolve = await resolveNotPresentInDynamoDB(element, tableHeader, csvHeader);
                element = resolve.item;
                hasStructureChange = true;
                notifications.push(MESSAGES.INFO.PMRESULT_CREATED_IN_DYNAMODB(element.Name, resolve.changeStructure));
            }
            resolve({pmresult, element, localFile, hasStructureChange, notifications})
        }catch (e) {
            reject(e);
        }
    })
};

const getStructures = (s3ObjectKey) => {
    return new Promise(async (resolve, reject) => {
        try{
            let pmresult = splitS3fromEvent(s3ObjectKey);
            let localFile = await AwsService.getS3Object(s3ObjectKey, _localFolder + '/' + pmresult + '.csv');
            let dynamoElement = await AwsService.getDynamoStructure(pmresult);
            let columnsResult = await MysqlService.getTableHeader(pmresult);
            let tableHeader = [];
            if(columnsResult.error !== null && columnsResult.error.code !== MESSAGES.ERROR.TABLE_NOT_FOUND){
                reject(columnsResult.error);
            }else if(columnsResult.error === null && columnsResult.results !== null){
                tableHeader = columnsResult.results.map(data => {
                    return data.Field;
                });
            }
            let csvHeader = await CsvService.getCSVHeader(localFile);
            resolve({pmresult, csvHeader, tableHeader, localFile, dynamoElement});
        }catch (e) {
            reject(e);
        }
    });
};

const runProcess = async (s3ObjectKey) => {
    return new Promise(async (resolve, reject) => {
        try{
            const {pmresult, element, localFile, hasStructureChange, notifications} = await checkStructures(s3ObjectKey);
            if(hasStructureChange){
                await AwsService.setDynamoStructure(element);
            }
            await MysqlService.loadDataInFile(element.TableName, localFile);
            if(notifications.length > 0){
                await AwsService.sendEmail(pmresult, notifications);
            }
            resolve({Message: MESSAGES.SUCCESS.FILE_PROCESSED(s3ObjectKey), Notifications: notifications});
        }catch (e) {
            reject({Message: MESSAGES.ERROR.FILE_NOT_PROCESSED(e)});
        }
    })
};

exports.RunEtlProcess = runProcess;