let fs = require('fs');
let Client = require('ssh2-sftp-client');
let AWS = require('aws-sdk');
let mysql = require('mysql');

let sftp = new Client();
let s3 = new AWS.S3();
var ses = new AWS.SES({region: 'xxxxxxxxxxx-xxxxxxxxxxx-xxxxxxxxxxx'});

let sftpConfig = {
  host: 'xxxxxxxxxxx.xxxxxxxxxxx.xxxxxxxxxxx',
  port: 000000000,
  username: 'xxxxxxxxxxx',
  password: 'xxxxxxxxxxx'
};

let localPath = '/tmp/';
let s3Bucket = 'xxxxxxxxxxx-xxxxxxxxxxx';

let dbPool = "";
let dbHost = "";
let dbUser = "";
let dbPassword = "";
let dbSchema = "";
let s3Folder = '';
let remotePath = '';
let remoteProcessedPath = '';
let emailSubjectQA = '';

/**
 * [initVars description]
 * @author Oscar García Chávez
 * @date   2020-12-31
 * @return [description]
 * @return {[type]}      [description]
 */
function initVars(){
	switch(process.env.environment){
		case "QA":
			remotePath = '/xxxxxxxxxxx/xxxxxxxxxxx/temp-pending-processing/';
			remoteProcessedPath = '/xxxxxxxxxxx/xxxxxxxxxxx/temp-pending-processing/processed/';
			s3Folder = 'qa/xxxxxxxxxxx/xxxxxxxxxxx/';
			emailSubjectQA = 'xxxxxxxxxxx - ';
			dbHost = "xxxxxxxxxxx.amazonaws.com";
			dbUser = "xxxxxxxxxxx";
			dbPassword = "xxxxxxxxxxx!";
			dbSchema = "xxxxxxxxxxx";
			break;
		case "PROD":
			remotePath = '/xxxxxxxxxxx/xxxxxxxxxxx/';
			remoteProcessedPath = '/xxxxxxxxxxx/xxxxxxxxxxx/processed/';
			s3Folder = 'prod/xxxxxxxxxxx/xxxxxxxxxxx/';
			dbHost = "xxxxxxxxxxx.amazonaws.com";
			dbUser = "xxxxxxxxxxx";
			dbPassword = "xxxxxxxxxxx,.";
			dbSchema = "xxxxxxxxxxx";
			break;
	}
}

/**
 * [initDBPool description]
 * @author Oscar García Chávez
 * @date   2020-12-31
 * @return [description]
 * @return {[type]}      [description]
 */
function initDBPool(){
  	dbPool  = mysql.createPool({
	    host     : dbHost,
	    user     : dbUser,
	    password : dbPassword,
	    database : dbSchema
	});
}

/**
 * [description]
 * @author Oscar García Chávez
 * @date   2020-08-27
 * @return [description]
 * @param  {[type]}      query  [description]
 * @param  {[type]}      values [description]
 * @return {[type]}             [description]
 */
const mysqlPreparedStatement = (query, values) => {
    return new Promise((resolve, reject) => {
        dbPool.getConnection((mysqlConError, connection) => {
            if(mysqlConError){
            	reject(mysqlConError);
            }
            connection.query(query, values, (mysqlQueryError, results) => {
                connection.release();
                resolve({error: mysqlQueryError, results: results});
            });
        });
    });
};

/**
 * [initSFTP description]
 * @author Oscar García Chávez
 * @date   2020-08-24
 * @return [description]
 * @return {[type]}      [description]
 */
function initSFTP(){
	return new Promise((resolve, reject) => {
      sftp.connect(sftpConfig)
		.then(() => {
			resolve();
		})
		.catch(err => {
			console.error(err.message);
			resolve();
		});
    });
}

/**
 * [endSFTP description]
 * @author Oscar García Chávez
 * @date   2020-08-24
 * @return [description]
 * @return {[type]}      [description]
 */
function endSFTP(){
	return new Promise((resolve, reject) => {
      sftp.end()
      	.then(() => {
			resolve();
		})
		.catch(err => {
			console.error(err.message);
			resolve();
		});
    });
}

/**
 * [listFiles description]
 * @author Oscar García Chávez
 * @date   2020-08-24
 * @return [description]
 * @return {[type]}      [description]
 */
function listFiles(){
    return new Promise((resolve, reject) => {
    	sftp.list(remotePath)
			.then(data => {
				console.log("listFiles", data)
				resolve(data)
			})
			.catch(err => {
				console.error(err.message);
				resolve();
			});
    });
};

/**
 * [retrieveFile description]
 * @author Oscar García Chávez
 * @date   2020-08-24
 * @return [description]
 * @param  {[type]}      fileName [description]
 * @return {[type]}               [description]
 */
function retrieveFile(fileName){
	var remoteFile = remotePath + fileName
	var destPath = localPath + fileName;
    return new Promise((resolve, reject) => {
		sftp.get(remoteFile, destPath)
		.then(() => {
			console.log("retrieveFile", fileName)
			resolve(destPath);
		})
		.catch(err => {
			console.error(err.message);
			resolve(false);
		});
    });
};

/**
 * [moveFile description]
 * @author Oscar García Chávez
 * @date   2020-08-27
 * @return [description]
 * @param  {[type]}      fileName [description]
 * @return {[type]}               [description]
 */
function moveFile(fileName, todayDir){
	var remoteFile = remotePath + fileName
	var remoteNewPath = remoteProcessedPath + todayDir + "/" + fileName;
    return new Promise((resolve, reject) => {
		sftp.rename(remoteFile, remoteNewPath)
		.then(() => {
			console.log("moveFile", fileName)
			resolve();
		})
		.catch(err => {
			console.error(err.message);
			resolve();
		});
    });
};

/**
 * [createTodayDir description]
 * @author Oscar García Chávez
 * @date   2020-08-27
 * @return [description]
 * @param  {[type]}      date [description]
 */
function createTodayDir(date){
    return new Promise((resolve, reject) => {
		sftp.mkdir(remoteProcessedPath+date, true)
		.then(() => {
			resolve();
		})
		.catch(err => {
			console.error(err.message);
			resolve();
		});
    });
};

/**
 * [uploadToS3 description]
 * @author Oscar García Chávez
 * @date   2020-08-24
 * @return [description]
 * @param  {[type]}      tempPath    [description]
 * @param  {[type]}      fileS3Key [description]
 * @return {[type]}                  [description]
 */
function uploadToS3(tempPath, fileS3Key){
	return new Promise((resolve, reject) => {
      	fs.readFile(tempPath, function (err, data) {
            if (err) {
                console.log(err);
                console.log("Params: ",tempPath, fileS3Key);
                resolve();
            }

            var params = {
				Bucket: s3Bucket,
				Key: fileS3Key,
				Body: data
			};

            s3.putObject(params, function (s3Err, resp) {
            	if (s3Err){
            		console.log(s3Err);
                	resolve();
            	}
                console.log("uploadToS3 - File: ",tempPath, " - ETAG: ", resp);
                resolve();
            });
        });
    });
}

function sendFilesNotification(files_data){

	var d     = new Date();
	var month = String("0" + (d.getMonth() + 1)).slice(-2);
	var day   = String("0" + d.getDate()).slice(-2);
	var date  = d.getFullYear() + "/" + month + "/" + day;
	var body  = "Estimados xxxxxxxxxxx:"
	body      += "<br>";
	body      += "<br>";
	body      += "A continuación, se adjunta el detalle de la extración de archivos correspondiente al día de hoy, según:";
	body      += "<br>";
	body      += "<ul>";

	if(files_data.length > 0){
		for (var file_data of files_data) {
			body     += "<li>Archivo: "+file_data.name+"</li>";
			body     += "<li>Tipo: "+file_data.type+"</li>";
			body     += "<br>";
		}
	}else{
		body     += "<li>No se encontraron nuevos archivos para procesar</li>";
	}

	body     += "</ul>";
	body     += "<br>";
	body     += "Mensaje automático enviado por xxxxxxxxxxx. Favor no responder a este correo."
	body     += "<br>";
	body     += "Saludos cordiales.";

	var params = {
        Destination: {
            ToAddresses: ["xxxxxxxxxxx@xxxxxxxxxxx.cl", "xxxxxxxxxxx@xxxxxxxxxxx.cl", "xxxxxxxxxxx@xxxxxxxxxxx.cl"]
        },
        Message: {
            Body: {
                Html: { Data: body}
            },
            Subject: { Data: emailSubjectQA+"Notificación - Extración archivos SFTP xxxxxxxxxxx "+date
            }
        },
        Source: "xxxxxxxxxxx <xxxxxxxxxxx@xxxxxxxxxxx.com>"
    };

    return new Promise((resolve, reject) => {
      	ses.sendEmail(params, function (err, data) {
	        if (err) {
	            console.log(err);
	            resolve();
	        } else {
	            console.log("sendFilesNotification:", data);
	            resolve();
	        }
	    });
    });
}

/**
 * [handler description]
 * @author Oscar García Chávez
 * @date   2020-08-24
 * @return [description]
 * @param  {[type]}      event [description]
 * @return {[type]}            [description]
 */
exports.handler = async (event) => {

	initVars();

	initDBPool();

	await initSFTP();

	var files_data = []
	var files = await listFiles();

	if (files.length > 0){
        console.log( "Files: "+files.length);
		var d = new Date();
		var month = String("0" + (d.getMonth() + 1)).slice(-2);
		var day   = String("0" + d.getDate()).slice(-2);
		var date = d.getFullYear() + month + day + "_";

		var todayDir = d.getFullYear() + month + day;
		await createTodayDir(todayDir);

		for (const file of files) {
			if (file.type == "-") {
				var extension   = file.name.split('.').pop();
				var fileFixName = date + Date.now() + "."+extension;
				var tempPath    = await retrieveFile(file.name);

				if (tempPath != false){
					files_data.push({type: extension, name: file.name});
				}

				await moveFile(file.name, todayDir);

				if (extension=="xlsx") {
					var fileType = 1;
					var s3KeyMod = "processing/";
					var status   = 0;
				}else{
					var fileType = 2;
					var s3KeyMod = "processed/";
					var status   = 1;
				}

				var fileS3Key = s3Folder+s3KeyMod+todayDir+"/"+fileFixName;

				await uploadToS3(tempPath, fileS3Key);

				var fileURL = "https://"+s3Bucket+".s3.amazonaws.com/"+fileS3Key;
				var toInsert = {
								nombre: file.name,
								tipo: fileType,
								ruta: fileURL,
								estado: status
		                    };
				var queryInsert = "INSERT INTO xxxxxxxxxxx SET ?";

		        await mysqlPreparedStatement(queryInsert, toInsert);
			}
		}

	}

	await endSFTP();

	await sendFilesNotification(files_data);
};