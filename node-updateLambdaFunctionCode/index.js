exports.handler = async ( event, context ) => {

	// Setting global AWS Object
	var AWS = require('aws-sdk');
	// Setting credentials from file
	AWS.config.loadFromPath('./config/aws_credentials.json');


	/* Lamdba code UPDATE */
	// Initializing S3 Object
	var lambda = new AWS.Lambda();
	// Parameters
	var params = {
		FunctionName: "TelegramCoreFunction01",
		Publish: false,
		S3Bucket: "telegrambot-ent",
		S3Key: "botFunction01.zip",
	};
	// Doing UPDATE
	await lambda.updateFunctionCode(params, function(err, data) {
		if (err) {
			console.log(err, err.stack);
			return false;
		}
		else{
			console.log(data);
			return true;
		}
	});
};