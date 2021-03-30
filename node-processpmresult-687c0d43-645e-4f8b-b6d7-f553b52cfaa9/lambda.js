const {RunEtlProcess} = require('./src/EtlService')

exports.handler = function(event) {
    RunEtlProcess(event.Records[0].s3.object.key).then(message => {
        console.log(JSON.stringify(message, null, 2));
        process.exit();
    }).catch(error => {
        console.log(JSON.stringify({ERROR: error}, null, 2));
        process.exit();
    });
};
