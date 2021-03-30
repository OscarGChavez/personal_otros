const { exec } = require('child_process');

exports.CsvService = {
    getCSVHeader: (path) => {
        return new Promise((resolve, reject) => {
            exec('head -n 1 ' + path, (err, stdout, stderr) => {
                if(err) reject(err)
                let fields = stdout.split(",").map((object) => {
                    return object.split('"').join('').split('\n').join('');
                });
                resolve(fields);
            });
        });
    }
};