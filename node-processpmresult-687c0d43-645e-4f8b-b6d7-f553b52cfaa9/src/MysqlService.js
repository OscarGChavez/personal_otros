const _mysql = require('mysql');
const {MYSQL_CONSTANTS} = require('./ConstantsAndStrings');
const _mysqlConnection = _mysql.createPool({
    host: MYSQL_CONSTANTS.HOST,
    user: MYSQL_CONSTANTS.USER,
    password: MYSQL_CONSTANTS.PASSWORD,
    database: MYSQL_CONSTANTS.DATABASE,
    charset: MYSQL_CONSTANTS.CHARSET,
    multipleStatements : true,
    queueLimit : 0, // unlimited queueing
    connectionLimit : 0
});

const mysqlStatement = (query) => {
    return new Promise((resolve, reject) => {
        _mysqlConnection.getConnection((mysqlConError, connection) => {
            if(mysqlConError) reject(mysqlConError);
            connection.query(query, (mysqlQueryError, results) => {
                resolve({error: mysqlQueryError, results: results});
            });
        });
    });
};

exports.MysqlService = {
    getTableHeader: (pmresult) => {return mysqlStatement(MYSQL_CONSTANTS.SQL.SHOW_COLUMNS(pmresult))},
    loadDataInFile: (pmresult, file) => {
        return mysqlStatement(MYSQL_CONSTANTS.SQL.LOAD_DATA_LOCAL_INFILE(file, pmresult))
    },
    createTable: (tableName, fields) => {
        return mysqlStatement(MYSQL_CONSTANTS.SQL.CREATE_TABLE(tableName, fields));
    },
    renameTable: (name, newName) => {
        return mysqlStatement(MYSQL_CONSTANTS.SQL.RENAME_TABLE(name, newName));
    },
    splitColumn: (columnName) => {
        return MYSQL_CONSTANTS.SQL.SPLIT_COLUMN(columnName);
    }
};