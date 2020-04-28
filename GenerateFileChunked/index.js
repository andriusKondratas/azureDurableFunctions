/*
 * This function is not intended to be invoked directly. Instead it will be
 * triggered by an orchestrator function.
 */

const Snowflake = require('snowflake-sdk');
const Azure = require('azure-storage');
const Uuidv1 = require('uuid/v1');
const Stringify = require('csv-stringify');
const Stream = require('stream');

let snowflakeConn = null;  //cache the connection
let attempt = 0;  //cache attempts

/**
 * Issues a query against Snowflake
 * @param {*} sqlStatement : The sql statement to be executed. 
 * 
 */
function issueQuery(p_sqlStatement) {
    console.log(`Issuing query ${p_sqlStatement} ...`);

    var storageConnectionString = process.env["AZURE_STORAGE_CONNECTION_STRING"];
    var blobService = Azure.createBlobService(storageConnectionString);
    var blobName = 'generic' + Uuidv1() + '.csv';
    var response = {};
    response['BlobName:'] = blobName;
    response['RowCount:'] = 0;
    var hrstart = process.hrtime();

    return new Promise((resolve, reject) => {

        var blockIds = [];

        //execute sql statement
        var statement = snowflakeConn.execute({
            sqlText: p_sqlStatement
        });

        //create streams
        var stream = statement.streamRows();

        var stringifyStream = Stringify({
            header: false,
            formatters: {
                date: (v) => Moment(v).format(DATE_FORMAT)
            }
        });


        stringifyStream.on('data', function (chunk) {
            response['RowCount:'] = response['RowCount:'] + 1;
            var blockId = Uuidv1();
            const bufferStream = new Stream.PassThrough({
                highWaterMark: chunk.length
            });
            bufferStream.end(chunk);
            blobService.createBlockFromStream(blockId, 'genericdatafiles', blobName, bufferStream, chunk.length, function (error, response) {
                if (error) console.log(error);

                blockIds.push(blockId);
                console.log('Block created: blockId', blockId);
            });
        });

        stringifyStream.on('end', function () {
            var res = {
                status: 200,
                body: response,
                headers: { 'Content-Type': 'application/json' }
            };

            console.log('stringifyStream.on "end"');

            blobService.commitBlocks('genericdatafiles', blobName, blockIds, function (error, result) {
                if (error) console.log(error);

                console.log("All blocks uploaded");
                var hrend = process.hrtime(hrstart)
                console.info('Execution time (sec ms): %ds %dms', hrend[0], hrend[1] / 1000000)
            });

            resolve(res);
        });

        // catch any errors 
        stream.on('error', (err) => reject(err));
        stringifyStream.on('error', (err) => reject(err));

        stream
            .pipe(stringifyStream);

    });
}

/**
 * Connects to snowflake.
 */
function connectToSnowflake() {

    if (typeof snowflakeConn !== 'undefined' && snowflakeConn != null) {
        return new Promise(function (resolve, reject) {
            resolve(snowflakeConn);
        });
    }

    console.log(`Connecting to snowflake...`);
    Snowflake.configure({ ocspFailOpen: false });

    snowflakeConnObj = Snowflake.createConnection({
        account: "",
        username: "",
        password: "",
        database: "",
        schema: "",
        clientSessionKeepAlive: true,
        warehouse: "",
        role: ""
    });

    return new Promise((resolve, reject) => {
        snowflakeConnObj.connect(
            function (err, conn) {
                if (err) {
                    console.error('Unable to connect: ' + err.message);
                    reject(err);
                }
                else {
                    var connection_ID = conn.getId();
                    console.log(`Successfully connected to Snowflake. Connection ID: ${connection_ID}`);
                    resolve(conn);
                }
            }
        );
    });
}


module.exports = async function (context) {

    console.log('................................................................................................................');
    console.log(`REQUEST ${attempt}.........................................................................................................`);

    attempt = attempt + 1;
    var snowflakeConnPromise = connectToSnowflake();

    snowflakeConnPromise.then((conn) => {
        snowflakeConn = conn;

        var dumpResultsPromise = issueQuery(`select TOP ${context.bindings.name.fetchLimit} * from fact_statusmessage`);
        dumpResultsPromise.then((resp) => {
            console.log(`Successfully finished dumping results  ${JSON.stringify(resp)}`);
            return resp;
        }).catch((error) => {
            console.error('ERROR :' + error.message);
        });
    }).catch((error) => {
        console.error('ERROR :' + error.message);
    });
};