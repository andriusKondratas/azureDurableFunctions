/*
 * This function is not intended to be invoked directly. Instead it will be
 * triggered by an orchestrator function.
 */

const Snowflake = require('snowflake-sdk');
const Azure = require('azure-storage');
const Uuidv1 = require('uuid/v1');
const Stringify = require('csv-stringify');
const Util = require('util');

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

    response['BlobName'] = blobName;
    response['RowCount'] = 0;


    return new Promise((resolve, reject) => {

        //execute sql statement
        var statement = snowflakeConn.execute({
            sqlText: p_sqlStatement
        });

        //create row stream
        var rowStream = statement.streamRows();

        //create csv stream
        var stringifyCsvStream = Stringify({
            header: false,
            formatters: {
                date: (v) => Moment(v).format(DATE_FORMAT)
            }
        });

        //create block blob stream
        var blobWriteStream = blobService.createWriteStreamToBlockBlob(
            'genericdatafiles',
            blobName,
            (error, result, response) => {
                if (error) {
                    console.error('Unable to open stream: ' + error.message);
                    response['Error message:'] = error.message;
                    reject(error);
                }
                else {
                    console.log(`Successfully opened stream to azure block blob. Response: ${JSON.stringify(response)}`);
                    resolve(result);
                }
            }
        );


        //catch any errors 
        rowStream.on('error', (err) => { response['Error message:'] = err.message; reject(err) });
        stringifyCsvStream.on('error', (err) => { response['Error message:'] = err.message; reject(err) });
        blobWriteStream.on('error', (err) => { response['Error message:'] = err.message; reject(err) });


        //track row count
        rowStream.on('data', function (row) {
            response['RowCount:'] = response['RowCount:'] + 1;
        });

        rowStream.on('end', function () {
            console.log("Streaming has ended");
            resolve(response);
        });

        rowStream
            .pipe(stringifyCsvStream)
            //.pipe(blobWriteStream);
    })
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
    var hrstart = process.hrtime(); //start time

    var finalResponse = {};
    var res = {
        status: 200,
        body: finalResponse,
        headers: { 'Content-Type': 'application/json' }
    };

    snowflakeConnPromise.then((conn) => {
        snowflakeConn = conn;
        var dumpResultsPromise = issueQuery(`select TOP ${context.bindings.name.fetchLimit} * from fact_statusmessage`);

        dumpResultsPromise.then((response) => {
            var hrend = process.hrtime(hrstart);

            console.log(`Successfully finished dumping results. Response:  ${JSON.stringify(response)}`);
            console.info('Execution time (sec ms): %ds %dms', hrend[0], hrend[1] / 1000000);

            finalResponse['Execution time'] = Util.format('Execution time (sec ms): %ds %dms', hrend[0], hrend[1] / 1000000);
            finalResponse['Dump response'] = response;

            return res;
        }).catch((error) => {
            console.error('ERROR :' + error.message);
            finalResponse['Error message'] = error.message;
        });
    }).catch((error) => {
        console.error('ERROR :' + error.message);
        finalResponse['Error message'] = error.message;
    });

    //return res;
};