/*
 * This function is not intended to be invoked directly. Instead it will be
 * triggered by an HTTP starter function.
 */

const df = require("durable-functions");

module.exports = df.orchestrator(function* (context) {
    const outputs = [];
    const req = context.df.getInput();

    if(req.chunked) {
        outputs.push(yield context.df.callActivity("GenerateFileChunked", context.df.getInput()));
    }else{
        outputs.push(yield context.df.callActivity("GenerateFile", context.df.getInput()));
    }

    return outputs;
});