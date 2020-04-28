## Azure Durable Functions POC
* `GenerateFile` - Accepts HTTP GET request with sample query & executes & dumps contents to blob
* `GenerateFileChunked` - "--", Use different approach (blocks vs stream) when creating block Blobs
* [GitHub repository](https://github.com/Azure/azure-functions-durable-js)
### Prerequisites:
* VS Code installed
* Azure subscription is present [Start Free](https://azure.microsoft.com/en-us/free/?ref=microsoft.com&utm_source=microsoft.com&utm_medium=docs&utm_campaign=visualstudio)
* You have created resource group & Storage (Blob)
* Azure Functions VS Code Extension is installed
* Azure Core Tools is installed [Azure Core Tools](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=linux%2Ccsharp%2Cbash)
* NodeJS & npm are installed
```
curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -
sudo apt-get install -y nodejs
```
* Clone repository `https://github.com/officeI/azureDurableFunctions.git ${repository location}`
* Durable functions npm package is installed
   * Open terminal 
 ```
   cd ${repository location}
   npm install durable-functions
   npm install typescript
   npm audit fix
   npm clean-install
   npm start
 ```
   * During first launch additional dependencies are downloaded

