const { extractTransformLoad } = require("./models/etlPipeline");

console.log("Welcome to Tyler Richards solution to the ETL Processing Test!\n");
console.log(
    "I've produced an ETL Pipeline to extract the JSON data from all of the .json.gz files in the input folder, transform the data to the desired schema for the test, and load all transformed data sets into .json files in the output folder.\n"
);
console.log(
    'I\'ve also included a few unit tests to ensure the data is being transformed correctly. You can run the tests by running "npm test" in the terminal.\n'
);
console.log(
    "================================================================================================================================================================================\n"
);

extractTransformLoad();
