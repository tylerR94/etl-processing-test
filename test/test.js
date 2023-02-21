const assert = require("assert");

describe("Test", function () {
    describe("Url transformation", function () {
        // this test case is based on the example "u" -> "url_object" transformation used in the instructions
        it("Should return a url object with the correct properties: domain (string), path (string), query_object (object[string], and hash (string).", function () {
            const { transformUrl } = require("../models/etlPipeline");
            assert.equal(
                JSON.stringify({
                    domain: "www.test.com",
                    path: "/products/productA.html",
                    query_object: { a: "5435", b: "test" },
                    hash: "#reviews",
                }),
                JSON.stringify(
                    transformUrl(
                        "https://www.test.com/products/productA.html?a=5435&b=test#reviews"
                    )
                )
            );
        });
    });

    describe(".json.gz to .json transformation", function () {
        it("Should verify the t1669976028340.json.gz input file exists, run the program, and verify the t1669976028340.json output file exists with the proper json.", async function () {
            const fs = require("fs");
            try {
                if (fs.existsSync("./input/t1669976028340.json.gz")) {
                    const {
                        extractTransformLoad,
                    } = require("../models/etlPipeline");
                    const { sleep } = require("../models/utils");

                    await extractTransformLoad();

                    // sleeping for 1 second to allow node time to populate the output folder before checking for it. this is sort of hacky but working with the local file system like this is hacky to begin with
                    await sleep(1000);

                    let outputFile;

                    try {
                        outputFile = fs.readFileSync(
                            "./output/t1669976028340.json"
                        );
                    } catch (outputError) {
                        assert.fail(
                            "The output file (./output/t1669976028340.json) doesn't exist. This could be due to file system latency not populating the file quick enough for this test, please try running this test again with the output folder intact."
                        );
                    }

                    assert.equal(
                        '[{"timestamp":1669976028340,"url_object":{"domain":"www.example.org","path":"/ho/ez","query_object":{},"hash":"#w;#n?r?h"},"ec":{"et":"dl","n":"digitalData","u":{"page_name":"store","store_id":153}}}]',
                        outputFile
                    );
                } else {
                    assert.fail(
                        "The necessary t1669976028340.json.gz file does not exist in the input folder. Please pull it down from the test repository and try again."
                    );
                }
            } catch (err) {
                assert.fail(err.message);
            }
        });
    });
});
