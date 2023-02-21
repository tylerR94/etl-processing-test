const fs = require("fs");
const util = require("util");
const readFile = (fileName) => util.promisify(fs.readFile)(fileName);
const { ungzip } = require("node-gzip");

/**
 * Decompress individual gzipped json files.
 *
 * @param {string} filePath - Path to the file to be decompressed. A .json.gz file is required.
 * @returns {Promise<object>} - The decompressed json object from the given file path.
 */
async function parseJsonGz(filePath) {
    if (!filePath || filePath.trim().slice(-8) !== ".json.gz") {
        throw new Error("A valid file path (ending in .json.gz) is required.");
    }

    try {
        const data = await readFile(filePath);
        const unzipped = await ungzip(data);
        const parsedToJson = JSON.parse(unzipped.toString());
        return parsedToJson;
    } catch (error) {
        console.error(error);
    }
}

/**
 * Transform a website url into an object with the following properties: domain, path, query_object, hash.
 *
 * @param {string} websiteUrl - The website url to be transformed.
 * @returns {object} - The transformed url object.
 *
 * @example
 * transformUrl("https://www.google.com/search?q=hello&oq=hello&aqs=chrome..69i57j0l5.1005j0j7&sourceid=chrome&ie=UTF-8#foo")
 * // returns
 * {
 *   domain: "www.google.com",
 *   path: "/search",
 *   query_object: {
 *     q: "hello",
 *     oq: "hello",
 *     aqs: "chrome..69i57j0l5.1005j0j7",
 *     sourceid: "chrome",
 *     ie: "UTF-8"
 *   },
 *   hash: "#foo"
 * }
 */
function transformUrl(websiteUrl) {
    const url = new URL(websiteUrl);
    return {
        domain: url.hostname,
        path: url.pathname,
        query_object: Object.fromEntries(url.searchParams),
        hash: url.hash,
    };
}

/**
 * Transform the data from the parsed json file into an array of objects with the following properties: timestamp, url_object, ec.
 *
 * @param {object} data - The parsed json data from the gzipped file.
 * @returns {array<object>} - An array of objects, one for every element of the event (e) array, with the relative timestamp and url_object from their parent.
 *
 * @example
 * transformData({
 *   ts: 1234567890,
 *   u: "https://www.google.com/search?q=hello#foo",
 *   e: ["foo", "baz"]
 * })
 * // returns
 * [
 *   {
 *     timestamp: 1234567890,
 *     url_object: {
 *       domain: "www.google.com",
 *       path: "/search",
 *       query_object: {
 *         q: "hello",
 *       },
 *       hash: "#foo"
 *     },
 *     ec: "foo"
 *   },
 *   {
 *     timestamp: 1234567890,
 *     url_object: {
 *       domain: "www.google.com",
 *       path: "/search",
 *       query_object: {
 *         q: "hello",
 *       },
 *       hash: "#foo"
 *     },
 *     ec: "baz"
 *   },
 * ]
 */
function transformData(data) {
    const transformation = [];

    const timestamp = data.ts;
    const url_object = transformUrl(data.u);

    data.e.forEach((event) => {
        transformation.push({
            timestamp,
            url_object,
            ec: event,
        });
    });

    return transformation;
}

/**
 * Save transformed data to a json file in the output folder.
 *
 * @param {string} fileName - The name of the file to be saved, not including any file extension.
 * @param {array<object>} arrayOfObjects - The array of objects to be saved.
 * @returns {void}
 */
async function saveTransformedData(fileName, arrayOfObjects) {
    const output = JSON.stringify(arrayOfObjects);
    await fs.writeFile(`./output/${fileName}.json`, output, (err) => {
        if (err) {
            console.error(err);
        }
    });
}

/**
 * Extract, transform, and load the data from the input folder into the output folder, following the transformation schema provided in the ETL Processing Test instructions.
 *
 * @returns {void}
 */
async function extractTransformLoad() {
    // create the output directory if it does not yet exist
    if (!fs.existsSync("./output")) {
        fs.mkdirSync("./output");
    }

    // read the input directory and process every file within it
    await fs.readdir("./input", async (err, files) => {
        if (!err) {
            console.log(`Found ${files.length} files in the input folder.`);
            console.log("Starting to process files...");

            await files.forEach(async (file) => {
                if (file.trim().slice(-8) !== ".json.gz") {
                    return;
                }

                try {
                    const fileNameWithoutExtension = file.slice(0, -8);
                    const parsedData = await parseJsonGz(`./input/${file}`);
                    const jsonToOutput = transformData(parsedData);
                    await saveTransformedData(
                        fileNameWithoutExtension,
                        jsonToOutput
                    );
                } catch (error) {
                    console.error(error);
                }
            });

            console.log(
                "All files processed, see the ./output folder for results."
            );
        } else {
            console.error(err);
        }
    });
}

module.exports = { transformUrl, extractTransformLoad };
