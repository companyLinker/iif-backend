import express from "express";
import multer from "multer";
import { MongoClient, ObjectId } from "mongodb";
import csv from "csv-parser";
import fs from "fs/promises";
import XLSX from "xlsx";
import moment from "moment";

const app = express();
app.use(express.json());
const port = 3000;

// CORS configuration
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );
  if (req.method === "OPTIONS") {
    res.header("Access-Control-Allow-Methods", "PUT, POST, PATCH, DELETE, GET");
    return res.status(200).json({});
  }
  next();
});

const mongoUrl =
  "mongodb+srv://krushant:kK58jbHcl5taHmNb@transactions-cluster.bkfz4.mongodb.net/?retryWrites=true&w=majority&appName=transactions-cluster";
const dbName = "transactionsDB";
const brandsDbName = "brandsDB";

const upload = multer({
  dest: "./Uploads/",
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      "text/csv",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "application/vnd.ms-excel",
    ];
    if (allowedTypes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error("Only CSV, XLSX, and XLS files are allowed"));
    }
  },
});

let db;
let brandsDb;
let mongoClient;

async function connectToMongoDB() {
  try {
    const client = await MongoClient.connect(mongoUrl, {
      connectTimeoutMS: 30000,
      socketTimeoutMS: 30000,
    });
    db = client.db(dbName);
    brandsDb = client.db(brandsDbName);
    console.log("Connected to MongoDB");
    await db.collection("SampleData").createIndex({ Date: 1 });
    await db.collection("SampleData").createIndex({ StoreName: 1, Date: 1 });
    await db.collection("BMData").createIndex({ store_name: 1 });
    await db.collection("Formats").createIndex({ name: 1 }, { unique: true });
    return client;
  } catch (err) {
    console.error("Error connecting to MongoDB:", {
      message: err.message,
      stack: err.stack,
    });
    throw err;
  }
}

// Clean column names and values for SampleData
function cleanObjectForSampleData(obj) {
  console.log("Processing columns and sample values:", {
    columns: Object.keys(obj),
    sample: obj,
  });
  return Object.keys(obj).reduce((acc, key) => {
    // Replace dots with underscores and remove extra spaces
    const cleanKey = key.trim().replace(/\s+/g, "").replace(/\./g, "_");
    let value = obj[key];

    if (typeof value === "string") {
      value = value.trim().replace(/\x00/g, "");
      if (cleanKey.toLowerCase() !== "date") {
        const parsedNumber = parseFloat(value.replace(/[^0-9.-]/g, ""));
        if (!isNaN(parsedNumber) && value.match(/^-?\d*\.?\d*$/)) {
          value = parsedNumber;
        }
      }
    } else if (value === null || value === undefined) {
      value = null;
    }

    if (cleanKey.toLowerCase() === "date") {
      if (typeof value === "number") {
        const excelEpoch = new Date(Date.UTC(1899, 11, 31));
        const utcDate = new Date(excelEpoch.getTime() + (value - 1) * 86400000);
        value = moment.utc(utcDate).format("MM-DD-YYYY");
      } else if (value) {
        console.log(`Raw date value: "${value}"`);
        const parsedDate = moment(
          String(value),
          [
            "MM-DD-YYYY",
            "MM/DD/YYYY",
            "YYYY-MM-DD",
            "DD-MM-YYYY",
            "DD/MM/YYYY",
            "M/D/YYYY",
            "MM-DD-YY",
            "M-D-YYYY",
          ],
          true
        );
        if (parsedDate.isValid()) {
          value = parsedDate.format("MM-DD-YYYY");
        } else {
          console.log(`Failed to parse date: "${value}"`);
          value = null;
        }
      }
    }

    acc[cleanKey] = value;
    return acc;
  }, {});
}

// Clean column names and values for BMData
function cleanObjectForBMData(obj) {
  const cleaned = Object.keys(obj).reduce((acc, key) => {
    const cleanKey = key.trim().replace(/\s+/g, "").replace(/\./g, "_");
    let value = obj[key];

    if (typeof value === "string") {
      value = value.trim().replace(/\x00/g, "");
    } else if (value instanceof Date) {
      value = moment(value).format("MM-DD-YYYY");
    } else if (typeof value === "number" && !isNaN(value)) {
      const date = new Date((value - 25569) * 86400 * 1000);
      value = moment(date).format("MM-DD-YYYY");
    }

    acc[cleanKey] = value;
    return acc;
  }, {});

  delete cleaned._id;
  return cleaned;
}

connectToMongoDB()
  .then((client) => {
    mongoClient = client;

    // Endpoint to get brand collections
    app.get("/api/brands", async (req, res) => {
      try {
        if (!brandsDb) {
          await connectToMongoDB();
          if (!brandsDb) throw new Error("Failed to connect to MongoDB");
        }
        const collections = await brandsDb.listCollections().toArray();
        const brandNames = collections.map((col) => col.name);
        res.send(brandNames);
      } catch (err) {
        console.error("Error fetching brands:", err);
        res.status(500).send({ message: "Error fetching brands" });
      }
    });

    app.get("/api/filter-options", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db) throw new Error("Failed to connect to MongoDB");
        }

        const { state, brand } = req.query;
        const matchQuery = {};
        if (state) {
          matchQuery.State = Array.isArray(state) ? { $in: state } : state;
        }
        if (brand) {
          matchQuery.BRAND = Array.isArray(brand) ? { $in: brand } : brand;
        }

        const states = await db
          .collection("BMData")
          .distinct("State", { State: { $ne: null } });

        const brands = await db
          .collection("BMData")
          .distinct("BRAND", { BRAND: { $ne: null } });

        const storeMappings = await db
          .collection("BMData")
          .find(
            {
              ...matchQuery,
              POS_COMPANY_NAME: { $ne: null },
            },
            {
              projection: {
                POS_COMPANY_NAME: 1,
                State: 1,
                BRAND: 1,
                mapped_col_name: 1,
                _id: 0,
              },
            }
          )
          .toArray();

        console.log("Fetched filter options:", {
          stateCount: states.length,
          brandCount: brands.length,
          storeMappingCount: storeMappings.length,
          query: { state, brand },
        });

        res.send({
          states,
          brands,
          storeMappings,
        });
      } catch (err) {
        console.error("Error fetching filter options:", err);
        res.status(500).send({ message: "Error fetching filter options" });
      }
    });

    app.get("/api/formats", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db) throw new Error("Failed to connect to MongoDB");
        }

        const formats = await db.collection("Formats").find({}).toArray();
        console.log(`Retrieved ${formats.length} formats`);
        res.status(200).send(
          formats.map((format) => ({
            ...format,
            _id: format._id.toString(),
          }))
        );
      } catch (err) {
        console.error("Error retrieving formats:", err.message);
        res
          .status(500)
          .send({ message: "Error retrieving formats", error: err.message });
      }
    });

    app.post("/api/formats", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db) throw new Error("Failed to connect to MongoDB");
        }

        const formatData = req.body;
        if (!formatData.name) {
          return res.status(400).send({ message: "Format name is required" });
        }

        console.log("Received format data:", {
          name: formatData.name,
          fields: Object.keys(formatData),
        });

        const validFields = [
          "name",
          "keyMappings",
          "valueMappings",
          "selectedColumns",
          "nonZeroColumns",
          "positionMappings",
          "calculationType",
          "calculatedColumnTypes",
          "calculatedColumnIsCustomString",
          "calculatedColumnNames",
          "emptyColumnNames",
          "coaTargetIifColumn",
          "bankTargetIifColumn",
          "storeSplitIifColumn",
          "memoMappingType",
          "selectedStates",
          "selectedBrands",
          "selectedStoreNames",
          "normalizedColumnMap",
          "calculatedColumns",
        ];

        const cleanedFormat = Object.keys(formatData).reduce((acc, key) => {
          if (validFields.includes(key)) {
            acc[key] = formatData[key];
          }
          return acc;
        }, {});

        // Validate calculatedColumns
        if (cleanedFormat.calculatedColumns) {
          if (!Array.isArray(cleanedFormat.calculatedColumns)) {
            return res
              .status(400)
              .send({ message: "calculatedColumns must be an array" });
          }
          cleanedFormat.calculatedColumns = cleanedFormat.calculatedColumns.map(
            (col) => ({
              name: typeof col.name === "string" ? col.name : "",
              formula: typeof col.formula === "string" ? col.formula : "",
              selectedColumns: Array.isArray(col.selectedColumns)
                ? col.selectedColumns
                : [],
              calculationType:
                typeof col.calculationType === "string" &&
                ["Formula", "Answer"].includes(col.calculationType)
                  ? col.calculationType
                  : "Answer",
            })
          );
        } else {
          cleanedFormat.calculatedColumns = [];
        }

        if (!Array.isArray(cleanedFormat.emptyColumnNames)) {
          cleanedFormat.emptyColumnNames = [];
        }

        try {
          const result = await db
            .collection("Formats")
            .insertOne(cleanedFormat);
          console.log(
            `Created format: ${cleanedFormat.name}, ID: ${result.insertedId}`
          );

          const createdFormat = await db
            .collection("Formats")
            .findOne({ _id: result.insertedId });
          res.status(201).send({
            ...createdFormat,
            _id: createdFormat._id.toString(),
          });
        } catch (err) {
          if (err.code === 11000) {
            const existingFormat = await db
              .collection("Formats")
              .findOne({ name: formatData.name });
            console.error(`Duplicate format name: ${formatData.name}`);
            return res.status(400).send({
              message: `Format name "${formatData.name}" already exists`,
              _id: existingFormat._id.toString(),
            });
          }
          console.error("Failed to insert format:", err.message);
          throw err;
        }
      } catch (error) {
        console.error("Error creating format:", error.message);
        res
          .status(500)
          .send({ message: "Error creating format", error: error.message });
      }
    });

    app.put("/api/formats/:id", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db) throw new Error("MongoDB connection failed");
        }

        const formatId = req.params.id;
        const formatData = req.body;
        if (!formatData.name) {
          return res.status(400).send({ message: "Format name is required" });
        }

        console.log(`Updating format ID: ${formatId}, Data:`, formatData.name);

        const validFields = [
          "name",
          "keyMappings",
          "valueMappings",
          "selectedColumns",
          "nonZeroColumns",
          "positionMappings",
          "calculationType",
          "calculatedColumnTypes",
          "calculatedColumnIsCustomString",
          "calculatedColumnNames",
          "emptyColumnNames",
          "coaTargetIifColumn",
          "bankTargetIifColumn",
          "storeSplitIifColumn",
          "memoMappingType",
          "selectedStates",
          "selectedBrands",
          "selectedStoreNames",
          "normalizedColumnMap",
          "calculatedColumns",
        ];

        const cleanedFormatData = Object.keys(formatData).reduce((acc, key) => {
          if (validFields.includes(key)) {
            acc[key] = formatData[key];
          }
          return acc;
        }, {});

        if (cleanedFormatData.calculatedColumns) {
          if (!Array.isArray(cleanedFormatData.calculatedColumns)) {
            return res
              .status(400)
              .send({ message: "calculatedColumns must be an array" });
          }
          cleanedFormatData.calculatedColumns =
            cleanedFormatData.calculatedColumns.map((col) => ({
              name: typeof col.name === "string" ? col.name : "",
              formula: typeof col.formula === "string" ? col.formula : "",
              selectedColumns: Array.isArray(col.selectedColumns)
                ? col.selectedColumns
                : [],
              calculationType:
                typeof col.calculationType === "string" &&
                ["Formula", "Answer"].includes(col.calculationType)
                  ? col.calculationType
                  : "Answer",
            }));
        } else {
          cleanedFormatData.calculatedColumns = [];
        }

        if (!Array.isArray(cleanedFormatData.emptyColumnNames)) {
          cleanedFormatData.emptyColumnNames = [];
        }

        try {
          const result = await db
            .collection("Formats")
            .updateOne(
              { _id: new ObjectId(formatId) },
              { $set: cleanedFormatData }
            );

          if (result.matchedCount === 0) {
            console.warn(`No format found with ID: ${formatId}`);
            return res.status(404).send({ message: "Format not found" });
          }

          console.log(
            `Updated format ID: ${formatId}, Name: ${cleanedFormatData.name}`
          );
          const updatedFormat = await db
            .collection("Formats")
            .findOne({ _id: new ObjectId(formatId) });
          res.status(200).send({
            ...updatedFormat,
            _id: updatedFormat._id.toString(),
          });
        } catch (err) {
          if (err.code === 11000) {
            console.error(`Duplicate format name: ${formatData.name}`);
            const existingFormat = await db
              .collection("Formats")
              .findOne({ name: formatData.name });
            return res.status(400).send({
              message: `Format name "${formatData.name}" already exists`,
              _id: existingFormat._id.toString(),
            });
          }
          console.error("Failed to update format:", err.message);
          throw err;
        }
      } catch (error) {
        console.error("Error updating format:", error.message);
        res
          .status(500)
          .send({ message: "Error updating format", error: error.message });
      }
    });

    app.post("/api/upload", upload.array("files"), async (req, res) => {
      try {
        if (!req.files || req.files.length === 0) {
          return res.status(400).send({ message: "No files uploaded" });
        }
        const brand = req.body.brand;
        if (!brand) {
          return res.status(400).send({ message: "Brand is required" });
        }

        let totalInserted = 0;
        let totalDuplicates = 0;
        let totalSheets = 0;

        const filePromises = req.files.map(async (file) => {
          const filePath = file.path;
          const fileExtension = file.originalname
            .split(".")
            .pop()
            .toLowerCase();
          let data = [];
          let sheetCount = 1;

          try {
            if (fileExtension === "csv") {
              await new Promise((resolve, reject) => {
                const results = [];
                fs.createReadStream(filePath)
                  .pipe(csv())
                  .on("data", (row) => {
                    const cleanedRow = cleanObjectForSampleData(row);
                    if (Object.keys(cleanedRow).length > 0) {
                      results.push(cleanedRow);
                    }
                  })
                  .on("end", () => {
                    data = results;
                    resolve();
                  })
                  .on("error", reject);
              });
            } else if (fileExtension === "xlsx" || fileExtension === "xls") {
              const workbook = XLSX.readFile(filePath);
              const sheetNames = workbook.SheetNames;
              sheetCount = sheetNames.length;

              for (const sheetName of sheetNames) {
                const sheet = workbook.Sheets[sheetName];
                const rawData = XLSX.utils.sheet_to_json(sheet, {
                  defval: null,
                });
                const sheetData = rawData
                  .map(cleanObjectForSampleData)
                  .filter((row) => Object.keys(row).length > 0);
                console.log(
                  `Processed sheet: ${sheetName}, Rows: ${sheetData.length}`
                );
                data = data.concat(sheetData);
              }
            } else {
              throw new Error("Invalid file format");
            }

            if (data.length === 0) {
              console.warn("No data processed from file:", filePath);
              return { insertedCount: 0, duplicateCount: 0, sheetCount };
            }

            const keys = data
              .filter((record) => record.StoreName && record.Date)
              .map((record) => ({
                StoreName: record.StoreName,
                Date: record.Date,
              }));

            const existingRecords = await brandsDb
              .collection(brand)
              .find({
                $or: keys,
              })
              .project({ StoreName: 1, Date: 1 })
              .toArray();

            const existingKeys = new Set(
              existingRecords.map((r) => `${r.StoreName}|${r.Date}`)
            );

            const recordsToInsert = data.filter((record) => {
              if (!record.StoreName || !record.Date) {
                console.warn(
                  "Skipping record missing StoreName or Date:",
                  record
                );
                return false;
              }
              const key = `${record.StoreName}|${record.Date}`;
              return !existingKeys.has(key);
            });

            let insertedCount = 0;
            let duplicateCount = data.length - recordsToInsert.length;

            if (recordsToInsert.length > 0) {
              const result = await brandsDb
                .collection(brand)
                .insertMany(recordsToInsert, { ordered: false });
              insertedCount = result.insertedCount;
              console.log(
                `Insert result for file ${file.originalname}:`,
                insertedCount,
                "documents"
              );
            }

            return { insertedCount, duplicateCount, sheetCount };
          } finally {
            await fs
              .unlink(filePath)
              .catch((err) =>
                console.error(`Failed to delete file ${filePath}:`, err)
              );
          }
        });

        const results = await Promise.all(filePromises);

        totalInserted = results.reduce((sum, r) => sum + r.insertedCount, 0);
        totalDuplicates = results.reduce((sum, r) => sum + r.duplicateCount, 0);
        totalSheets = results.reduce((sum, r) => sum + r.sheetCount, 0);

        const response = {
          message:
            totalInserted > 0
              ? "Data uploaded successfully"
              : totalDuplicates > 0
              ? "No new data uploaded; all records were duplicates"
              : "No valid data to insert",
          insertedCount: totalInserted,
          duplicateCount: totalDuplicates,
          sheetCount: totalSheets,
          fileCount: req.files.length,
        };

        console.log("Upload response:", response);
        res.send(response);
      } catch (err) {
        console.error("Upload error:", err);
        res
          .status(500)
          .send({ message: "Error uploading files", error: err.message });
      }
    });

    app.post("/api/data", async (req, res) => {
      try {
        if (!brandsDb) {
          await connectToMongoDB();
          if (!brandsDb) throw new Error("Failed to reconnect to MongoDB");
        }

        const { startDate, endDate, brand } = req.body;
        if (!startDate || !endDate || !brand) {
          return res
            .status(400)
            .send({ message: "Start date, end date, and brand are required" });
        }

        console.log("Incoming startDate:", startDate);
        console.log("Incoming endDate:", endDate);
        console.log("Brand:", brand);

        const start = moment(
          startDate,
          ["MM-DD-YYYY", "M/D/YYYY", "MM/DD/YYYY", "YYYY-MM-DD", "M-D-YYYY"],
          true
        );
        const end = moment(
          endDate,
          ["MM-DD-YYYY", "M/D/YYYY", "MM/DD/YYYY", "YYYY-MM-DD", "M-D-YYYY"],
          true
        );

        if (!start.isValid() || !end.isValid()) {
          return res.status(400).send({
            message: "Invalid date format. Use MM-DD-YYYY (e.g., 03-01-2025)",
          });
        }

        const formattedStartDate = start.format("MM-DD-YYYY");
        const formattedEndDate = end.format("MM-DD-YYYY");

        console.log("Querying with formatted startDate:", formattedStartDate);
        console.log("Querying with formatted endDate:", formattedEndDate);

        const data = await brandsDb
          .collection(brand)
          .find({
            Date: {
              $gte: formattedStartDate,
              $lte: formattedEndDate,
            },
          })
          .toArray();

        console.log(
          "Found records for brand:",
          brand,
          "Count:",
          data.length,
          "Sample:",
          data.slice(0, 2)
        );

        if (data.length === 0) {
          const sample = await brandsDb
            .collection(brand)
            .find()
            .limit(5)
            .toArray();
          console.log("Sample data in DB for brand:", brand, sample);
          return res.send(data);
        }

        const finalColumns = [];
        if (data[0].hasOwnProperty("StoreName")) finalColumns.push("StoreName");
        if (data[0].hasOwnProperty("Date")) finalColumns.push("Date");
        const allColumns = Object.keys(data[0] || {}).filter(
          (col) => col !== "_id" && !finalColumns.includes(col)
        );
        finalColumns.push(...allColumns);

        const normalizedData = data.map((record) => {
          const normalizedRecord = { _id: record._id.toString() };
          finalColumns.forEach((col) => {
            normalizedRecord[col] =
              record[col] !== undefined ? record[col] : null;
          });
          return normalizedRecord;
        });

        console.log(
          "Normalized data sample for brand:",
          brand,
          normalizedData.slice(0, 2)
        );
        res.send(normalizedData);
      } catch (err) {
        console.error("Error fetching data:", err);
        res.status(500).send({ message: "Error fetching data" });
      }
    });

    app.post("/api/data-update", async (req, res) => {
      try {
        const { id, updates, brand } = req.body;
        if (!id || !updates || !brand) {
          console.error("Invalid update request:", { id, updates, brand });
          return res.status(400).send({
            message: "Invalid update request: Missing id, updates, or brand",
          });
        }

        console.log("Received update request:", { id, updates, brand });

        let objectId;
        try {
          objectId = new ObjectId(id);
        } catch (err) {
          console.error("Invalid ObjectId:", id);
          return res.status(400).send({ message: "Invalid ObjectId format" });
        }

        const beforeUpdate = await brandsDb
          .collection(brand)
          .findOne({ _id: objectId });
        console.log("Record before update:", beforeUpdate);

        const cleanedUpdates = cleanObjectForSampleData(updates);
        console.log("Cleaned updates:", cleanedUpdates);

        // Create the $set stage, ensuring field names with dots are preserved
        const setStage = {
          $set: Object.keys(cleanedUpdates).reduce((acc, key) => {
            acc[key] = cleanedUpdates[key];
            return acc;
          }, {}),
        };

        const result = await brandsDb
          .collection(brand)
          .updateOne({ _id: objectId }, [setStage]);

        console.log("Update result:", {
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
          acknowledged: result.acknowledged,
        });

        const afterUpdate = await brandsDb
          .collection(brand)
          .findOne({ _id: objectId });
        console.log("Record after update:", afterUpdate);

        if (result.matchedCount === 0) {
          console.warn("No record found for ID:", id);
          return res.status(404).send({ message: "Record not found" });
        }

        if (result.modifiedCount === 0) {
          console.warn("No changes applied for ID:", id);
          return res.status(200).send({
            message: "No changes applied to the record",
            before: beforeUpdate,
            after: afterUpdate,
          });
        }

        res.send({
          message: "Data updated successfully",
          modifiedCount: result.modifiedCount,
          before: beforeUpdate,
          after: afterUpdate,
        });
      } catch (err) {
        console.error("Error updating data:", err);
        res
          .status(500)
          .send({ message: "Error updating data", error: err.message });
      }
    });

    app.post("/api/data-bulk-update", async (req, res) => {
      try {
        const { updates, brand } = req.body;
        if (
          !updates ||
          !Array.isArray(updates) ||
          updates.length === 0 ||
          !brand
        ) {
          console.error("Invalid bulk update request:", { updates, brand });
          return res.status(400).send({
            message:
              "Invalid bulk update request: Missing or invalid updates or brand",
          });
        }

        console.log("Received bulk update request:", {
          updateCount: updates.length,
          brand,
        });

        const bulkOperations = updates
          .map(({ id, updates }) => {
            let objectId;
            try {
              objectId = new ObjectId(id);
            } catch (err) {
              console.warn("Invalid ObjectId skipped:", id);
              return null;
            }

            const cleanedUpdates = cleanObjectForSampleData(updates);
            console.log("Cleaned updates for ID:", { id, cleanedUpdates });

            return {
              updateOne: {
                filter: { _id: objectId },
                update: {
                  $set: Object.keys(cleanedUpdates).reduce((acc, key) => {
                    acc[key] = cleanedUpdates[key];
                    return acc;
                  }, {}),
                },
              },
            };
          })
          .filter((op) => op !== null);

        if (bulkOperations.length === 0) {
          console.warn("No valid operations to process");
          return res
            .status(400)
            .send({ message: "No valid updates to process" });
        }

        const result = await brandsDb
          .collection(brand)
          .bulkWrite(bulkOperations, { ordered: false });

        console.log("Bulk update result:", {
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
          acknowledged: result.acknowledged,
        });

        if (result.matchedCount === 0) {
          console.warn("No records found for bulk update");
          return res.status(404).send({ message: "No records found" });
        }

        res.send({
          message: "Bulk data updated successfully",
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
        });
      } catch (err) {
        console.error("Error during bulk update:", err);
        res
          .status(500)
          .send({ message: "Error updating data", error: err.message });
      }
    });

    app.post("/api/data-calculated-column", async (req, res) => {
      try {
        const { column, updates, isNewColumn, brand } = req.body;
        if (!column || !updates || updates.length === 0 || !brand) {
          console.error("Invalid calculated column request:", {
            column,
            updates,
            brand,
          });
          return res.status(400).send({
            message: "Invalid request: Missing column, updates, or brand",
          });
        }

        console.log("Received calculated column request:", {
          column,
          updateCount: updates.length,
          isNewColumn,
          brand,
        });

        let modifiedCount = 0;
        for (const update of updates) {
          const { _id, value } = update;
          try {
            const objectId = new ObjectId(_id);
            const updateOperation = isNewColumn
              ? { $set: { [column]: value } }
              : { $set: { [column]: value } };

            const result = await brandsDb
              .collection(brand)
              .updateOne({ _id: objectId }, updateOperation);

            if (result.matchedCount === 0) {
              console.warn(`No record found for ID: ${_id}`);
              continue;
            }

            modifiedCount += result.modifiedCount;
          } catch (err) {
            console.error(`Error processing update for ID ${_id}:`, err);
            continue;
          }
        }

        console.log("Calculated column update result:", {
          modifiedCount,
          totalUpdates: updates.length,
        });

        if (modifiedCount === 0) {
          return res.status(200).send({
            message: "No records were modified",
            modifiedCount,
          });
        }

        res.send({
          message: isNewColumn
            ? `New column '${column}' added successfully`
            : `Column '${column}' updated successfully`,
          modifiedCount,
        });
      } catch (err) {
        console.error("Error processing calculated column:", err);
        res.status(500).send({
          message: "Error processing calculated column",
          error: err.message,
        });
      }
    });

    app.post(
      "/api/bank-mapping-upload",
      upload.single("file"),
      async (req, res) => {
        try {
          if (!req.file) {
            return res.status(400).send({ message: "No file uploaded" });
          }

          const filePath = req.file.path;
          const fileExtension = req.file.originalname
            .split(".")
            .pop()
            .toLowerCase();
          let data = [];

          if (fileExtension === "csv") {
            await new Promise((resolve, reject) => {
              fs.createReadStream(filePath)
                .pipe(csv())
                .on("data", (row) => {
                  data.push(cleanObjectForBMData(row));
                })
                .on("end", resolve)
                .on("error", reject);
            });
          } else if (fileExtension === "xlsx" || fileExtension === "xls") {
            const workbook = XLSX.readFile(filePath);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const rawData = XLSX.utils.sheet_to_json(sheet);
            data = rawData.map(cleanObjectForBMData);
          } else {
            return res.status(400).send({ message: "Invalid file format" });
          }

          if (data.length === 0) {
            console.warn("No data processed from file:", filePath);
            return res
              .status(400)
              .send({ message: "No valid data found in file" });
          }

          console.log("Sample bank mapping data:", data.slice(0, 2));
          const result = await db.collection("BMData").insertMany(data);
          console.log(
            "Insert result for BMData:",
            result.insertedCount,
            "documents"
          );

          await fs.unlink(filePath);
          res.send({
            message: "Bank mapping data uploaded successfully",
            insertedCount: result.insertedCount,
          });
        } catch (err) {
          console.error("Bank mapping upload error:", err);
          res.status(500).send({
            message: "Error uploading bank mapping file",
            error: err.message,
          });
        }
      }
    );

    app.post("/api/bank-mapping-data", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db)
            return res.status(503).send({ message: "Database not connected" });
        }

        const body = req.body || {};
        const { state = [], posName = [], brand = [] } = body;

        let data = await db
          .collection("BMData")
          .aggregate([
            {
              $match: {
                ...(state.length > 0 && { State: { $in: state } }),
                ...(posName.length > 0 && { POSNAME: { $in: posName } }),
                ...(brand.length > 0 && { BRAND: { $in: brand } }),
              },
            },
          ])
          .toArray();

        console.log("Found bank mapping records:", data.length);

        if (data.length === 0) {
          const sample = await db
            .collection("BMData")
            .find()
            .limit(5)
            .toArray();
          console.log("Sample bank mapping data in DB:", sample);
          return res.send([]);
        }

        const finalColumns = ["_id", "store_name", "mapped_col_name"];
        const allColumns = Object.keys(data[0] || {}).filter(
          (col) =>
            col !== "_id" &&
            col !== "store_name" &&
            col !== "mapped_col_name" &&
            !finalColumns.includes(col)
        );
        finalColumns.push(...allColumns);

        const normalizedData = data.map((record) => {
          const normalizedRecord = { _id: record._id.toString() };
          finalColumns.forEach((col) => {
            if (col !== "_id") {
              normalizedRecord[col] =
                record[col] !== undefined ? record[col] : null;
            }
          });
          return normalizedRecord;
        });

        console.log(
          "Normalized bank mapping data sample:",
          normalizedData.slice(0, 2)
        );
        res.send(normalizedData);
      } catch (err) {
        console.error("Error fetching bank mapping data:", err);
        res.status(500).send({ message: "Error fetching bank mapping data" });
      }
    });

    app.post("/api/bank-mapping-delete", async (req, res) => {
      try {
        const { ids } = req.body;
        if (!ids || !Array.isArray(ids) || ids.length === 0) {
          return res.status(400).send({ message: "No valid IDs provided" });
        }

        console.log("Received IDs for deletion:", ids);
        const objectIds = ids.map((id) => new ObjectId(id));
        const result = await db
          .collection("BMData")
          .deleteMany({ _id: { $in: objectIds } });
        console.log("Delete result:", result);
        res.send({
          message: "Data deleted successfully",
          deletedCount: result.deletedCount,
        });
      } catch (err) {
        console.error("Error deleting data:", err);
        res.status(500).send({ message: "Error deleting data" });
      }
    });

    app.post("/api/bank-mapping-update", async (req, res) => {
      try {
        const { id, updates } = req.body;
        if (!id || !updates) {
          return res.status(400).send({ message: "Invalid update request" });
        }

        const cleanedUpdates = cleanObjectForBMData(updates);
        const result = await db
          .collection("BMData")
          .updateOne({ _id: new ObjectId(id) }, { $set: cleanedUpdates });
        if (result.matchedCount === 0) {
          return res.status(404).send({ message: "Record not found" });
        }
        res.send({ message: "Data updated successfully" });
      } catch (err) {
        console.error("Error updating data:", err);
        res.status(500).send({ message: "Error updating data" });
      }
    });

    app.listen(port, () => {
      console.log(`Server listening on port ${port}`);
    });
  })
  .catch((err) => {
    console.error("Failed to start server:", err);
    process.exit(1);
  });

process.on("SIGINT", async () => {
  if (mongoClient) {
    await mongoClient.close();
    console.log("MongoDB connection closed");
  }
  process.exit(0);
});
