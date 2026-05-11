import express from "express";
import multer from "multer";
import { MongoClient, ObjectId } from "mongodb";
import csv from "csv-parser";
import fs from "fs";
import XLSX from "xlsx";
import moment from "moment";
import { evaluate } from "mathjs";
import { v4 as uuidv4 } from "uuid";
import dotenv from "dotenv";
import archiver from "archiver";
import axios from "axios";
dotenv.config();

const app = express();
app.use(express.json());
const port = 3001;

// CORS configuration
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept",
  );
  if (req.method === "OPTIONS") {
    res.header("Access-Control-Allow-Methods", "PUT, POST, PATCH, DELETE, GET");
    return res.status(200).json({});
  }
  next();
});

const mongoUrl = process.env.VITE_MONGO_URL;
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
let transactionsRef;
let mongoClient;

// Google Sheets Logging Function
const logToGoogleSheet = async (logData) => {
  const webhookUrl = process.env.VITE_GOOGLE_SHEET_WEBHOOK;
  if (!webhookUrl) return;
  try {
    await axios.post(webhookUrl, logData);
  } catch (err) {
    console.error("Error logging to Google Sheet:", err.message);
  }
};

async function connectToMongoDB() {
  try {
    if (
      mongoClient &&
      mongoClient.topology &&
      mongoClient.topology.isConnected()
    ) {
      if (!db) db = mongoClient.db(dbName);
      if (!brandsDb) brandsDb = mongoClient.db(brandsDbName);
      if (!transactionsRef) transactionsRef = mongoClient.db("Transactions");
      return mongoClient;
    }

    mongoClient = await MongoClient.connect(mongoUrl, {
      connectTimeoutMS: 30000,
      socketTimeoutMS: 30000,
      maxPoolSize: 50,
    });

    db = mongoClient.db(dbName);
    brandsDb = mongoClient.db(brandsDbName);
    transactionsRef = mongoClient.db("Transactions");

    await db.collection("SampleData").createIndex({ Date: 1 });
    await db.collection("SampleData").createIndex({ StoreName: 1, Date: 1 });
    await db.collection("BMData").createIndex({ Name: 1 });
    await db.collection("Formats").createIndex({ name: 1 }, { unique: true });
    await db
      .collection("Formulas")
      .createIndex({ formula: 1, column: 1, brand: 1 }, { unique: true });

    return mongoClient;
  } catch (err) {
    console.error("Error connecting to MongoDB:", {
      message: err.message,
      stack: err.stack,
    });
    mongoClient = null;
    throw err;
  }
}

function cleanObjectForSampleData(obj) {
  return Object.keys(obj).reduce((acc, key) => {
    const cleanKey = key.trim().replace(/\s+/g, "").replace(/\./g, "_");
    let value = obj[key];

    if (typeof value === "string") {
      value = value.trim().replace(/\x00/g, "");
      if (cleanKey.toLowerCase() !== "date") {
        const parsedNumber = parseFloat(value.replace(/[^0-9.-]/g, ""));
        if (!isNaN(parsedNumber) && value.match(/^-?\d*\.?\d*$/)) {
          value = Number(parsedNumber.toFixed(2));
        }
      }
    } else if (typeof value === "number" && !isNaN(value)) {
      value = Number(value.toFixed(2));
    } else if (value === null || value === undefined) {
      value = null;
    }

    if (cleanKey.toLowerCase() === "date") {
      if (typeof value === "number") {
        const excelEpoch = new Date(Date.UTC(1899, 11, 31));
        const utcDate = new Date(excelEpoch.getTime() + (value - 1) * 86400000);
        value = moment.utc(utcDate).format("MM-DD-YYYY");
      } else if (value) {
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
          true,
        );
        if (parsedDate.isValid()) {
          value = parsedDate.format("MM-DD-YYYY");
        } else {
          value = null;
        }
      }
    }

    acc[cleanKey] = value;
    return acc;
  }, {});
}

async function initializeTransactionLog() {
  try {
    await db
      .collection("TransactionLog")
      .createIndex({ transactionId: 1 }, { unique: true });
    await db
      .collection("TransactionLog")
      .createIndex({ brand: 1, status: 1, createdAt: -1 });
    await db
      .collection("TransactionLog")
      .createIndex({ createdAt: 1 }, { expireAfterSeconds: 30 * 24 * 60 * 60 });
  } catch (err) {
    console.error("Error initializing TransactionLog:", err);
  }
}

function cleanObjectForBMData(obj) {
  const cleaned = Object.keys(obj).reduce((acc, key) => {
    const cleanKey = key.trim().replace(/\s+/g, "").replace(/\./g, "_");
    let value = obj[key];

    if (typeof value === "string") {
      value = value.trim().replace(/\x00/g, "");
      if (cleanKey.toLowerCase() === "storno") {
        acc[cleanKey] = value;
        return acc;
      }
    } else if (value instanceof Date) {
      value = moment(value).format("MM-DD-YYYY");
    } else if (typeof value === "number" && !isNaN(value)) {
      if (cleanKey.toLowerCase() === "date") {
        const date = new Date((value - 25569) * 86400 * 1000);
        value = moment(date).format("MM-DD-YYYY");
      } else {
        value = value.toString();
      }
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
    initializeTransactionLog();

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
        }

        const { brand } = req.query;

        const brands = await db.collection(brandsDbName).distinct("Brand");
        const states = await db.collection("BMData").distinct("State");

        const storeMappings = await db
          .collection("BMData")
          .find(
            {
              $or: [
                { StoreNo: { $exists: true, $ne: null } },
                { storeno: { $exists: true, $ne: null } },
                { Store_No: { $exists: true, $ne: null } },
              ],
            },
            {
              projection: {
                _id: 0,
                StoreNo: 1,
                storeno: 1,
                Store_No: 1,
                State: 1,
                state: 1,
              },
            },
          )
          .toArray();

        res.send({ brands, states, storeMappings });
      } catch (err) {
        console.error("Error fetching filter options:", err);
        res.status(500).send({ message: "Error fetching filter options" });
      }
    });

    app.get("/api/formulas", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db) throw new Error("Failed to connect to MongoDB");
        }
        const formulas = await db.collection("Formulas").find({}).toArray();
        res.status(200).send(
          formulas.map((formula) => ({
            ...formula,
            _id: formula._id.toString(),
          })),
        );
      } catch (err) {
        console.error("Error retrieving formulas:", err);
        res
          .status(500)
          .send({ message: "Error retrieving formulas", error: err.message });
      }
    });

    app.post("/api/formulas", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db) throw new Error("Failed to connect to MongoDB");
        }

        const { formula, column, selected, brand } = req.body;
        if (!formula || !column || !brand) {
          return res
            .status(400)
            .send({ message: "Formula, column, and brand are required" });
        }

        if (selected) {
          await db
            .collection("Formulas")
            .updateMany(
              { brand, selected: true },
              { $set: { selected: false } },
            );
        }

        const result = await db.collection("Formulas").insertOne({
          formula,
          column,
          selected: !!selected,
          brand,
          createdAt: new Date(),
        });

        res.status(201).send({
          _id: result.insertedId.toString(),
          formula,
          column,
          selected: !!selected,
          brand,
          createdAt: new Date(),
        });
      } catch (err) {
        console.error("Error creating formula:", err);
        if (err.code === 11000) {
          return res.status(400).send({
            message: "Formula for this column and brand already exists",
          });
        }
        res
          .status(500)
          .send({ message: "Error creating formula", error: err.message });
      }
    });

    app.put("/api/formulas/:id", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db) throw new Error("MongoDB connection failed");
        }

        const formulaId = req.params.id;
        const { selected, formula, column } = req.body;

        let updateData = {};
        if (selected !== undefined) updateData.selected = selected;
        if (formula !== undefined) updateData.formula = formula;
        if (column !== undefined) updateData.column = column;

        if (selected === true) {
          const existing = await db
            .collection("Formulas")
            .findOne({ _id: new ObjectId(formulaId) });
          if (!existing)
            return res.status(404).send({ message: "Formula not found" });

          await db.collection("Formulas").updateMany(
            {
              brand: existing.brand,
              selected: true,
              _id: { $ne: new ObjectId(formulaId) },
            },
            { $set: { selected: false } },
          );
        }

        const updateResult = await db
          .collection("Formulas")
          .updateOne({ _id: new ObjectId(formulaId) }, { $set: updateData });

        if (updateResult.matchedCount === 0) {
          return res.status(404).send({ message: "Formula not found" });
        }

        const updatedFormula = await db
          .collection("Formulas")
          .findOne({ _id: new ObjectId(formulaId) });
        res.status(200).send({
          ...updatedFormula,
          _id: updatedFormula._id.toString(),
        });
      } catch (err) {
        console.error("Error toggling/updating formula:", err);
        res
          .status(500)
          .send({ message: "Error updating formula", error: err.message });
      }
    });

    app.get("/api/formats", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db) throw new Error("Failed to connect to MongoDB");
        }

        const formats = await db.collection("Formats").find({}).toArray();
        res.status(200).send(
          formats.map((format) => ({
            ...format,
            _id: format._id.toString(),
          })),
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
            }),
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

    app.post(
      "/api/generate-cheque-sheet",
      upload.single("file"),
      async (req, res) => {
        try {
          if (!req.file) {
            return res.status(400).send({ message: "No file uploaded" });
          }

          if (!db) await connectToMongoDB();

          const bmData = await db
            .collection("BMData")
            .find({
              Cashpro: { $regex: /^yes$/i },
            })
            .toArray();

          const validAccounts = new Set();

          const cleanBankAccount = (accStr) => {
            if (!accStr) return "";
            let cleaned = String(accStr)
              .replace(/Acc No\s*:/i, "")
              .trim();
            if (cleaned.includes(".")) {
              cleaned = cleaned.split(".")[0];
            }
            return cleaned.replace(/[^a-zA-Z0-9]/g, "");
          };

          bmData.forEach((doc) => {
            if (doc.BankAccountNo1)
              validAccounts.add(cleanBankAccount(doc.BankAccountNo1));
            if (doc.BankAccountNo2)
              validAccounts.add(cleanBankAccount(doc.BankAccountNo2));
            if (doc.BankAccountNo3)
              validAccounts.add(cleanBankAccount(doc.BankAccountNo3));
            if (doc.BankAccountNo4)
              validAccounts.add(cleanBankAccount(doc.BankAccountNo4));
          });

          const fileContent = fs.readFileSync(req.file.path, "utf8");
          const lines = fileContent.split(/\r?\n/);

          const outputRows = [];
          const logRows = [];

          logRows.push(
            "Account Number,Check Number,Amount,Issue Date,Indicator,Payee,Reason Removed",
          );

          for (const line of lines) {
            if (!line.trim()) continue;

            const row = line.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/);

            if (row[0] && row[0].includes("Account Number")) continue;

            let rawAccountNum = row[0]
              ? String(row[0]).replace(/['"]/g, "").trim()
              : "";
            if (rawAccountNum.includes(".")) {
              rawAccountNum = rawAccountNum.split(".")[0];
            }
            const accountNum = rawAccountNum.replace(/[^a-zA-Z0-9]/g, "");

            const checkNum = row[1] ? row[1].replace(/['"]/g, "").trim() : "";

            const amount = row[2] ? row[2].replace(/['"]/g, "").trim() : "";

            let dateStr = row[3] ? row[3].replace(/['"]/g, "").trim() : "";
            const parsedDate = moment(
              dateStr,
              [
                "MM-DD-YYYY",
                "MM/DD/YYYY",
                "YYYY-MM-DD",
                "M/D/YYYY",
                "MM/DD/YY",
                "M/D/YY",
                "YYYY/MM/DD",
              ],
              true,
            );
            const formattedDate = parsedDate.isValid()
              ? parsedDate.format("MM/DD/YYYY")
              : dateStr;

            const indicator = row[4] ? row[4].replace(/['"]/g, "").trim() : "";
            const payee = row[5] ? row[5].trim() : "";

            const cleanedRowString = `${accountNum},${checkNum},${amount},${formattedDate},${indicator},${payee}`;

            if (validAccounts.has(accountNum)) {
              outputRows.push(cleanedRowString);
            } else {
              logRows.push(
                `${cleanedRowString},Account not matched in BMData or Cashpro != Yes`,
              );
            }
          }

          await fs.promises.unlink(req.file.path).catch(console.error);

          const archive = archiver("zip", { zlib: { level: 9 } });

          res.setHeader("Content-Type", "application/zip");
          res.setHeader(
            "Content-Disposition",
            'attachment; filename="ChequeSheets.zip"',
          );

          archive.pipe(res);
          archive.append(outputRows.join("\n"), { name: "output.csv" });
          archive.append(logRows.join("\n"), { name: "removed_log.csv" });

          await archive.finalize();
        } catch (err) {
          console.error("Error generating cheque sheets:", err);
          res
            .status(500)
            .send({ message: "Error generating sheets", error: err.message });
        }
      },
    );

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
              { $set: cleanedFormatData },
            );

          if (result.matchedCount === 0) {
            console.warn(`No format found with ID: ${formatId}`);
            return res.status(404).send({ message: "Format not found" });
          }

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

        if (!brandsDb) await connectToMongoDB();

        await brandsDb.collection(brand).createIndex({ StoreName: 1, Date: 1 });

        let totalInserted = 0;
        let totalDuplicates = 0;
        let totalSheets = 0;

        const BATCH_SIZE = 1000;

        for (const file of req.files) {
          const filePath = file.path;
          const fileExtension = file.originalname
            .split(".")
            .pop()
            .toLowerCase();

          let fileData = [];
          let sheetCount = 0;

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
                    fileData = results;
                    sheetCount = 1;
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

                fileData = fileData.concat(sheetData);
              }
            } else {
              throw new Error("Invalid file format");
            }

            if (fileData.length === 0) {
              console.warn("No data processed from file:", filePath);
              continue;
            }

            for (let i = 0; i < fileData.length; i += BATCH_SIZE) {
              const batch = fileData.slice(i, i + BATCH_SIZE);

              const keys = batch
                .filter((record) => record.StoreName && record.Date)
                .map((record) => ({
                  StoreName: record.StoreName,
                  Date: record.Date,
                }));

              if (keys.length === 0) continue;

              const existingRecords = await brandsDb
                .collection(brand)
                .find({ $or: keys })
                .project({ StoreName: 1, Date: 1 })
                .toArray();

              const existingKeys = new Set(
                existingRecords.map((r) => `${r.StoreName}|${r.Date}`),
              );

              const recordsToInsert = batch.filter((record) => {
                if (!record.StoreName || !record.Date) return false;
                const key = `${record.StoreName}|${record.Date}`;
                return !existingKeys.has(key);
              });

              if (recordsToInsert.length > 0) {
                const result = await brandsDb
                  .collection(brand)
                  .insertMany(recordsToInsert, { ordered: false });
                totalInserted += result.insertedCount;
              }

              totalDuplicates += batch.length - recordsToInsert.length;
              await new Promise((resolve) => setTimeout(resolve, 5));
            }

            totalSheets += sheetCount;
          } catch (err) {
            console.error(`Error processing file ${file.originalname}:`, err);
          } finally {
            await fs.promises
              .unlink(filePath)
              .catch((err) =>
                console.error(`Failed to delete file ${filePath}:`, err),
              );
          }
        }

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
          return res.status(400).json({
            success: false,
            message: "Start date, end date, and brand are required",
          });
        }

        const start = moment(
          startDate.trim(),
          [
            "MM-DD-YYYY",
            "M/D/YYYY",
            "MM/DD/YYYY",
            "YYYY-MM-DD",
            "M-D-YYYY",
            "MM.DD.YYYY",
            "DD-MM-YYYY",
            "D/M/YYYY",
          ],
          true,
        );

        const end = moment(
          endDate.trim(),
          [
            "MM-DD-YYYY",
            "M/D/YYYY",
            "MM/DD/YYYY",
            "YYYY-MM-DD",
            "M-D-YYYY",
            "MM.DD.YYYY",
            "DD-MM-YYYY",
            "D/M/YYYY",
          ],
          true,
        );

        if (!start.isValid() || !end.isValid()) {
          return res.status(400).json({
            success: false,
            message:
              "Invalid date format. Please use MM-DD-YYYY (e.g., 03-22-2026)",
          });
        }

        if (end.isBefore(start)) {
          return res.status(400).json({
            success: false,
            message: "End date cannot be before start date",
          });
        }

        const startOfDay = start.startOf("day").toDate();
        const endOfDay = end.endOf("day").toDate();

        const pipeline = [
          {
            $addFields: {
              __parsedDate: {
                $dateFromString: {
                  dateString: "$Date",
                  format: "%m-%d-%Y",
                  onError: null,
                  onNull: null,
                },
              },
            },
          },
          {
            $match: {
              __parsedDate: { $ne: null },
              __parsedDate: {
                $gte: startOfDay,
                $lte: endOfDay,
              },
            },
          },
          {
            $project: {
              __parsedDate: 0,
            },
          },
        ];

        const data = await brandsDb
          .collection(brand)
          .aggregate(pipeline)
          .toArray();

        if (data.length === 0) {
          return res.status(200).json({
            success: true,
            data: [],
            message: "No records found in selected date range",
          });
        }

        const allColumns = Array.from(
          new Set(data.flatMap((record) => Object.keys(record))),
        ).filter((col) => col !== "_id");

        const priorityColumns = ["StoreName", "Date"];
        const finalColumns = [
          ...priorityColumns.filter((col) => allColumns.includes(col)),
          ...allColumns.filter((col) => !priorityColumns.includes(col)),
        ];

        const normalizedData = data.map((record) => {
          const normalizedRecord = { _id: record._id.toString() };
          finalColumns.forEach((col) => {
            normalizedRecord[col] =
              record[col] !== undefined && record[col] !== null
                ? record[col]
                : null;
          });
          return normalizedRecord;
        });

        res.status(200).json({
          success: true,
          data: normalizedData,
          count: normalizedData.length,
        });
      } catch (err) {
        console.error("[ /api/data ] Error:", err);
        res.status(500).json({
          success: false,
          message: "Internal server error while fetching data",
          error: err.message,
        });
      }
    });

    app.post("/api/data-update", async (req, res) => {
      try {
        const { id, updates, brand } = req.body;
        if (!id || !updates || !brand) {
          return res.status(400).send({
            message: "Invalid update request: Missing id, updates, or brand",
          });
        }

        let objectId = new ObjectId(id);
        const result = await brandsDb
          .collection(brand)
          .updateOne({ _id: objectId }, [
            { $set: cleanObjectForSampleData(updates) },
          ]);

        if (result.matchedCount === 0)
          return res.status(404).send({ message: "Record not found" });

        res.send({
          message: "Data updated successfully",
          modifiedCount: result.modifiedCount,
        });
      } catch (err) {
        res
          .status(500)
          .send({ message: "Error updating data", error: err.message });
      }
    });

    app.post("/api/data-bulk-update", async (req, res) => {
      try {
        const { updates, brand, username } = req.body; // username from frontend
        if (
          !updates ||
          !Array.isArray(updates) ||
          updates.length === 0 ||
          !brand
        ) {
          return res
            .status(400)
            .send({ message: "Invalid bulk update request" });
        }

        const activeFormula = await db
          .collection("Formulas")
          .findOne({ brand, selected: true });
        const formulaColumnsCache = new Map();
        const transactionId = uuidv4();
        const transactionLog = {
          transactionId,
          brand,
          operation: "bulk-update",
          status: "active",
          updates: [],
          createdAt: new Date(),
        };

        const bulkOperations = await Promise.all(
          updates.map(async ({ id, updates: newUpdates }) => {
            let objectId = new ObjectId(id);
            const cleanedUpdates = cleanObjectForSampleData(newUpdates);
            const originalRecord = await brandsDb
              .collection(brand)
              .findOne({ _id: objectId });
            if (!originalRecord) return null;

            // LOGGING LOGIC START
            for (const colName in cleanedUpdates) {
              const oldValue = originalRecord[colName];
              const newValue = cleanedUpdates[colName];
              if (String(oldValue) !== String(newValue)) {
                await logToGoogleSheet({
                  username: username || "System",
                  brand: brand,
                  recordId: id,
                  columnName: colName,
                  oldValue: oldValue ?? "NULL",
                  newValue: newValue,
                  date: originalRecord.Date,
                  store: originalRecord.StoreName,
                });
              }
            }
            // LOGGING LOGIC END

            let additionalUpdates = {};
            if (activeFormula) {
              const { formula, column } = activeFormula;
              let expression = formula;
              let resultIsString = false;
              let formulaColumns = formulaColumnsCache.get(formula);
              if (!formulaColumns) {
                formulaColumns = [];
                Object.keys(originalRecord).forEach((key) => {
                  const escapedKey = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
                  const regex = new RegExp(
                    `(^|[\\s+\\-*/%\\(])\\s*(${escapedKey})\\s*([\\s+\\-*/%\\)]|$)`,
                    "gi",
                  );
                  if (regex.test(formula)) formulaColumns.push({ key, regex });
                });
                formulaColumnsCache.set(formula, formulaColumns);
              }

              formulaColumns.forEach(({ key }) => {
                let cellValue =
                  key in cleanedUpdates
                    ? cleanedUpdates[key]
                    : originalRecord[key];
                cellValue =
                  cellValue === null || cellValue === "" ? 0 : cellValue;
                if (typeof cellValue === "string") {
                  const parsed = parseFloat(cellValue.replace(/[^0-9.-]/g, ""));
                  if (isNaN(parsed)) {
                    resultIsString = true;
                    cellValue = `"${cellValue}"`;
                  } else cellValue = parsed;
                }
                const escapedKey = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
                const regex = new RegExp(
                  `(^|[\\s+\\-*/%\\(])\\s*(${escapedKey})\\s*([\\s+\\-*/%\\)]|$)`,
                  "g",
                );
                expression = expression.replace(regex, `$1${cellValue}$3`);
              });

              try {
                let result = resultIsString
                  ? expression.replace(/"/g, "")
                  : Number(parseFloat(evaluate(expression)).toFixed(2));
                additionalUpdates[column] = result;
              } catch (e) {
                additionalUpdates[column] = 0;
              }
            }

            const allUpdates = { ...cleanedUpdates, ...additionalUpdates };
            transactionLog.updates.push({
              id,
              originalValues: originalRecord,
              newValues: allUpdates,
            });

            return {
              updateOne: {
                filter: { _id: objectId },
                update: { $set: allUpdates },
              },
            };
          }),
        );

        await db.collection("TransactionLog").insertOne(transactionLog);
        const result = await brandsDb
          .collection(brand)
          .bulkWrite(bulkOperations.filter((op) => op !== null));
        res.send({
          message: "Bulk update completed",
          modifiedCount: result.modifiedCount,
          transactionId,
        });
      } catch (err) {
        res.status(500).send({
          message: "Error performing bulk update",
          error: err.message,
        });
      }
    });

    app.post("/api/clear-transaction-logs", async (req, res) => {
      try {
        const { brand } = req.body;
        const result = await db
          .collection("TransactionLog")
          .deleteMany({ brand });
        res.send({
          message: `Cleared ${result.deletedCount} logs`,
          deletedCount: result.deletedCount,
        });
      } catch (err) {
        res.status(500).send({ message: "Error clearing logs" });
      }
    });

    app.post("/api/data-calculated-column", async (req, res) => {
      try {
        const { column, updates, isNewColumn, brand, username } = req.body;
        const transactionId = uuidv4();
        const transactionLog = {
          transactionId,
          brand,
          operation: "calculated-column",
          status: "active",
          updates: [],
          column,
          isNewColumn,
          createdAt: new Date(),
        };

        for (const update of updates) {
          const { _id, value } = update;
          const objectId = new ObjectId(_id);
          const originalRecord = await brandsDb
            .collection(brand)
            .findOne(
              { _id: objectId },
              { projection: { [column]: 1, Date: 1, StoreName: 1 } },
            );
          const originalValue = originalRecord ? originalRecord[column] : null;

          // Log the calculation
          await logToGoogleSheet({
            username: username || "System",
            brand: brand,
            recordId: _id,
            columnName: column,
            oldValue: originalValue ?? "NULL",
            newValue: value,
            date: originalRecord?.Date,
            store: originalRecord?.StoreName,
          });

          transactionLog.updates.push({
            id: _id,
            originalValues: { [column]: originalValue },
            newValues: { [column]: value },
          });
          await brandsDb
            .collection(brand)
            .updateOne({ _id: objectId }, { $set: { [column]: value } });
        }

        await db.collection("TransactionLog").insertOne(transactionLog);
        res.send({ message: `Column '${column}' processed`, transactionId });
      } catch (err) {
        res.status(500).send({ message: "Error processing calculated column" });
      }
    });

    app.post("/api/undo", async (req, res) => {
      try {
        const { brand, username } = req.body; // username from frontend
        if (!brand)
          return res.status(400).send({ message: "Brand is required" });

        const transaction = await db
          .collection("TransactionLog")
          .findOne({ brand, status: "active" }, { sort: { createdAt: -1 } });

        if (!transaction)
          return res.status(404).send({ message: "No actions to undo" });

        const bulkOps = [];
        for (const update of transaction.updates) {
          const { id, originalValues } = update;
          const objectId = new ObjectId(id);

          // Fetch current state before reverting to log the change
          const currentState = await brandsDb
            .collection(brand)
            .findOne({ _id: objectId });

          if (currentState) {
            for (const key in originalValues) {
              const oldValue = currentState[key];
              const newValue = originalValues[key];

              if (String(oldValue) !== String(newValue)) {
                await logToGoogleSheet({
                  username: username || "System (Undo)",
                  brand: brand,
                  recordId: id,
                  columnName: key + " (UNDO)", // Mark as undo
                  oldValue: oldValue ?? "NULL",
                  newValue: newValue ?? "NULL",
                  date: currentState.Date,
                  store: currentState.StoreName,
                });
              }
            }
          }

          bulkOps.push({
            updateOne: {
              filter: { _id: objectId },
              update: { $set: originalValues },
            },
          });
        }

        const result = await brandsDb.collection(brand).bulkWrite(bulkOps);
        await db
          .collection("TransactionLog")
          .updateOne(
            { transactionId: transaction.transactionId },
            { $set: { status: "undone" } },
          );
        res.send({
          message: "Undo completed and logged",
          modifiedCount: result.modifiedCount,
        });
      } catch (err) {
        res.status(500).send({ message: "Error performing undo" });
      }
    });

    app.post("/api/redo", async (req, res) => {
      try {
        const { brand, username } = req.body;
        const transaction = await db
          .collection("TransactionLog")
          .findOne({ brand, status: "undone" }, { sort: { createdAt: -1 } });

        if (!transaction)
          return res.status(404).send({ message: "No actions to redo" });

        const bulkOps = [];
        for (const update of transaction.updates) {
          const { id, newValues } = update;
          const objectId = new ObjectId(id);

          const currentState = await brandsDb
            .collection(brand)
            .findOne({ _id: objectId });

          if (currentState) {
            for (const key in newValues) {
              const oldValue = currentState[key];
              const newValue = newValues[key];

              if (String(oldValue) !== String(newValue)) {
                await logToGoogleSheet({
                  username: username || "System (Redo)",
                  brand: brand,
                  recordId: id,
                  columnName: key + " (REDO)", // Mark as redo
                  oldValue: oldValue ?? "NULL",
                  newValue: newValue ?? "NULL",
                  date: currentState.Date,
                  store: currentState.StoreName,
                });
              }
            }
          }

          bulkOps.push({
            updateOne: {
              filter: { _id: objectId },
              update: { $set: newValues },
            },
          });
        }

        const result = await brandsDb.collection(brand).bulkWrite(bulkOps);
        await db
          .collection("TransactionLog")
          .updateOne(
            { transactionId: transaction.transactionId },
            { $set: { status: "active" } },
          );
        res.send({
          message: "Redo completed and logged",
          modifiedCount: result.modifiedCount,
        });
      } catch (err) {
        res.status(500).send({ message: "Error performing redo" });
      }
    });

    app.post("/api/check-transactions", async (req, res) => {
      try {
        const { brand } = req.body;
        const canUndo = await db
          .collection("TransactionLog")
          .findOne({ brand, status: "active" }, { sort: { createdAt: -1 } });
        const canRedo = await db
          .collection("TransactionLog")
          .findOne({ brand, status: "undone" }, { sort: { createdAt: -1 } });
        res.send({ canUndo: !!canUndo, canRedo: !!canRedo });
      } catch (err) {
        res.status(500).send({ message: "Error checking transactions" });
      }
    });

    app.post(
      "/api/bank-mapping-upload",
      upload.single("file"),
      async (req, res) => {
        try {
          if (!req.file)
            return res.status(400).send({ message: "No file uploaded" });
          const filePath = req.file.path;
          const fileExtension = req.file.originalname
            .split(".")
            .pop()
            .toLowerCase();
          let data = [];
          let headers = [];

          if (fileExtension === "csv") {
            await new Promise((resolve, reject) => {
              const results = [];
              fs.createReadStream(filePath)
                .pipe(csv())
                .on("headers", (h) => {
                  headers = h.map((header) =>
                    header.trim().replace(/\s+/g, "").replace(/\./g, "_"),
                  );
                })
                .on("data", (row) => results.push(cleanObjectForBMData(row)))
                .on("end", resolve)
                .on("error", reject);
              data = results;
            });
          } else {
            const workbook = XLSX.readFile(filePath);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const rawData = XLSX.utils.sheet_to_json(sheet, { header: 1 });
            headers = rawData[0].map((h) =>
              h
                ? h.toString().trim().replace(/\s+/g, "").replace(/\./g, "_")
                : "",
            );
            data = rawData.slice(1).map((row) => {
              const rowData = {};
              headers.forEach(
                (h, i) => (rowData[h] = row[i] !== undefined ? row[i] : null),
              );
              return cleanObjectForBMData(rowData);
            });
          }
          const result = await db.collection("BMData").insertMany(data);
          await db
            .collection("BMDataHeaders")
            .updateOne(
              { _id: "headerOrder" },
              { $set: { headers } },
              { upsert: true },
            );
          await fs.promises.unlink(filePath);
          res.send({
            message: "Bank mapping data uploaded",
            insertedCount: result.insertedCount,
          });
        } catch (err) {
          res.status(500).send({ message: "Error uploading bank mapping" });
        }
      },
    );

    app.post("/api/bank-mapping-data", async (req, res) => {
      try {
        const { state = [], posName = [], brand = [] } = req.body || {};
        let data = await db
          .collection("BMData")
          .aggregate([
            {
              $match: {
                ...(state.length > 0 && { State: { $in: state } }),
                ...(posName.length > 0 && { Name: { $in: posName } }),
                ...(brand.length > 0 && { Brand: { $in: brand } }),
              },
            },
          ])
          .toArray();
        if (data.length === 0) return res.send([]);
        const headerDoc = await db
          .collection("BMDataHeaders")
          .findOne({ _id: "headerOrder" });
        const headers = headerDoc ? headerDoc.headers : [];
        const allColumns =
          headers.length > 0
            ? headers
            : Object.keys(data[0]).filter((col) => col !== "_id");
        res.send(
          data.map((record) => {
            const normalized = { _id: record._id.toString() };
            allColumns.forEach(
              (col) =>
                (normalized[col] =
                  record[col] !== undefined ? record[col] : null),
            );
            return normalized;
          }),
        );
      } catch (err) {
        res.status(500).send({ message: "Error fetching mapping data" });
      }
    });

    app.post("/api/bank-mapping-delete", async (req, res) => {
      try {
        const { ids } = req.body;
        const result = await db
          .collection("BMData")
          .deleteMany({ _id: { $in: ids.map((id) => new ObjectId(id)) } });
        res.send({
          message: "Deleted successfully",
          deletedCount: result.deletedCount,
        });
      } catch (err) {
        res.status(500).send({ message: "Error deleting data" });
      }
    });

    app.post(
      "/api/store-data-upload",
      upload.single("file"),
      async (req, res) => {
        try {
          if (!req.file)
            return res.status(400).send({ message: "No file uploaded" });
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
                .on("data", (row) => data.push(cleanObjectForBMData(row)))
                .on("end", resolve)
                .on("error", reject);
            });
          } else {
            const workbook = XLSX.readFile(filePath);
            data = XLSX.utils
              .sheet_to_json(workbook.Sheets[workbook.SheetNames[0]])
              .map(cleanObjectForBMData);
          }
          const result = await db.collection("StoreData").insertMany(data);
          await fs.promises.unlink(filePath);
          res.send({
            message: "Store data uploaded",
            insertedCount: result.insertedCount,
          });
        } catch (err) {
          res.status(500).send({ message: "Error uploading store data" });
        }
      },
    );

    app.post("/api/store-data", async (req, res) => {
      try {
        const data = await db.collection("StoreData").find({}).toArray();
        if (data.length === 0) return res.send({ storeData: [] });
        const allColumns = Array.from(
          new Set(data.flatMap((record) => Object.keys(record))),
        ).filter((col) => col !== "_id");
        res.send({
          storeData: data.map((record) => {
            const normalized = { _id: record._id.toString() };
            allColumns.forEach(
              (col) =>
                (normalized[col] =
                  record[col] !== undefined ? record[col] : null),
            );
            return normalized;
          }),
        });
      } catch (err) {
        res.status(500).send({ message: "Error fetching store data" });
      }
    });

    app.post("/api/store-data-delete", async (req, res) => {
      try {
        const result = await db.collection("StoreData").deleteMany({
          _id: { $in: req.body.ids.map((id) => new ObjectId(id)) },
        });
        res.send({
          message: "Deleted successfully",
          deletedCount: result.deletedCount,
        });
      } catch (err) {
        res.status(500).send({ message: "Error deleting" });
      }
    });

    app.post("/api/store-data-add", async (req, res) => {
      try {
        const result = await db
          .collection("StoreData")
          .insertOne(cleanObjectForBMData(req.body.data));
        res.send({
          message: "Added successfully",
          insertedId: result.insertedId,
        });
      } catch (err) {
        res.status(500).send({ message: "Error adding" });
      }
    });

    app.post("/api/store-data-update", async (req, res) => {
      try {
        const result = await db
          .collection("StoreData")
          .updateOne(
            { _id: new ObjectId(req.body.id) },
            { $set: cleanObjectForBMData(req.body.updates) },
          );
        res.send({ message: "Updated successfully" });
      } catch (err) {
        res.status(500).send({ message: "Error updating" });
      }
    });

    app.post("/api/bank-mapping-add", async (req, res) => {
      try {
        const result = await db
          .collection("BMData")
          .insertOne(cleanObjectForBMData(req.body.data));
        res.send({
          message: "Added successfully",
          insertedId: result.insertedId,
        });
      } catch (err) {
        res.status(500).send({ message: "Error adding bank mapping" });
      }
    });

    app.post("/api/bank-mapping-update", async (req, res) => {
      try {
        const result = await db
          .collection("BMData")
          .updateOne(
            { _id: new ObjectId(req.body.id) },
            { $set: cleanObjectForBMData(req.body.updates) },
          );
        res.send({ message: "Updated successfully" });
      } catch (err) {
        res.status(500).send({ message: "Error updating bank mapping" });
      }
    });

    app.get("/api/bofa-accounts", async (req, res) => {
      try {
        if (!mongoClient) await connectToMongoDB();
        const transactionsDb = mongoClient.db("Transactions");
        const metadataDb = mongoClient.db("transactionsDB");
        let accounts = await transactionsDb
          .collection("bofa_transactions")
          .aggregate([
            {
              $group: {
                _id: "$accountNumber",
                accountName: { $first: "$accountName" },
              },
            },
            { $project: { _id: 0, accountNumber: "$_id", accountName: 1 } },
          ])
          .toArray();
        const mappings = await metadataDb
          .collection("BMData")
          .find({})
          .project({
            BankAccountNo1: 1,
            BankAccountNo2: 1,
            BankAccountNo3: 1,
            BankAccountNo4: 1,
            CompanyTaxName: 1,
          })
          .toArray();
        accounts = accounts.map((acc) => {
          if (acc.accountName) return acc;
          const found = mappings.find((m) =>
            [
              m.BankAccountNo1,
              m.BankAccountNo2,
              m.BankAccountNo3,
              m.BankAccountNo4,
            ].some((bn) => String(bn).includes(String(acc.accountNumber))),
          );
          return {
            accountNumber: acc.accountNumber,
            accountName: found ? found.CompanyTaxName : "Unknown Entity",
          };
        });
        res.send(
          accounts.sort((a, b) =>
            (a.accountName || "").localeCompare(b.accountName || ""),
          ),
        );
      } catch (err) {
        res.status(500).send({ message: "Error fetching accounts" });
      }
    });

    app.post("/api/bofa-data", async (req, res) => {
      try {
        if (!mongoClient) await connectToMongoDB();
        const transactionsDb = mongoClient.db("Transactions");
        const { accountNumbers, startDate, endDate } = req.body;
        const data = await transactionsDb
          .collection("bofa_transactions")
          .find({
            accountNumber: { $in: accountNumbers },
            asOfDate: { $gte: startDate, $lte: endDate },
          })
          .sort({ accountNumber: 1, asOfDate: 1 })
          .toArray();
        res.send(data);
      } catch (err) {
        res.status(500).send({ message: "Error fetching bofa data" });
      }
    });

    app.get("/api/due-to-due-mappings", async (req, res) => {
      try {
        const metadataDb = mongoClient.db("transactionsDB");
        const data = await metadataDb.collection("DueToDueCompanies").find({}).toArray();
        res.send(data);
      } catch (err) {
        res.status(500).send({ message: "Error fetching mappings", error: err.message });
      }
    });

    app.post("/api/due-to-due-mappings", async (req, res) => {
      try {
        const metadataDb = mongoClient.db("transactionsDB");
        const { canonical, variant, addedBy } = req.body;
        const newMapping = {
          canonical,
          variant,
          addedBy: addedBy || "Admin",
          addedAt: new Date()
        };
        const result = await metadataDb.collection("DueToDueCompanies").insertOne(newMapping);
        res.send({ message: "Added successfully", insertedId: result.insertedId });
      } catch (err) {
        res.status(500).send({ message: "Error adding mapping", error: err.message });
      }
    });

    app.put("/api/due-to-due-mappings/:id", async (req, res) => {
      try {
        const metadataDb = mongoClient.db("transactionsDB");
        const { canonical, variant, addedBy } = req.body;
        const result = await metadataDb.collection("DueToDueCompanies").updateOne(
          { _id: new ObjectId(req.params.id) },
          { $set: { canonical, variant, addedBy: addedBy || "Admin", addedAt: new Date() } }
        );
        res.send({ message: "Updated successfully" });
      } catch (err) {
        res.status(500).send({ message: "Error updating mapping", error: err.message });
      }
    });

    app.delete("/api/due-to-due-mappings/:id", async (req, res) => {
      try {
        const metadataDb = mongoClient.db("transactionsDB");
        const result = await metadataDb.collection("DueToDueCompanies").deleteOne({ _id: new ObjectId(req.params.id) });
        res.send({ message: "Deleted successfully", deletedCount: result.deletedCount });
      } catch (err) {
        res.status(500).send({ message: "Error deleting mapping", error: err.message });
      }
    });

    app.listen(port, () => {});
  })
  .catch((err) => {
    process.exit(1);
  });

process.on("SIGINT", async () => {
  if (mongoClient) await mongoClient.close();
  process.exit(0);
});
