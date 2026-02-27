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
let mongoClient;

async function connectToMongoDB() {
  try {
    // 1. Check if we already have a valid connection
    if (
      mongoClient &&
      mongoClient.topology &&
      mongoClient.topology.isConnected()
    ) {
      // Ensure db references are set even if client persists
      if (!db) db = mongoClient.db(dbName);
      if (!brandsDb) brandsDb = mongoClient.db(brandsDbName);
      return mongoClient;
    }

    // 2. Create new connection with pool settings
    mongoClient = await MongoClient.connect(mongoUrl, {
      connectTimeoutMS: 30000,
      socketTimeoutMS: 30000,
      maxPoolSize: 50, // Allow up to 50 concurrent connections
    });

    db = mongoClient.db(dbName);
    brandsDb = mongoClient.db(brandsDbName);

    // 3. Initialize Indexes (Idempotent - safe to run multiple times)
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
    // Force reset if connection failed
    mongoClient = null;
    throw err;
  }
}

// Clean column names and values for SampleData
function cleanObjectForSampleData(obj) {
  return Object.keys(obj).reduce((acc, key) => {
    // Replace dots with underscores and remove extra spaces
    const cleanKey = key.trim().replace(/\s+/g, "").replace(/\./g, "_");
    let value = obj[key];

    if (typeof value === "string") {
      value = value.trim().replace(/\x00/g, "");
      if (cleanKey.toLowerCase() !== "date") {
        const parsedNumber = parseFloat(value.replace(/[^0-9.-]/g, ""));
        if (!isNaN(parsedNumber) && value.match(/^-?\d*\.?\d*$/)) {
          // Round to two decimal places for numeric strings
          value = Number(parsedNumber.toFixed(2));
        }
      }
    } else if (typeof value === "number" && !isNaN(value)) {
      // Round numbers to two decimal places
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
      .createIndex({ createdAt: 1 }, { expireAfterSeconds: 30 * 24 * 60 * 60 }); // 30 days TTL
  } catch (err) {
    console.error("Error initializing TransactionLog:", err);
  }
}

// Clean column names and values for BMData
function cleanObjectForBMData(obj) {
  const cleaned = Object.keys(obj).reduce((acc, key) => {
    const cleanKey = key.trim().replace(/\s+/g, "").replace(/\./g, "_");
    let value = obj[key];

    if (typeof value === "string") {
      value = value.trim().replace(/\x00/g, "");
      // Explicitly treat 'Storeno_' as a string, not a date
      if (cleanKey.toLowerCase() === "storno") {
        acc[cleanKey] = value;
        return acc;
      }
    } else if (value instanceof Date) {
      value = moment(value).format("MM-DD-YYYY");
    } else if (typeof value === "number" && !isNaN(value)) {
      // Only convert numbers to dates if they are in Excel serial date format
      // and the key is explicitly 'Date'
      if (cleanKey.toLowerCase() === "date") {
        const date = new Date((value - 25569) * 86400 * 1000);
        value = moment(date).format("MM-DD-YYYY");
      } else {
        value = value.toString(); // Treat other numbers as strings
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
        }

        const { brand } = req.query;

        // 1. Get Distinct Brands and States (Keep existing logic)
        const brands = await db.collection(brandsDbName).distinct("Brand");
        const states = await db.collection("BMData").distinct("State");

        // 2. FETCH STORE MAPPINGS
        // FIX: We removed the { Brand: brand } filter here.
        // We now fetch ALL items that have a StoreNo.
        // This solves the issue where "Popeye's" doesn't match "Popeyes" in the DB.
        const storeMappings = await db
          .collection("BMData")
          .find(
            {
              $or: [
                { StoreNo: { $exists: true, $ne: null } },
                { storeno: { $exists: true, $ne: null } }, // Handle casing variations
                { Store_No: { $exists: true, $ne: null } },
              ],
            },
            {
              projection: {
                _id: 0,
                StoreNo: 1,
                storeno: 1, // Fetch variations
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
        const { selected } = req.body;

        let updateResult;
        if (selected) {
          const formula = await db
            .collection("Formulas")
            .findOne({ _id: new ObjectId(formulaId) });
          if (!formula) {
            return res.status(404).send({ message: "Formula not found" });
          }
          await db.collection("Formulas").updateMany(
            {
              brand: formula.brand,
              selected: true,
              _id: { $ne: new ObjectId(formulaId) },
            },
            { $set: { selected: false } },
          );
          updateResult = await db
            .collection("Formulas")
            .updateOne(
              { _id: new ObjectId(formulaId) },
              { $set: { selected: true } },
            );
        } else {
          updateResult = await db
            .collection("Formulas")
            .updateOne(
              { _id: new ObjectId(formulaId) },
              { $set: { selected: false } },
            );
        }

        if (updateResult.matchedCount === 0) {
          return res.status(404).send({ message: "Formula not found" });
        }

        const updatedFormula = await db
          .collection("Formulas")
          .findOne({ _id: new ObjectId(formulaId) });
        res.status(200).send({
          ...updatedFormula,
          _id: updatedFormula._id.toString(),
          selected: updatedFormula.selected,
        });
      } catch (err) {
        console.error("Error toggling formula:", err);
        res
          .status(500)
          .send({ message: "Error toggling formula", error: err.message });
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

          // 1. Fetch valid accounts where Cashpro is 'Yes'
          const bmData = await db
            .collection("BMData")
            .find({
              Cashpro: { $regex: /^yes$/i },
            })
            .toArray();

          const validAccounts = new Set();

          // Helper function to extract just the numbers/letters from the account string
          const cleanBankAccount = (accStr) => {
            if (!accStr) return "";
            let cleaned = String(accStr)
              .replace(/Acc No\s*:/i, "")
              .trim();

            // If Excel appended .00, split by decimal and keep the first part
            if (cleaned.includes(".")) {
              cleaned = cleaned.split(".")[0];
            }

            return cleaned.replace(/[^a-zA-Z0-9]/g, ""); // Strip remaining spaces/dashes
          };

          // Populate the Set with cleaned BMData account numbers
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

          // 2. Read and parse the uploaded file
          const fileContent = fs.readFileSync(req.file.path, "utf8");
          const lines = fileContent.split(/\r?\n/);

          const outputRows = [];
          const logRows = [];

          // Add header to the log file for clarity
          logRows.push(
            "Account Number,Check Number,Amount,Issue Date,Indicator,Payee,Reason Removed",
          );

          for (const line of lines) {
            if (!line.trim()) continue; // Skip empty lines

            // Split by comma, respecting quotes
            const row = line.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/);

            // Skip the header row from the input file
            if (row[0] && row[0].includes("Account Number")) continue;

            // 3. Clean fields and format
            // FIX: Clean the input account number to remove .00 and extract exact string
            let rawAccountNum = row[0]
              ? String(row[0]).replace(/['"]/g, "").trim()
              : "";
            if (rawAccountNum.includes(".")) {
              rawAccountNum = rawAccountNum.split(".")[0]; // remove .00
            }
            const accountNum = rawAccountNum.replace(/[^a-zA-Z0-9]/g, "");

            const checkNum = row[1] ? row[1].replace(/['"]/g, "").trim() : "";

            // Keep the tab (\t) in amount but remove ONLY the quotes
            const amount = row[2] ? row[2].replace(/"/g, "") : "";

            // FIX: Format Date strictly to MM/DD/YYYY
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

            // Keep Quotes for Payees containing commas
            const payee = row[5] ? row[5].trim() : "";

            const cleanedRowString = `${accountNum},${checkNum},${amount},${formattedDate},${indicator},${payee}`;

            // 4. Validate against BMData set
            if (validAccounts.has(accountNum)) {
              outputRows.push(cleanedRowString);
            } else {
              logRows.push(
                `${cleanedRowString},Account not matched in BMData or Cashpro != Yes`,
              );
            }
          }

          // Clean up uploaded file from server
          await fs.promises.unlink(req.file.path).catch(console.error);

          // 5. Generate ZIP File and send response
          const archive = archiver("zip", { zlib: { level: 9 } });

          res.setHeader("Content-Type", "application/zip");
          res.setHeader(
            "Content-Disposition",
            'attachment; filename="ChequeSheets.zip"',
          );

          archive.pipe(res);

          // Append generated files to the zip
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

        // Ensure DB is connected
        if (!brandsDb) await connectToMongoDB();

        // Create index for the brand collection if it doesn't exist
        // This speeds up the duplicate check query significantly
        await brandsDb.collection(brand).createIndex({ StoreName: 1, Date: 1 });

        let totalInserted = 0;
        let totalDuplicates = 0;
        let totalSheets = 0;

        // BATCH SIZE: Process 1000 rows at a time to prevent Timeouts/Memory crashes
        const BATCH_SIZE = 1000;

        // Process files SEQUENTIALLY to save memory (using for...of instead of map/Promise.all)
        for (const file of req.files) {
          const filePath = file.path;
          const fileExtension = file.originalname
            .split(".")
            .pop()
            .toLowerCase();

          let fileData = [];
          let sheetCount = 0;

          try {
            // --- Parsing Logic (Kept existing logic) ---
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
              continue; // Skip to next file
            }

            // --- BATCH PROCESSING START ---
            // We split the large fileData into smaller chunks
            for (let i = 0; i < fileData.length; i += BATCH_SIZE) {
              const batch = fileData.slice(i, i + BATCH_SIZE);

              // 1. Prepare keys for duplicate check
              const keys = batch
                .filter((record) => record.StoreName && record.Date)
                .map((record) => ({
                  StoreName: record.StoreName,
                  Date: record.Date,
                }));

              if (keys.length === 0) continue;

              // 2. Find existing records ONLY for this batch
              const existingRecords = await brandsDb
                .collection(brand)
                .find({ $or: keys })
                .project({ StoreName: 1, Date: 1 })
                .toArray();

              const existingKeys = new Set(
                existingRecords.map((r) => `${r.StoreName}|${r.Date}`),
              );

              // 3. Filter duplicates
              const recordsToInsert = batch.filter((record) => {
                if (!record.StoreName || !record.Date) return false;
                const key = `${record.StoreName}|${record.Date}`;
                return !existingKeys.has(key);
              });

              // 4. Insert Batch
              if (recordsToInsert.length > 0) {
                const result = await brandsDb
                  .collection(brand)
                  .insertMany(recordsToInsert, { ordered: false });
                totalInserted += result.insertedCount;
              }

              totalDuplicates += batch.length - recordsToInsert.length;

              // Small pause to allow Node event loop to handle other requests (prevents blocking)
              await new Promise((resolve) => setTimeout(resolve, 5));
            }
            // --- BATCH PROCESSING END ---

            totalSheets += sheetCount;
          } catch (err) {
            console.error(`Error processing file ${file.originalname}:`, err);
            // Optionally continue to next file or throw
          } finally {
            // Clean up uploaded file
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
          return res
            .status(400)
            .send({ message: "Start date, end date, and brand are required" });
        }

        const start = moment(
          startDate,
          ["MM-DD-YYYY", "M/D/YYYY", "MM/DD/YYYY", "YYYY-MM-DD", "M-D-YYYY"],
          true,
        );
        const end = moment(
          endDate,
          ["MM-DD-YYYY", "M/D/YYYY", "MM/DD/YYYY", "YYYY-MM-DD", "M-D-YYYY"],
          true,
        );

        if (!start.isValid() || !end.isValid()) {
          return res.status(400).send({
            message: "Invalid date format. Use MM-DD-YYYY (e.g., 03-01-2025)",
          });
        }

        // 1. Create JavaScript Date objects for comparison
        // We format to YYYY-MM-DD first to ensure clean parsing into a Date object
        const startDateForQuery = new Date(start.format("YYYY-MM-DD"));
        const endDateForQuery = new Date(end.format("YYYY-MM-DD"));

        // 2. Use Aggregation to parse the stored strings into Dates for comparison
        const data = await brandsDb
          .collection(brand)
          .aggregate([
            {
              $addFields: {
                // Create a temporary field that converts "MM-DD-YYYY" string to a real Date
                __parsedDate: {
                  $dateFromString: {
                    dateString: "$Date",
                    format: "%m-%d-%Y",
                    onError: new Date("1970-01-01T00:00:00Z"), // Fallback if format is wrong
                    onNull: new Date("1970-01-01T00:00:00Z"),
                  },
                },
              },
            },
            {
              $match: {
                // Filter using the real Date objects (Handles cross-year correctly)
                __parsedDate: {
                  $gte: startDateForQuery,
                  $lte: endDateForQuery,
                },
              },
            },
            {
              $project: {
                __parsedDate: 0, // Remove the temporary field from the final result
              },
            },
          ])
          .toArray();

        if (data.length === 0) {
          const sample = await brandsDb
            .collection(brand)
            .find()
            .limit(5)
            .toArray();
          return res.send(data);
        }

        // Collect all unique columns across all documents
        const allColumns = Array.from(
          new Set(data.flatMap((record) => Object.keys(record))),
        ).filter((col) => col !== "_id");

        // Define priority columns to appear first
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

        const cleanedUpdates = cleanObjectForSampleData(updates);

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

        const afterUpdate = await brandsDb
          .collection(brand)
          .findOne({ _id: objectId });

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
          updates
            .map(async ({ id, updates }) => {
              let objectId;
              try {
                objectId = new ObjectId(id);
              } catch (err) {
                console.warn("Invalid ObjectId skipped:", id);
                return null;
              }

              const cleanedUpdates = cleanObjectForSampleData(updates);

              // Fetch original record to log previous values
              const originalRecord = await brandsDb
                .collection(brand)
                .findOne({ _id: objectId }, { projection: { _id: 0 } });

              if (!originalRecord) {
                console.warn(`Record not found for ID: ${id}`);
                return null;
              }

              let additionalUpdates = {};
              if (activeFormula) {
                const { formula, column } = activeFormula;

                let expression = formula;
                let resultIsString = false;

                let formulaColumns = formulaColumnsCache.get(formula);
                if (!formulaColumns) {
                  formulaColumns = [];
                  const recordKeys = Object.keys(originalRecord).filter(
                    (key) => key !== "_id",
                  );
                  recordKeys.forEach((key) => {
                    const escapedKey = key.replace(
                      /[.*+?^${}()|[\]\\]/g,
                      "\\$&",
                    );
                    const regex = new RegExp(
                      `(^|[\\s+\\-*/%\\(])\\s*(${escapedKey})\\s*([\\s+\\-*/%\\)]|$)`,
                      "gi",
                    );
                    if (regex.test(formula)) {
                      formulaColumns.push({ key, regex });
                    }
                  });
                  formulaColumnsCache.set(formula, formulaColumns);
                }

                formulaColumns.forEach(({ key }) => {
                  let cellValue =
                    key in cleanedUpdates
                      ? cleanedUpdates[key]
                      : originalRecord[key];
                  if (
                    cellValue === null ||
                    cellValue === undefined ||
                    cellValue === ""
                  ) {
                    cellValue = 0;
                  } else if (typeof cellValue === "string") {
                    const cleanedValue = cellValue.replace(/[^0-9.-]/g, "");
                    const parsedValue = parseFloat(cleanedValue);
                    if (
                      isNaN(parsedValue) ||
                      !cleanedValue.match(/^-?\d*\.?\d*$/)
                    ) {
                      resultIsString = true;
                      cellValue = `"${cellValue}"`;
                    } else {
                      cellValue = parsedValue;
                    }
                  }
                  const escapedKey = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
                  const regex = new RegExp(
                    `(^|[\\s+\\-*/%\\(])\\s*(${escapedKey})\\s*([\\s+\\-*/%\\)]|$)`,
                    "g",
                  );
                  expression = expression.replace(regex, `$1${cellValue}$3`);
                });

                try {
                  let result;
                  if (resultIsString) {
                    result = expression.replace(/"/g, "");
                  } else {
                    result = evaluate(expression);
                    if (isNaN(result) || !isFinite(result)) {
                      throw new Error(
                        `Formula evaluation resulted in invalid number: "${result}"`,
                      );
                    }
                  }
                  additionalUpdates[column] = result;
                } catch (error) {
                  console.error(
                    `Error evaluating formula for ID ${id}: ${error.message}`,
                    { expression, formula, column },
                  );
                  additionalUpdates[column] = 0;
                }
              }

              // Log the original and new values
              const allUpdates = { ...cleanedUpdates, ...additionalUpdates };
              const originalValues = {};
              Object.keys(allUpdates).forEach((key) => {
                originalValues[key] =
                  originalRecord[key] !== undefined
                    ? originalRecord[key]
                    : null;
              });

              transactionLog.updates.push({
                id,
                originalValues,
                newValues: allUpdates,
              });

              return {
                updateOne: {
                  filter: { _id: objectId },
                  update: {
                    $set: {
                      ...Object.keys(cleanedUpdates).reduce((acc, key) => {
                        acc[key] = cleanedUpdates[key];
                        return acc;
                      }, {}),
                      ...additionalUpdates,
                    },
                  },
                },
              };
            })
            .filter((op) => op !== null),
        );

        if (bulkOperations.length === 0) {
          console.warn("No valid operations to execute");
          return res
            .status(400)
            .send({ message: "No valid updates to process" });
        }

        // Save transaction log
        await db.collection("TransactionLog").insertOne(transactionLog);

        const result = await brandsDb
          .collection(brand)
          .bulkWrite(bulkOperations);

        res.send({
          message: "Bulk update completed",
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
          transactionId,
        });
      } catch (err) {
        console.error("Error in bulk update:", err);
        res.status(500).send({
          message: "Error performing bulk update",
          error: err.message,
        });
      }
    });

    app.post("/api/clear-transaction-logs", async (req, res) => {
      try {
        const { brand } = req.body;
        if (!brand) {
          return res.status(400).send({ message: "Brand is required" });
        }

        // Log sample TransactionLog entries for debugging
        const sampleLogs = await db
          .collection("TransactionLog")
          .find({ brand })
          .limit(5)
          .toArray();

        const result = await db
          .collection("TransactionLog")
          .deleteMany({ brand });

        res.send({
          message: `Cleared ${result.deletedCount} transaction logs`,
          deletedCount: result.deletedCount,
        });
      } catch (err) {
        console.error("Error clearing transaction logs:", err);
        res.status(500).send({
          message: "Error clearing transaction logs",
          error: err.message,
        });
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

        let modifiedCount = 0;
        for (const update of updates) {
          const { _id, value } = update;
          try {
            const objectId = new ObjectId(_id);
            const originalRecord = await brandsDb
              .collection(brand)
              .findOne({ _id: objectId }, { projection: { [column]: 1 } });

            const originalValue = originalRecord
              ? originalRecord[column]
              : null;

            transactionLog.updates.push({
              id: _id,
              originalValues: { [column]: originalValue },
              newValues: { [column]: value },
            });

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

        // Save transaction log
        await db.collection("TransactionLog").insertOne(transactionLog);

        if (modifiedCount === 0) {
          return res.status(200).send({
            message: "No records were modified",
            modifiedCount,
            transactionId,
          });
        }

        res.send({
          message: isNewColumn
            ? `New column '${column}' added successfully`
            : `Column '${column}' updated successfully`,
          modifiedCount,
          transactionId,
        });
      } catch (err) {
        console.error("Error processing calculated column:", err);
        res.status(500).send({
          message: "Error processing calculated column",
          error: err.message,
        });
      }
    });

    // New endpoint for undo operation
    app.post("/api/undo", async (req, res) => {
      try {
        const { brand } = req.body;
        if (!brand) {
          return res.status(400).send({ message: "Brand is required" });
        }

        // Find the most recent active transaction
        const transaction = await db
          .collection("TransactionLog")
          .findOne({ brand, status: "active" }, { sort: { createdAt: -1 } });

        if (!transaction) {
          return res.status(404).send({ message: "No actions to undo" });
        }

        const bulkOperations = transaction.updates
          .map(({ id, originalValues }) => {
            try {
              const objectId = new ObjectId(id);
              return {
                updateOne: {
                  filter: { _id: objectId },
                  update: { $set: originalValues },
                },
              };
            } catch (err) {
              console.warn(`Invalid ObjectId for undo: ${id}`);
              return null;
            }
          })
          .filter((op) => op !== null);

        if (bulkOperations.length === 0) {
          await db
            .collection("TransactionLog")
            .updateOne(
              { transactionId: transaction.transactionId },
              { $set: { status: "undone" } },
            );
          return res.status(200).send({
            message: "No valid records to undo, transaction marked as undone",
          });
        }

        const result = await brandsDb
          .collection(brand)
          .bulkWrite(bulkOperations);

        // Mark transaction as undone
        await db
          .collection("TransactionLog")
          .updateOne(
            { transactionId: transaction.transactionId },
            { $set: { status: "undone" } },
          );

        res.send({
          message: "Undo completed",
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
        });
      } catch (err) {
        console.error("Error performing undo:", err);
        res.status(500).send({
          message: "Error performing undo",
          error: err.message,
        });
      }
    });

    // New endpoint for redo operation
    app.post("/api/redo", async (req, res) => {
      try {
        const { brand } = req.body;
        if (!brand) {
          return res.status(400).send({ message: "Brand is required" });
        }

        // Find the most recent undone transaction
        const transaction = await db
          .collection("TransactionLog")
          .findOne({ brand, status: "undone" }, { sort: { createdAt: -1 } });

        if (!transaction) {
          return res.status(404).send({ message: "No actions to redo" });
        }

        const bulkOperations = transaction.updates
          .map(({ id, newValues }) => {
            try {
              const objectId = new ObjectId(id);
              return {
                updateOne: {
                  filter: { _id: objectId },
                  update: { $set: newValues },
                },
              };
            } catch (err) {
              console.warn(`Invalid ObjectId for redo: ${id}`);
              return null;
            }
          })
          .filter((op) => op !== null);

        if (bulkOperations.length === 0) {
          await db
            .collection("TransactionLog")
            .updateOne(
              { transactionId: transaction.transactionId },
              { $set: { status: "active" } },
            );
          return res.status(200).send({
            message: "No valid records to redo, transaction marked as active",
          });
        }

        const result = await brandsDb
          .collection(brand)
          .bulkWrite(bulkOperations);

        // Mark transaction as active
        await db
          .collection("TransactionLog")
          .updateOne(
            { transactionId: transaction.transactionId },
            { $set: { status: "active" } },
          );

        res.send({
          message: "Redo completed",
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
        });
      } catch (err) {
        console.error("Error performing redo:", err);
        res.status(500).send({
          message: "Error performing redo",
          error: err.message,
        });
      }
    });

    app.post("/api/check-transactions", async (req, res) => {
      try {
        const { brand } = req.body;
        if (!brand) {
          return res.status(400).send({ message: "Brand is required" });
        }

        const canUndo = await db
          .collection("TransactionLog")
          .findOne({ brand, status: "active" }, { sort: { createdAt: -1 } });

        const canRedo = await db
          .collection("TransactionLog")
          .findOne({ brand, status: "undone" }, { sort: { createdAt: -1 } });

        res.send({
          canUndo: !!canUndo,
          canRedo: !!canRedo,
        });
      } catch (err) {
        console.error("Error checking transactions:", err);
        res.status(500).send({
          message: "Error checking transactions",
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
          let headers = []; // Store the header order

          if (fileExtension === "csv") {
            await new Promise((resolve, reject) => {
              const results = [];
              fs.createReadStream(filePath)
                .pipe(csv())
                .on("headers", (headerList) => {
                  headers = headerList.map((header) =>
                    header.trim().replace(/\s+/g, "").replace(/\./g, "_"),
                  ); // Capture and clean headers
                })
                .on("data", (row) => {
                  results.push(cleanObjectForBMData(row));
                })
                .on("end", resolve)
                .on("error", reject);
              data = results;
            });
          } else if (fileExtension === "xlsx" || fileExtension === "xls") {
            const workbook = XLSX.readFile(filePath);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const rawData = XLSX.utils.sheet_to_json(sheet, { header: 1 });
            headers = rawData[0].map((header) =>
              header
                ? header
                    .toString()
                    .trim()
                    .replace(/\s+/g, "")
                    .replace(/\./g, "_")
                : "",
            ); // Capture and clean headers
            data = rawData.slice(1).map((row) => {
              const rowData = {};
              headers.forEach((header, index) => {
                rowData[header] = row[index] !== undefined ? row[index] : null;
              });
              return cleanObjectForBMData(rowData);
            });
          } else {
            return res.status(400).send({ message: "Invalid file format" });
          }

          if (data.length === 0) {
            console.warn("No data processed from file:", filePath);
            return res
              .status(400)
              .send({ message: "No valid data found in file" });
          }

          // Insert new data
          const result = await db.collection("BMData").insertMany(data);

          // Store the header order in a separate collection or document
          await db
            .collection("BMDataHeaders")
            .updateOne(
              { _id: "headerOrder" },
              { $set: { headers } },
              { upsert: true },
            );

          await fs.promises.unlink(filePath);
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
      },
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
                ...(posName.length > 0 && { Name: { $in: posName } }),
                ...(brand.length > 0 && { Brand: { $in: brand } }),
              },
            },
          ])
          .toArray();

        if (data.length === 0) {
          return res.send([]);
        }

        // Retrieve stored header order
        const headerDoc = await db
          .collection("BMDataHeaders")
          .findOne({ _id: "headerOrder" });
        const headers = headerDoc ? headerDoc.headers : [];

        // If no headers are stored, fall back to the first document's keys
        const allColumns =
          headers.length > 0
            ? headers
            : Object.keys(data[0]).filter((col) => col !== "_id");

        const normalizedData = data.map((record) => {
          const normalizedRecord = { _id: record._id.toString() };
          allColumns.forEach((col) => {
            normalizedRecord[col] =
              record[col] !== undefined ? record[col] : null;
          });
          return normalizedRecord;
        });

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

        const objectIds = ids
          .map((id) => {
            try {
              return new ObjectId(id);
            } catch (err) {
              console.warn(`Invalid ObjectId skipped: ${id}`);
              return null;
            }
          })
          .filter((id) => id !== null);

        if (objectIds.length === 0) {
          return res
            .status(400)
            .send({ message: "No valid ObjectIds provided" });
        }

        const result = await db
          .collection("BMData")
          .deleteMany({ _id: { $in: objectIds } });

        res.send({
          message: "Data deleted successfully",
          deletedCount: result.deletedCount,
        });
      } catch (err) {
        console.error("Error deleting data:", err);
        res
          .status(500)
          .send({ message: "Error deleting data", error: err.message });
      }
    });

    // Endpoint to upload StoreData
    app.post(
      "/api/store-data-upload",
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
                  data.push(cleanObjectForBMData(row)); // Reusing cleanObjectForBMData for consistency
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

          const result = await db.collection("StoreData").insertMany(data);

          await fs.promises.unlink(filePath);
          res.send({
            message: "Store data uploaded successfully",
            insertedCount: result.insertedCount,
          });
        } catch (err) {
          console.error("Store data upload error:", err);
          res.status(500).send({
            message: "Error uploading store data file",
            error: err.message,
          });
        }
      },
    );

    // Endpoint to fetch StoreData
    app.post("/api/store-data", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db)
            return res.status(503).send({ message: "Database not connected" });
        }

        const data = await db.collection("StoreData").find({}).toArray();

        if (data.length === 0) {
          return res.send({ storeData: [] });
        }

        // Dynamically collect all unique columns from the data
        const allColumns = Array.from(
          new Set(data.flatMap((record) => Object.keys(record))),
        ).filter((col) => col !== "_id");

        // Ensure 'NAME' and 'NO' appear first, if present
        const priorityColumns = ["NAME", "NO"];
        const finalColumns = [
          ...priorityColumns.filter((col) => allColumns.includes(col)),
          ...allColumns.filter(
            (col) => !priorityColumns.includes(col) && col !== "_id",
          ),
        ];

        const normalizedData = data.map((record) => {
          const normalizedRecord = { _id: record._id.toString() };
          finalColumns.forEach((col) => {
            normalizedRecord[col] =
              record[col] !== undefined ? record[col] : null;
          });
          return normalizedRecord;
        });

        res.send({ storeData: normalizedData }); // Wrap the response in storeData
      } catch (err) {
        console.error("Error fetching store data:", err);
        res.status(500).send({ message: "Error fetching store data" });
      }
    });

    // Endpoint to delete StoreData
    app.post("/api/store-data-delete", async (req, res) => {
      try {
        const { ids } = req.body;
        if (!ids || !Array.isArray(ids) || ids.length === 0) {
          return res.status(400).send({ message: "No valid IDs provided" });
        }

        const objectIds = ids
          .map((id) => {
            try {
              return new ObjectId(id);
            } catch (err) {
              console.warn(`Invalid ObjectId skipped: ${id}`);
              return null;
            }
          })
          .filter((id) => id !== null);

        if (objectIds.length === 0) {
          return res
            .status(400)
            .send({ message: "No valid ObjectIds provided" });
        }

        const result = await db
          .collection("StoreData")
          .deleteMany({ _id: { $in: objectIds } });

        res.send({
          message: "Data deleted successfully",
          deletedCount: result.deletedCount,
        });
      } catch (err) {
        console.error("Error deleting store data:", err);
        res.status(500).send({
          message: "Error deleting store data",
          error: err.message,
        });
      }
    });

    // Endpoint to add new StoreData
    app.post("/api/store-data-add", async (req, res) => {
      try {
        const { data } = req.body;
        if (!data || typeof data !== "object") {
          return res.status(400).send({ message: "Invalid data provided" });
        }

        const cleanedData = cleanObjectForBMData(data);
        if (Object.keys(cleanedData).length === 0) {
          return res.status(400).send({ message: "No valid data to insert" });
        }

        const result = await db.collection("StoreData").insertOne(cleanedData);

        res.send({
          message: "New store data added successfully",
          insertedId: result.insertedId.toString(),
        });
      } catch (err) {
        console.error("Error adding store data:", err);
        res.status(500).send({
          message: "Error adding store data",
          error: err.message,
        });
      }
    });

    // Endpoint to update StoreData
    app.post("/api/store-data-update", async (req, res) => {
      try {
        const { id, updates } = req.body;
        if (!id || !updates) {
          return res.status(400).send({ message: "Invalid update request" });
        }

        const cleanedUpdates = cleanObjectForBMData(updates);
        const result = await db
          .collection("StoreData")
          .updateOne({ _id: new ObjectId(id) }, { $set: cleanedUpdates });
        if (result.matchedCount === 0) {
          return res.status(404).send({ message: "Record not found" });
        }
        res.send({ message: "Data updated successfully" });
      } catch (err) {
        console.error("Error updating store data:", err);
        res.status(500).send({ message: "Error updating store data" });
      }
    });

    app.post("/api/bank-mapping-add", async (req, res) => {
      try {
        const { data } = req.body;
        if (!data || typeof data !== "object") {
          return res.status(400).send({ message: "Invalid data provided" });
        }

        const cleanedData = cleanObjectForBMData(data);
        if (Object.keys(cleanedData).length === 0) {
          return res.status(400).send({ message: "No valid data to insert" });
        }

        const result = await db.collection("BMData").insertOne(cleanedData);

        res.send({
          message: "New bank mapping data added successfully",
          insertedId: result.insertedId.toString(),
        });
      } catch (err) {
        console.error("Error adding bank mapping data:", err);
        res.status(500).send({
          message: "Error adding bank mapping data",
          error: err.message,
        });
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

    app.get("/api/store-data", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db) throw new Error("Failed to connect to MongoDB");
        }

        const storeData = await db
          .collection("StoreData")
          .find(
            {},
            {
              projection: {
                NAME: 1,
                NO: 1,
                _id: 0,
              },
            },
          )
          .toArray();

        res.send({ storeData });
      } catch (err) {
        console.error("Error fetching store data:", err);
        res
          .status(500)
          .send({ message: "Error fetching store data", error: err.message });
      }
    });

    app.get("/api/bofa-accounts", async (req, res) => {
      try {
        if (!db) await connectToMongoDB();

        // 1. Get the list of accounts from Transactions (as before)
        let accounts = await db
          .collection("bofa_transactions")
          .aggregate([
            { $sort: { accountName: -1 } }, // Try to find a name if it exists
            {
              $group: {
                _id: "$accountNumber",
                accountName: { $first: "$accountName" },
              },
            },
            {
              $project: {
                _id: 0,
                accountNumber: "$_id",
                accountName: 1,
              },
            },
          ])
          .toArray();

        // 2. Fetch the Master Mapping Data (BMData) to fill in the blanks
        // We only fetch fields we need to optimize performance
        const mappings = await db
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

        // 3. Merge: If accountName is missing, look it up in Mappings
        accounts = accounts.map((acc) => {
          // If we already found a name in the transactions, keep it
          if (acc.accountName) return acc;

          // Otherwise, search for the Account Number in BMData
          const foundMap = mappings.find((m) => {
            const accNumStr = String(acc.accountNumber);
            return (
              String(m.BankAccountNo1).includes(accNumStr) ||
              String(m.BankAccountNo2).includes(accNumStr) ||
              String(m.BankAccountNo3).includes(accNumStr) ||
              String(m.BankAccountNo4).includes(accNumStr)
            );
          });

          return {
            accountNumber: acc.accountNumber,
            // Use mapped name, or fall back to "Unknown" if still not found
            accountName: foundMap ? foundMap.CompanyTaxName : "Unknown Entity",
          };
        });

        // 4. Final Sort by Name
        accounts.sort((a, b) => {
          const nameA = a.accountName || "";
          const nameB = b.accountName || "";
          return nameA.localeCompare(nameB);
        });

        res.send(accounts);
      } catch (err) {
        console.error("Error fetching BofA accounts:", err);
        res.status(500).send({ message: "Error fetching accounts" });
      }
    });

    // 2. Fetch Data for Selected Accounts & Date Range
    app.post("/api/bofa-data", async (req, res) => {
      try {
        if (!db) await connectToMongoDB();
        const { accountNumbers, startDate, endDate } = req.body;

        // Query match
        const query = {
          accountNumber: { $in: accountNumbers },
          asOfDate: { $gte: startDate, $lte: endDate },
        };

        const transactions = await db
          .collection("bofa_transactions")
          .find(query)
          .sort({ accountNumber: 1, asOfDate: 1 })
          .toArray();

        res.send(transactions);
      } catch (err) {
        console.error("Error fetching BofA transactions:", err);
        res.status(500).send({ message: "Error fetching data" });
      }
    });

    app.listen(port, () => {});
  })
  .catch((err) => {
    console.error("Failed to start server:", err);
    process.exit(1);
  });

process.on("SIGINT", async () => {
  if (mongoClient) {
    await mongoClient.close();
  }
  process.exit(0);
});
