import express from "express";
import multer from "multer";
import { MongoClient, ObjectId } from "mongodb";
import csv from "csv-parser";
import fs from "fs";
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
  process.env.MONGO_URL;
const dbName = "transactionsDB";

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
let mongoClient;

async function connectToMongoDB() {
  try {
    const client = await MongoClient.connect(mongoUrl, {
      connectTimeoutMS: 30000,
      socketTimeoutMS: 30000,
    });
    db = client.db(dbName);
    console.log("Connected to MongoDB");
    await db.collection("CTData").createIndex({ Date: 1 }); // Index for CTData
    await db.collection("BMData").createIndex({ store_name: 1 }); // Index for BMData
    return client;
  } catch (err) {
    console.error("Error connecting to MongoDB:", err);
    throw err;
  }
}

// Clean column names and values for CTData (original working version)
function cleanObjectForCTData(obj) {
  console.log("Processing columns and sample values:", {
    columns: Object.keys(obj),
    sample: obj,
  });
  return Object.keys(obj).reduce((acc, key) => {
    const cleanKey = key.trim().replace(/\s+/g, "");
    let value = obj[key];

    if (typeof value === "string") {
      value = value.trim().replace(/\x00/g, "");
      if (cleanKey.toLowerCase() !== "date") {
        const parsedNumber = parseFloat(value.replace(/[^0-9.-]/g, ""));
        if (!isNaN(parsedNumber) && value.match(/^-?\d*\.?\d*$/)) {
          value = parsedNumber;
        } else if (value) {
          console.warn(
            `Non-numeric string value "${value}" for column "${cleanKey}"`
          );
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
          value = "Invalid Date";
        }
      }
    }

    acc[cleanKey] = value;
    return acc;
  }, {});
}

// Clean column names and values for BMData (preserves _id for updates)
function cleanObjectForBMData(obj) {
  const cleaned = Object.keys(obj).reduce((acc, key) => {
    const cleanKey = key.trim().replace(/\s+/g, "").replace(/\./g, "_");
    let value = obj[key];

    if (typeof value === "string") {
      value = value.trim().replace(/\x00/g, "");
    } else if (value instanceof Date) {
      value = moment(value).format("MM-DD-YYYY");
    } else if (typeof value === "number" && !isNaN(value)) {
      // Handle Excel serial dates
      const date = new Date((value - 25569) * 86400 * 1000);
      value = moment(date).format("MM-DD-YYYY");
    }

    acc[cleanKey] = value;
    return acc;
  }, {});

  delete cleaned._id; // Remove _id for new inserts, letting MongoDB generate it
  return cleaned;
}

connectToMongoDB()
  .then((client) => {
    mongoClient = client;
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

    // Filter options endpoint for homepage interactive filtering
    app.get("/api/filter-options", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db) throw new Error("Failed to connect to MongoDB");
        }

        // Extract query parameters for state and brand
        const { state, brand } = req.query;

        // Build match query for store mappings
        const matchQuery = {};
        if (state) {
          matchQuery.State = Array.isArray(state) ? { $in: state } : state;
        }
        if (brand) {
          matchQuery.BRAND = Array.isArray(brand) ? { $in: brand } : brand;
        }

        // Fetch distinct states (not filtered by selections)
        const states = await db
          .collection("BMData")
          .distinct("State", { State: { $ne: null } });

        // Fetch distinct brands (not filtered by selections)
        const brands = await db
          .collection("BMData")
          .distinct("BRAND", { BRAND: { $ne: null } });

        // Fetch store mappings filtered by state and/or brand
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

    // Upload endpoint for CTData
    app.post("/api/upload", upload.single("file"), async (req, res) => {
      try {
        if (!req.file) {
          return res.status(400).send({ message: "No file uploaded" });
        }

        const file = req.file; // Handle single file (since frontend sends one at a time)
        const filePath = file.path;
        const fileExtension = file.originalname.split(".").pop().toLowerCase();
        let data = [];
        let sheetCount = 1; // Default for CSV

        if (fileExtension === "csv") {
          await new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
              .pipe(csv())
              .on("data", (row) => {
                const cleanedRow = cleanObjectForCTData(row);
                if (Object.keys(cleanedRow).length > 0) {
                  // Ensure row has data
                  data.push(cleanedRow);
                }
              })
              .on("end", resolve)
              .on("error", reject);
          });
        } else if (fileExtension === "xlsx" || fileExtension === "xls") {
          const workbook = XLSX.readFile(filePath);
          const sheetNames = workbook.SheetNames;
          sheetCount = sheetNames.length;

          for (const sheetName of sheetNames) {
            const sheet = workbook.Sheets[sheetName];
            const rawData = XLSX.utils.sheet_to_json(sheet, { defval: null }); // Handle empty cells
            const sheetData = rawData
              .map(cleanObjectForCTData)
              .filter((row) => Object.keys(row).length > 0); // Filter out empty rows
            console.log(
              `Processed sheet: ${sheetName}, Rows: ${sheetData.length}`
            );
            data = data.concat(sheetData); // Combine data from all sheets
          }
        } else {
          return res.status(400).send({ message: "Invalid file format" });
        }

        if (data.length === 0) {
          console.warn("No data processed from file:", filePath);
          return res
            .status(400)
            .send({ message: "No valid data found in file" });
        }

        console.log(
          "Sample processed CTData from all sheets:",
          data.slice(0, 2)
        );
        const result = await db.collection("CTData").insertMany(data);
        console.log(
          "Insert result for CTData:",
          result.insertedCount,
          "documents"
        );

        fs.unlinkSync(filePath);
        res.send({
          message: "Data uploaded successfully",
          insertedCount: result.insertedCount,
          sheetCount: sheetCount,
        });
      } catch (err) {
        console.error("Upload error:", err);
        res
          .status(500)
          .send({ message: "Error uploading file", error: err.message });
      }
    });

    // Data endpoint for CTData
    app.post("/api/data", async (req, res) => {
      try {
        if (!db) {
          await connectToMongoDB();
          if (!db) throw new Error("Failed to reconnect to MongoDB");
        }

        const { startDate, endDate } = req.body;
        if (!startDate || !endDate) {
          return res
            .status(400)
            .send({ message: "Start and end dates required" });
        }

        console.log("Incoming startDate:", startDate);
        console.log("Incoming endDate:", endDate);

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

        const startDateObj = start.toDate();
        const endDateObj = end.toDate();

        const data = await db
          .collection("CTData")
          .aggregate([
            {
              $addFields: {
                dateAsDate: {
                  $dateFromString: {
                    dateString: {
                      $ifNull: ["$Date", moment().format("MM-DD-YYYY")],
                    },
                    format: "%m-%d-%Y",
                    onError: moment().format("MM-DD-YYYY"),
                  },
                },
              },
            },
            {
              $match: {
                dateAsDate: { $gte: startDateObj, $lte: endDateObj },
              },
            },
            {
              $project: { dateAsDate: 0 },
            },
          ])
          .toArray();

        console.log(
          "Found CTData records:",
          data.length,
          "Sample:",
          data.slice(0, 2)
        );

        if (data.length === 0) {
          const sample = await db
            .collection("CTData")
            .find()
            .limit(5)
            .toArray();
          console.log("Sample CTData in DB:", sample);
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
          const normalizedRecord = {};
          finalColumns.forEach((col) => {
            normalizedRecord[col] =
              record[col] !== undefined ? record[col] : null;
          });
          return normalizedRecord;
        });

        console.log("Normalized CTData sample:", normalizedData.slice(0, 2));
        res.send(normalizedData);
      } catch (err) {
        console.error("Error fetching CTData:", err);
        res.status(500).send({ message: "Error fetching data" });
      }
    });

    // Upload endpoint for BMData
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

          fs.unlinkSync(filePath);
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

    // Fetch endpoint for BMData
    app.post("/api/bank-mapping-data", async (req, res) => {
      try {
        if (!db) {
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

    // Delete endpoint for BMData
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

    // Update endpoint for BMData
    app.post("/api/bank-mapping-update", async (req, res) => {
      try {
        const { id, updates } = req.body;
        if (!id || !updates) {
          return res.status(400).send({ message: "Invalid update request" });
        }

        const cleanedUpdates = cleanObjectForBMData(updates); // Preserve existing _id if present
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
