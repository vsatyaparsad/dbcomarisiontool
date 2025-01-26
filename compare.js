import pg from 'pg';
import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';
import ExcelJS from 'exceljs';
import fs from 'fs-extra';
import path from 'path';
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import { fileURLToPath } from 'url';

dotenv.config();

// Configure ES module filename
const __filename = fileURLToPath(import.meta.url);

// Database configurations (same as before)
const pgConfig = {
  host: process.env.POSTGRES_HOST,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DATABASE,
  port: process.env.POSTGRES_PORT,
};

const snowflakeConfig = {
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  database: process.env.SNOWFLAKE_DATABASE,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  schema: process.env.SNOWFLAKE_SCHEMA,
};

const QUERY_TIMEOUT = 30000;

// Database query functions (same as before)
async function executePostgresQueryBatch(sql, batchSize, offset) {
  const client = new pg.Client(pgConfig);
  try {
    await client.connect();
    const batchSQL = `${sql} LIMIT ${batchSize} OFFSET ${offset}`;
    const result = await client.query({ text: batchSQL, timeout: QUERY_TIMEOUT });
    return result.rows.map((row) => {
      const upperCaseRow = {};
      for (const key in row) {
        upperCaseRow[key.toUpperCase()] = row[key];
      }
      return upperCaseRow;
    });
  } finally {
    await client.end();
  }
}

async function executeSnowflakeQueryBatch(sql, batchSize, offset) {
  return new Promise((resolve, reject) => {
    const connection = snowflake.createConnection(snowflakeConfig);
    connection.connect((err) => {
      if (err) return reject(err);
      const batchSQL = `${sql} LIMIT ${batchSize} OFFSET ${offset}`;
      connection.execute({
        sqlText: batchSQL,
        complete: (err, stmt, rows) => {
          connection.destroy();
          if (err) return reject(err);
          resolve(rows);
        },
      });
    });
  });
}

// Worker thread processing
function workerMain() {
  const { outputPath, pgSQL, snowflakeSQL, batchSize } = workerData;

  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: outputPath });
  const pgSheet = workbook.addWorksheet('Postgres Data');
  const snowflakeSheet = workbook.addWorksheet('Snowflake Data');
  const comparisonSheet = workbook.addWorksheet('Detailed Comparison');
  const summarySheet = workbook.addWorksheet('Summary');

  let pgHeaders = [], snowflakeHeaders = [], comparisonHeaders = [];
  let offset = 0, matchedCount = 0, postgresOnlyCount = 0, snowflakeOnlyCount = 0;

  const processBatch = async () => {
    try {
      const [pgBatch, snowflakeBatch] = await Promise.all([
        executePostgresQueryBatch(pgSQL, batchSize, offset),
        executeSnowflakeQueryBatch(snowflakeSQL, batchSize, offset),
      ]);

      if (!pgBatch.length && !snowflakeBatch.length) {
        finalizeWorkbook();
        return;
      }

      if (offset === 0) initializeHeaders(pgBatch, snowflakeBatch);

      writeRawData(pgBatch, snowflakeBatch);
      compareBatches(pgBatch, snowflakeBatch);

      offset += batchSize;
      processBatch();
    } catch (error) {
      parentPort.postMessage({ error: error.message });
    }
  };

  const initializeHeaders = (pgBatch, snowflakeBatch) => {
    pgHeaders = Object.keys(pgBatch[0] || {});
    snowflakeHeaders = Object.keys(snowflakeBatch[0] || {});
    comparisonHeaders = [...pgHeaders, 'Status'];
    
    pgSheet.columns = pgHeaders.map((key) => ({ header: key, key }));
    snowflakeSheet.columns = snowflakeHeaders.map((key) => ({ header: key, key }));
    comparisonSheet.columns = comparisonHeaders.map((key) => ({ header: key, key }));
  };

  const writeRawData = (pgBatch, snowflakeBatch) => {
    pgBatch.forEach(row => pgSheet.addRow(row).commit());
    snowflakeBatch.forEach(row => snowflakeSheet.addRow(row).commit());
  };

  const compareBatches = (pgBatch, snowflakeBatch) => {
    const snowflakeMap = new Map(snowflakeBatch.map(row => [JSON.stringify(row), row]));

    pgBatch.forEach(pgRow => {
      const match = snowflakeMap.get(JSON.stringify(pgRow));
      const status = match ? 'Matched' : 'Postgres Only';
      addComparisonRow(pgRow, status, match);
      match ? matchedCount++ : postgresOnlyCount++;
    });

    snowflakeBatch.forEach(sfRow => {
      if (!pgBatch.some(pgRow => JSON.stringify(pgRow) === JSON.stringify(sfRow))) {
        addComparisonRow(sfRow, 'Snowflake Only', false);
        snowflakeOnlyCount++;
      }
    });
  };

  const addComparisonRow = (row, status, isMatch) => {
    const rowData = { ...row, Status: status };
    const excelRow = comparisonSheet.addRow(rowData);
    
    Object.keys(row).forEach(key => {
      const cell = excelRow.getCell(key);
      cell.fill = {
        type: 'pattern',
        pattern: 'solid',
        fgColor: { argb: isMatch ? 'C6EFCE' : 'FFC7CE' }
      };
      cell.font = { color: { argb: isMatch ? '006100' : '9C0006' } };
    });
    
    excelRow.commit();
  };

function workerMain() {
  // ... other code remains the same ...

  const finalizeWorkbook = async () => {
    // Initialize summary sheet headers
    summarySheet.addRow(['Metric', 'Value']).commit();

    // Add summary data rows
    summarySheet.addRow(['Postgres Row Count', matchedCount + postgresOnlyCount]).commit();
    summarySheet.addRow(['Snowflake Row Count', matchedCount + snowflakeOnlyCount]).commit();
    summarySheet.addRow(['Matched Records', matchedCount]).commit();
    summarySheet.addRow(['Postgres Only Records', postgresOnlyCount]).commit();
    summarySheet.addRow(['Snowflake Only Records', snowflakeOnlyCount]).commit();
    summarySheet.addRow(['Row Count Difference', postgresOnlyCount - snowflakeOnlyCount]).commit();

    await workbook.commit();
    parentPort.postMessage({ done: true });
  };

  // ... rest of the workerMain function remains unchanged ...
}

  processBatch();
}

// Main comparison function
async function compareSQLFiles(pgFolder, snowflakeFolder, outputFolder, batchSize = 5000) {
  try {
    await fs.ensureDir(outputFolder);
    const pgFiles = (await fs.readdir(pgFolder)).filter(f => f.endsWith('.sql'));
    console.log(`Found ${pgFiles.length} SQL files to compare`);

    const MAX_CONCURRENT = 3;
    for (let i = 0; i < pgFiles.length; i += MAX_CONCURRENT) {
      const chunk = pgFiles.slice(i, i + MAX_CONCURRENT);
      await Promise.all(chunk.map(processFile));
    }

    console.log('Comparison complete!');
  } catch (error) {
    console.error('Comparison failed:', error);
  }

  async function processFile(sqlFile) {
    try {
      console.log(`Processing ${sqlFile}...`);
      const pgPath = path.join(pgFolder, sqlFile);
      const sfPath = path.join(snowflakeFolder, sqlFile);

      if (!(await fs.pathExists(sfPath))) {
        console.warn(`Skipping ${sqlFile} - no matching Snowflake file`);
        return;
      }

      const [pgSQL, sfSQL] = await Promise.all([
        fs.readFile(pgPath, 'utf8'),
        fs.readFile(sfPath, 'utf8')
      ]);

      const outputPath = path.join(outputFolder, `${path.parse(sqlFile).name}_comparison.xlsx`);
      await new Promise((resolve, reject) => {
        const worker = new Worker(__filename, {
          workerData: { outputPath, pgSQL, snowflakeSQL: sfSQL, batchSize }
        });

        worker.on('message', msg => {
          if (msg.done) resolve();
          if (msg.error) reject(msg.error);
        });

        worker.on('error', reject);
        worker.on('exit', code => code !== 0 && reject(new Error(`Worker exited with code ${code}`)));
      });

      console.log(`Completed ${sqlFile}`);
    } catch (error) {
      console.error(`Error processing ${sqlFile}:`, error.message);
    }
  }
}

// Entry point
if (isMainThread) {
  const pgFolder = './postgres_sqls';
  const snowflakeFolder = './snowflake_sqls';
  const outputFolder = './comparison_results';
  
  compareSQLFiles(pgFolder, snowflakeFolder, outputFolder)
    .catch(console.error);
} else {
  workerMain();
}
