import pg from 'pg';
import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';
import ExcelJS from 'exceljs';
import fs from 'fs-extra';
import path from 'path';
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';

dotenv.config();

// Configure database connections
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

const QUERY_TIMEOUT = 30000; // 30 seconds

// Function to execute Postgres query in batches
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

// Function to execute Snowflake query in batches
async function executeSnowflakeQueryBatch(sql, batchSize, offset) {
  return new Promise((resolve, reject) => {
    const connection = snowflake.createConnection(snowflakeConfig);

    connection.connect((err) => {
      if (err) {
        reject(err);
        return;
      }

      const batchSQL = `${sql} LIMIT ${batchSize} OFFSET ${offset}`;
      connection.execute({
        sqlText: batchSQL,
        complete: (err, stmt, rows) => {
          connection.destroy();
          if (err) {
            reject(err);
            return;
          }
          resolve(rows);
        },
      });
    });
  });
}

// Worker function for parallel processing
function workerFunction() {
  const { outputPath, pgSQL, snowflakeSQL, batchSize } = workerData;

  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: outputPath });

  const pgSheet = workbook.addWorksheet('Postgres Data');
  const snowflakeSheet = workbook.addWorksheet('Snowflake Data');
  const comparisonSheet = workbook.addWorksheet('Detailed Comparison');
  const summarySheet = workbook.addWorksheet('Summary');

  let pgHeaders = [];
  let snowflakeHeaders = [];
  let comparisonHeaders = [];
  let offset = 0;

  let matchedCount = 0;
  let postgresOnlyCount = 0;
  let snowflakeOnlyCount = 0;

  const processBatch = async () => {
    const [pgBatch, snowflakeBatch] = await Promise.all([
      executePostgresQueryBatch(pgSQL, batchSize, offset),
      executeSnowflakeQueryBatch(snowflakeSQL, batchSize, offset),
    ]);

    if (pgBatch.length === 0 && snowflakeBatch.length === 0) {
      parentPort.postMessage({ done: true });
      return;
    }

    if (offset === 0) {
      pgHeaders = Object.keys(pgBatch[0] || {});
      snowflakeHeaders = Object.keys(snowflakeBatch[0] || {});
      comparisonHeaders = [...pgHeaders, 'Status'];

      pgSheet.columns = pgHeaders.map((key) => ({ header: key, key }));
      snowflakeSheet.columns = snowflakeHeaders.map((key) => ({ header: key, key }));
      comparisonSheet.columns = comparisonHeaders.map((key) => ({ header: key, key }));
    }

    pgBatch.forEach((row) => pgSheet.addRow(row).commit());
    snowflakeBatch.forEach((row) => snowflakeSheet.addRow(row).commit());

    const snowflakeMap = new Map(snowflakeBatch.map((row) => [JSON.stringify(row), row]));

    pgBatch.forEach((pgRow) => {
      const match = snowflakeMap.get(JSON.stringify(pgRow));
      const status = match ? 'Matched' : 'Postgres Only';

      const rowData = { ...pgRow, Status: status };
      const row = comparisonSheet.addRow(rowData);

      Object.keys(pgRow).forEach((key) => {
        const cell = row.getCell(key);
        cell.fill = {
          type: 'pattern',
          pattern: 'solid',
          fgColor: { argb: match ? 'C6EFCE' : 'FFC7CE' },
        };
        cell.font = { color: { argb: match ? '006100' : '9C0006' } };
      });

      const statusCell = row.getCell('Status');
      statusCell.fill = {
        type: 'pattern',
        pattern: 'solid',
        fgColor: { argb: match ? 'C6EFCE' : 'FFC7CE' },
      };
      statusCell.font = { color: { argb: match ? '006100' : '9C0006' } };

      if (match) {
        matchedCount++;
      } else {
        postgresOnlyCount++;
      }

      row.commit();
    });

    snowflakeBatch.forEach((snowflakeRow) => {
      if (!pgBatch.some((pgRow) => JSON.stringify(pgRow) === JSON.stringify(snowflakeRow))) {
        const rowData = { ...snowflakeRow, Status: 'Snowflake Only' };
        const row = comparisonSheet.addRow(rowData);

        Object.keys(snowflakeRow).forEach((key) => {
          const cell = row.getCell(key);
          cell.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: 'FFC7CE' },
          };
          cell.font = { color: { argb: '9C0006' } };
        });

        const statusCell = row.getCell('Status');
        statusCell.fill = {
          type: 'pattern',
          pattern: 'solid',
          fgColor: { argb: 'FFC7CE' },
        };
        statusCell.font = { color: { argb: '9C0006' } };

        snowflakeOnlyCount++;
        row.commit();
      }
    });

    offset += batchSize;
    processBatch();
  };

  processBatch().catch((err) => {
    parentPort.postMessage({ error: err });
  });

  parentPort.on('message', async (message) => {
    if (message.done) {
      summarySheet.addRow(['Metric', 'Value']).commit();
      summarySheet.addRow(['Postgres Row Count', matchedCount + postgresOnlyCount]).commit();
      summarySheet.addRow(['Snowflake Row Count', matchedCount + snowflakeOnlyCount]).commit();
      summarySheet.addRow(['Matched Records', matchedCount]).commit();
      summarySheet.addRow(['Postgres Only Records', postgresOnlyCount]).commit();
      summarySheet.addRow(['Snowflake Only Records', snowflakeOnlyCount]).commit();
      summarySheet.addRow(['Row Count Difference', postgresOnlyCount - snowflakeOnlyCount]).commit();

      await workbook.commit();
      parentPort.postMessage({ done: true });
    }
  });
}

// Main comparison function
async function compareSQLFiles(pgFolder, snowflakeFolder, outputFolder, batchSize = 5000) {
  try {
    await fs.ensureDir(outputFolder);

    const pgFiles = await fs.readdir(pgFolder);
    const sqlFiles = pgFiles.filter((file) => file.endsWith('.sql'));

    console.log(`Found ${sqlFiles.length} SQL files to compare`);

    const MAX_CONCURRENT_FILES = 3;
    const fileChunks = [];

    for (let i = 0; i < sqlFiles.length; i += MAX_CONCURRENT_FILES) {
      fileChunks.push(sqlFiles.slice(i, i + MAX_CONCURRENT_FILES));
    }

    for (const chunk of fileChunks) {
      await Promise.all(
        chunk.map(async (sqlFile) => {
          try {
            console.log(`Processing file: ${sqlFile}...`);

            const pgPath = path.join(pgFolder, sqlFile);
            const snowflakePath = path.join(snowflakeFolder, sqlFile);

            if (!(await fs.pathExists(snowflakePath))) {
              console.warn(`No matching Snowflake file found for ${sqlFile}`);
              return;
            }

            const pgSQL = await fs.readFile(pgPath, 'utf8');
            const snowflakeSQL = await fs.readFile(snowflakePath, 'utf8');

            const outputPath = path.join(outputFolder, `${path.parse(sqlFile).name}_comparison.xlsx`);

            const worker = new Worker(__filename, {
              workerData: { outputPath, pgSQL, snowflakeSQL, batchSize },
            });

            worker.on('message', (message) => {
              if (message.done) {
                console.log(`Completed file: ${sqlFile}`);
              } else if (message.error) {
                console.error(`Error processing file ${sqlFile}:`, message.error);
              }
            });

            worker.on('error', (error) => {
              console.error(`Worker error for file ${sqlFile}:`, error);
            });

            worker.on('exit', (code) => {
              if (code !== 0) {
                console.error(`Worker stopped with exit code ${code} for file ${sqlFile}`);
              }
            });
          } catch (error) {
            console.error(`Error processing file ${sqlFile}:`, error);
          }
        })
      );
    }

    console.log('Comparison complete!');
  } catch (error) {
    console.error('Error during comparison:', error);
  }
}

if (isMainThread) {
  // Example usage
  const pgFolder = './postgres_sqls'; // Folder containing Postgres SQL files
  const snowflakeFolder = './snowflake_sqls'; // Folder containing Snowflake SQL files
  const outputFolder = './comparison_results'; // Folder for Excel reports

  compareSQLFiles(pgFolder, snowflakeFolder, outputFolder).catch(console.error);
} else {
  workerFunction();
}
