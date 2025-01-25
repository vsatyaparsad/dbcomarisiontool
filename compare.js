import pg from 'pg';
import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';
import XLSX from 'xlsx';
import fs from 'fs-extra';
import path from 'path';
import { Transform } from 'stream';
import { pipeline } from 'stream/promises';

dotenv.config();

const CHUNK_SIZE = 100000; // Process 100k records at a time
const MAX_ROWS_PER_SHEET = 1000000; // Excel limit per sheet

// Configure database connections
const pgConfig = {
  host: process.env.POSTGRES_HOST,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DATABASE,
  port: process.env.POSTGRES_PORT
};

const snowflakeConfig = {
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  database: process.env.SNOWFLAKE_DATABASE,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  schema: process.env.SNOWFLAKE_SCHEMA
};

// Function to stream Postgres query results
async function* streamPostgresQuery(sql) {
  const client = new pg.Client(pgConfig);
  try {
    await client.connect();
    const cursor = await client.query(new pg.Cursor(sql));
    
    while (true) {
      const rows = await cursor.read(CHUNK_SIZE);
      if (rows.length === 0) break;
      yield rows;
    }
  } finally {
    await client.end();
  }
}

// Function to stream Snowflake query results
async function* streamSnowflakeQuery(sql) {
  return new Promise((resolve, reject) => {
    const connection = snowflake.createConnection(snowflakeConfig);
    
    connection.connect((err) => {
      if (err) {
        reject(err);
        return;
      }

      const statement = connection.execute({
        sqlText: sql,
        streamResult: true,
        complete: (err, stmt) => {
          if (err) {
            reject(err);
            return;
          }

          async function* generateChunks() {
            let chunk = [];
            
            while (true) {
              const row = stmt.streamRows().next();
              if (row.done) {
                if (chunk.length > 0) {
                  yield chunk;
                }
                break;
              }
              
              chunk.push(row.value);
              if (chunk.length >= CHUNK_SIZE) {
                yield chunk;
                chunk = [];
              }
            }
            
            connection.destroy();
          }

          resolve(generateChunks());
        }
      });
    });
  });
}

// Function to create Excel workbook for a chunk of data
function createChunkWorkbook(chunkNumber, headers) {
  const wb = XLSX.utils.book_new();
  
  // Create summary sheet first
  const summaryWs = XLSX.utils.aoa_to_sheet([
    ['Comparison Summary'],
    [''],
    ['Category', 'Count', 'Percentage']
  ]);
  
  // Set column widths for summary sheet
  summaryWs['!cols'] = [
    { wch: 30 }, // Category column
    { wch: 15 }, // Count column
    { wch: 15 }  // Percentage column
  ];
  
  XLSX.utils.book_append_sheet(wb, summaryWs, 'Summary');
  
  // Create data sheet
  const ws = XLSX.utils.aoa_to_sheet([headers]);
  XLSX.utils.book_append_sheet(wb, ws, `Chunk ${chunkNumber}`);
  
  return { wb, ws, summaryWs };
}

// Function to update summary statistics
function updateSummarySheet(wb, stats) {
  const ws = wb.Sheets['Summary'];
  
  // Calculate percentages
  const totalRecords = Math.max(stats.totalRecords.postgres, stats.totalRecords.snowflake);
  const matchedPercentage = ((stats.matchedRecords / totalRecords) * 100).toFixed(2);
  const unmatchedPercentage = (100 - parseFloat(matchedPercentage)).toFixed(2);
  
  // Prepare summary data
  const summaryData = [
    ['Source Record Counts:', '', ''],
    ['Postgres Records', stats.totalRecords.postgres, '100%'],
    ['Snowflake Records', stats.totalRecords.snowflake, '100%'],
    ['', '', ''],
    ['Matching Analysis:', '', ''],
    ['Matched Records', stats.matchedRecords, `${matchedPercentage}%`],
    ['Unmatched Records', (totalRecords - stats.matchedRecords), `${unmatchedPercentage}%`],
    ['', '', ''],
    ['Unmatched Details:', '', ''],
    ['Records Only in Postgres', stats.postgresOnlyRecords, `${((stats.postgresOnlyRecords / stats.totalRecords.postgres) * 100).toFixed(2)}%`],
    ['Records Only in Snowflake', stats.snowflakeOnlyRecords, `${((stats.snowflakeOnlyRecords / stats.totalRecords.snowflake) * 100).toFixed(2)}%`]
  ];
  
  // Add data to summary sheet
  summaryData.forEach((row, idx) => {
    XLSX.utils.sheet_add_aoa(ws, [row], { origin: `A${idx + 4}` });
  });
}

// Function to write comparison results in chunks
async function writeComparisonResults(pgResults, snowflakeResults, outputPath) {
  let currentChunk = 1;
  let rowsInCurrentSheet = 0;
  let { wb, ws } = createChunkWorkbook(currentChunk, ['Source', 'Status', ...Object.keys(pgResults[0] || {})]);

  // Initialize statistics
  const stats = {
    totalRecords: {
      postgres: pgResults.length,
      snowflake: snowflakeResults.length
    },
    matchedRecords: 0,
    postgresOnlyRecords: 0,
    snowflakeOnlyRecords: 0
  };

  const writeRow = (row, source, status) => {
    if (rowsInCurrentSheet >= MAX_ROWS_PER_SHEET) {
      // Update summary before saving
      updateSummarySheet(wb, stats);
      
      // Save current workbook and create new one
      XLSX.writeFile(wb, `${outputPath}_part${currentChunk}.xlsx`);
      currentChunk++;
      rowsInCurrentSheet = 0;
      ({ wb, ws } = createChunkWorkbook(currentChunk, ['Source', 'Status', ...Object.keys(row)]));
    }

    const rowData = {
      Source: { v: source },
      Status: { 
        v: status,
        s: {
          fill: { fgColor: { rgb: status === 'Matched' ? "C6EFCE" : "FFC7CE" } },
          font: { color: { rgb: status === 'Matched' ? "006100" : "9C0006" } }
        }
      },
      ...Object.entries(row).reduce((acc, [key, value]) => {
        acc[key] = {
          v: value,
          s: {
            fill: { fgColor: { rgb: status === 'Matched' ? "C6EFCE" : "FFC7CE" } },
            font: { color: { rgb: status === 'Matched' ? "006100" : "9C0006" } }
          }
        };
        return acc;
      }, {})
    };

    XLSX.utils.sheet_add_json(ws, [rowData], { 
      skipHeader: true,
      origin: -1
    });
    rowsInCurrentSheet++;
  };

  // Process results and track statistics
  const pgMap = new Map(pgResults.map(row => [JSON.stringify(row), row]));
  const snowflakeMap = new Map(snowflakeResults.map(row => [JSON.stringify(row), row]));

  // Write matched and unmatched Postgres records
  for (const row of pgResults) {
    const rowStr = JSON.stringify(row);
    if (snowflakeMap.has(rowStr)) {
      stats.matchedRecords++;
      writeRow(row, 'Postgres', 'Matched');
    } else {
      stats.postgresOnlyRecords++;
      writeRow(row, 'Postgres', 'Postgres Only');
    }
  }

  // Write Snowflake-only records
  for (const row of snowflakeResults) {
    const rowStr = JSON.stringify(row);
    if (!pgMap.has(rowStr)) {
      stats.snowflakeOnlyRecords++;
      writeRow(row, 'Snowflake', 'Snowflake Only');
    }
  }

  // Update summary sheet with final statistics
  updateSummarySheet(wb, stats);

  // Save final workbook
  XLSX.writeFile(wb, `${outputPath}_part${currentChunk}.xlsx`);
  
  // Return statistics for logging
  return stats;
}

// Main comparison function
async function compareSQLFiles(pgFolder, snowflakeFolder, outputFolder) {
  try {
    await fs.ensureDir(outputFolder);
    const pgFiles = await fs.readdir(pgFolder);
    const sqlFiles = pgFiles.filter(file => file.endsWith('.sql'));

    console.log(`Found ${sqlFiles.length} SQL files to compare`);

    for (const sqlFile of sqlFiles) {
      console.log(`\nProcessing ${sqlFile}...`);

      const pgPath = path.join(pgFolder, sqlFile);
      const snowflakePath = path.join(snowflakeFolder, sqlFile);

      if (!await fs.pathExists(snowflakePath)) {
        console.log(`Warning: No matching Snowflake file found for ${sqlFile}`);
        continue;
      }

      const pgSQL = await fs.readFile(pgPath, 'utf8');
      const snowflakeSQL = await fs.readFile(snowflakePath, 'utf8');

      try {
        console.log('Fetching data in chunks...');
        
        let pgResults = [];
        let snowflakeResults = [];
        
        // Stream Postgres results
        for await (const chunk of streamPostgresQuery(pgSQL)) {
          pgResults.push(...chunk);
          console.log(`Processed ${pgResults.length} Postgres records...`);
        }

        // Stream Snowflake results
        for await (const chunk of await streamSnowflakeQuery(snowflakeSQL)) {
          snowflakeResults.push(...chunk);
          console.log(`Processed ${snowflakeResults.length} Snowflake records...`);
        }

        console.log('Creating comparison report...');
        const outputBasePath = path.join(outputFolder, path.parse(sqlFile).name);
        const stats = await writeComparisonResults(pgResults, snowflakeResults, outputBasePath);
        
        console.log('\nComparison Summary:');
        console.log('-------------------');
        console.log(`Total Postgres Records: ${stats.totalRecords.postgres}`);
        console.log(`Total Snowflake Records: ${stats.totalRecords.snowflake}`);
        console.log(`Matched Records: ${stats.matchedRecords}`);
        console.log(`Records Only in Postgres: ${stats.postgresOnlyRecords}`);
        console.log(`Records Only in Snowflake: ${stats.snowflakeOnlyRecords}`);
        console.log(`Match Percentage: ${((stats.matchedRecords / Math.max(stats.totalRecords.postgres, stats.totalRecords.snowflake)) * 100).toFixed(2)}%`);
        
        // Save summary as JSON
        await fs.writeJSON(`${outputBasePath}_summary.json`, stats, { spaces: 2 });

      } catch (error) {
        console.error(`Error comparing ${sqlFile}:`, error);
      }
    }

    console.log('\nComparison complete!');

  } catch (error) {
    console.error('Error during comparison:', error);
  }
}

// Example usage
const pgFolder = './postgres_sqls';
const snowflakeFolder = './snowflake_sqls';
const outputFolder = './comparison_results';

compareSQLFiles(pgFolder, snowflakeFolder, outputFolder).catch(console.error);