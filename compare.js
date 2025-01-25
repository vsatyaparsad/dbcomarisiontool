import pg from 'pg';
import Cursor from 'pg-cursor';
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
    const cursor = client.query(new Cursor(sql));
    
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
    console.log('Initializing Snowflake connection...');
    console.log('SQL Query:', sql);
    
    const connection = snowflake.createConnection(snowflakeConfig);
    
    connection.connect((err) => {
      if (err) {
        console.error('Snowflake connection error:', err);
        reject(err);
        return;
      }
      
      console.log('Snowflake connected successfully');

      // First, try to get the row count
      connection.execute({
        sqlText: `SELECT COUNT(*) as total FROM (${sql})`,
        complete: (err, stmt) => {
          if (err) {
            console.error('Error getting row count:', err);
          } else {
            stmt.fetchRows({
              each: (row) => {
                console.log('Expected row count:', row.TOTAL);
              },
              end: (err) => {
                if (err) {
                  console.error('Error fetching row count:', err);
                }
              }
            });
          }
        }
      });

      const statement = connection.execute({
        sqlText: sql,
        streamResult: true,
        complete: (err, stmt) => {
          if (err) {
            console.error('Snowflake execute error:', err);
            reject(err);
            return;
          }
          
          console.log('Snowflake query executed successfully');
          console.log('Statement status:', stmt.getStatus());
          console.log('Statement rowCount:', stmt.getRowCount());

          async function* generateChunks() {
            let chunk = [];
            let rowCount = 0;
            
            try {
              console.log('Starting to stream rows...');
              
              // Get stream and check if it's valid
              const stream = stmt.streamRows();
              if (!stream) {
                console.error('Stream is null or undefined');
                return;
              }

              console.log('Stream created successfully');
              
              for await (const row of stream) {
                rowCount++;
                if (rowCount === 1) {
                  console.log('First row data:', JSON.stringify(row, null, 2));
                }
                
                chunk.push(row);
                if (chunk.length >= CHUNK_SIZE) {
                  console.log(`Yielding chunk of ${chunk.length} rows. Total rows so far: ${rowCount}`);
                  yield chunk;
                  chunk = [];
                }
              }
              
              if (chunk.length > 0) {
                console.log(`Yielding final chunk of ${chunk.length} rows. Total rows: ${rowCount}`);
                yield chunk;
              }
              
              console.log(`Finished streaming. Total rows processed: ${rowCount}`);
            } catch (error) {
              console.error('Error while streaming rows:', error);
              console.error('Error details:', error.message);
              throw error;
            } finally {
              console.log('Closing Snowflake connection...');
              connection.destroy((err) => {
                if (err) {
                  console.error('Error while destroying connection:', err);
                } else {
                  console.log('Snowflake connection closed successfully');
                }
              });
            }
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
  const ws = XLSX.utils.aoa_to_sheet([headers]);
  XLSX.utils.book_append_sheet(wb, ws, `Chunk ${chunkNumber}`);
  return { wb, ws };
}

// Function to write comparison results in chunks
async function writeComparisonResults(pgResults, snowflakeResults, outputPath) {
  let currentChunk = 1;
  let rowsInCurrentSheet = 0;
  let { wb, ws } = createChunkWorkbook(currentChunk, ['Source', 'Status', ...Object.keys(pgResults[0] || {})]);

  const writeRow = (row, source, status) => {
    if (rowsInCurrentSheet >= MAX_ROWS_PER_SHEET) {
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

  // Process results
  const pgMap = new Map(pgResults.map(row => [JSON.stringify(row), row]));
  const snowflakeMap = new Map(snowflakeResults.map(row => [JSON.stringify(row), row]));

  // Write matched and unmatched Postgres records
  for (const row of pgResults) {
    const rowStr = JSON.stringify(row);
    const status = snowflakeMap.has(rowStr) ? 'Matched' : 'Postgres Only';
    writeRow(row, 'Postgres', status);
  }

  // Write Snowflake-only records
  for (const row of snowflakeResults) {
    const rowStr = JSON.stringify(row);
    if (!pgMap.has(rowStr)) {
      writeRow(row, 'Snowflake', 'Snowflake Only');
    }
  }

  // Save final workbook
  XLSX.writeFile(wb, `${outputPath}_part${currentChunk}.xlsx`);
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
        await writeComparisonResults(pgResults, snowflakeResults, outputBasePath);
        
        console.log(`Created comparison reports in ${outputFolder}`);
        
        // Generate summary
        const summary = {
          totalRecords: {
            postgres: pgResults.length,
            snowflake: snowflakeResults.length
          },
          matchedRecords: pgResults.filter(row => 
            snowflakeResults.some(sRow => JSON.stringify(row) === JSON.stringify(sRow))
          ).length
        };

        await fs.writeJSON(`${outputBasePath}_summary.json`, summary, { spaces: 2 });
        console.log('Summary:', summary);

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
