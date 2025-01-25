import pg from 'pg';
import Cursor from 'pg-cursor';
import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';
import XLSX from 'xlsx';
import fs from 'fs-extra';
import path from 'path';

dotenv.config();

const CHUNK_SIZE = 100000;
const MAX_ROWS_PER_SHEET = 1000000;

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

async function* streamSnowflakeQuery(sql) {
  return new Promise((resolve, reject) => {
    console.log('Snowflake Config:', {
      account: snowflakeConfig.account,
      username: snowflakeConfig.username,
      database: snowflakeConfig.database,
      warehouse: snowflakeConfig.warehouse,
      schema: snowflakeConfig.schema
    });
    console.log('Executing SQL:', sql);

    const connection = snowflake.createConnection(snowflakeConfig);

    connection.connect((err) => {
      if (err) {
        console.error('Snowflake connection error:', err);
        reject(err);
        return;
      }

      console.log('Connected to Snowflake successfully');

      // Execute the main query
      connection.execute({
        sqlText: sql,
        complete: function(err, stmt, rows) {
          if (err) {
            console.error('Failed to execute query:', err);
            connection.destroy();
            reject(err);
            return;
          }

          console.log('Query executed successfully');
          console.log('Number of rows:', rows ? rows.length : 0);

          if (!rows || rows.length === 0) {
            console.log('No rows returned from query');
            connection.destroy();
            resolve([]);
            return;
          }

          // Process rows in chunks
          async function* generateChunks() {
            let chunk = [];
            let processedRows = 0;

            for (const row of rows) {
              chunk.push(row);
              processedRows++;

              if (chunk.length >= CHUNK_SIZE) {
                console.log(`Yielding chunk of ${chunk.length} rows. Total processed: ${processedRows}`);
                yield chunk;
                chunk = [];
              }
            }

            if (chunk.length > 0) {
              console.log(`Yielding final chunk of ${chunk.length} rows. Total processed: ${processedRows}`);
              yield chunk;
            }

            console.log('Finished processing all rows');
            connection.destroy();
          }

          resolve(generateChunks());
        }
      });
    });
  });
}

function createChunkWorkbook(chunkNumber, headers) {
  const wb = XLSX.utils.book_new();
  const ws = XLSX.utils.aoa_to_sheet([headers]);
  XLSX.utils.book_append_sheet(wb, ws, `Chunk ${chunkNumber}`);
  return { wb, ws };
}

async function writeComparisonResults(pgResults, snowflakeResults, outputPath) {
  console.log('Starting to write comparison results');
  console.log('Postgres results count:', pgResults.length);
  console.log('Snowflake results count:', snowflakeResults.length);

  let currentChunk = 1;
  let rowsInCurrentSheet = 0;
  
  // Ensure we have headers even if results are empty
  const headers = ['Source', 'Status', ...(pgResults[0] ? Object.keys(pgResults[0]) : 
                                        snowflakeResults[0] ? Object.keys(snowflakeResults[0]) : [])];
  
  let { wb, ws } = createChunkWorkbook(currentChunk, headers);

  const writeRow = (row, source, status) => {
    if (rowsInCurrentSheet >= MAX_ROWS_PER_SHEET) {
      console.log(`Saving workbook part ${currentChunk}`);
      XLSX.writeFile(wb, `${outputPath}_part${currentChunk}.xlsx`);
      currentChunk++;
      rowsInCurrentSheet = 0;
      ({ wb, ws } = createChunkWorkbook(currentChunk, headers));
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

  console.log('Processing Postgres records...');
  for (const row of pgResults) {
    const rowStr = JSON.stringify(row);
    const status = snowflakeMap.has(rowStr) ? 'Matched' : 'Postgres Only';
    writeRow(row, 'Postgres', status);
  }

  console.log('Processing Snowflake records...');
  for (const row of snowflakeResults) {
    const rowStr = JSON.stringify(row);
    if (!pgMap.has(rowStr)) {
      writeRow(row, 'Snowflake', 'Snowflake Only');
    }
  }

  console.log(`Saving final workbook part ${currentChunk}`);
  XLSX.writeFile(wb, `${outputPath}_part${currentChunk}.xlsx`);
}

async function compareSQLFiles(pgFolder, snowflakeFolder, outputFolder) {
  try {
    console.log('Starting comparison process...');
    console.log('Creating output folder:', outputFolder);
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
        console.log('Fetching data from databases...');
        
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
        throw error; // Re-throw to stop processing if there's an error
      }
    }

    console.log('\nComparison complete!');

  } catch (error) {
    console.error('Error during comparison:', error);
    throw error;
  }
}

// Example usage
const pgFolder = './postgres_sqls';
const snowflakeFolder = './snowflake_sqls';
const outputFolder = './comparison_results';

compareSQLFiles(pgFolder, snowflakeFolder, outputFolder).catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
