import { createReadStream, createWriteStream } from 'fs';
import path from 'path';
import csvToJson from 'csvtojson';
import { mkdirSafelySync } from '../utils';

import './env';
import { CONVERTING_OPTIONS, getLogger, getTimeLabel, onError, READ_STREAM_OPTIONS } from '../task2common';

const CSV_DIR = path.join(__dirname, 'csv');
const OUTPUT_DIR = path.join(__dirname, 'output');

const CSV_FILE_PATH = path.join(CSV_DIR, 'data.csv');


const readStream = createReadStream(CSV_FILE_PATH, READ_STREAM_OPTIONS);
mkdirSafelySync(OUTPUT_DIR);
const writeStream = createWriteStream(path.join(OUTPUT_DIR, 'data-file.txt'));

console.log('\n');

await new Promise((resolve) => {
  const label = 'file';
  console.time(getTimeLabel(label));
  let i = 0;
  readStream
    .on('data', getLogger(label, i))
    .on('error', onError)
    .on('end', () => {
      console.timeEnd(getTimeLabel(label));
      resolve(0);
    })
    .pipe(csvToJson(CONVERTING_OPTIONS))
    .pipe(writeStream);
});

console.log('\nAll tasks are finished...');
