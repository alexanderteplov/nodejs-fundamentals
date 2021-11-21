import { createReadStream, createWriteStream } from 'fs';
import path from 'path';
import csvToJson from 'csvtojson';
import { mkdirSafelySync } from '../utils';

import './env';

const CSV_DIR = path.join(__dirname, 'csv');
const OUTPUT_DIR = path.join(__dirname, 'output');

const CSV_FILE_PATH = path.join(CSV_DIR, 'data.csv');
export const READ_STREAM_OPTIONS = { highWaterMark: 16 };
export const CONVERTING_OPTIONS = {
  delimiter: ';',
  checkType: true,
};

const readStream = createReadStream(CSV_FILE_PATH, READ_STREAM_OPTIONS);
mkdirSafelySync(OUTPUT_DIR);
const writeStream = createWriteStream(path.join(OUTPUT_DIR, 'data-file.txt'));

export const getTimeLabel = (label: string) => `\nEND writing ${label}`;

export const onError = (e: Error | null | undefined) => {
  console.error(e);
};

export const getLogger = (label: string, counter: number) => () => console.log(`${label} chunk ${++counter}`);

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
