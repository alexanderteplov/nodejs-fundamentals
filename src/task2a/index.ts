import { createReadStream, createWriteStream } from 'fs';
import path from 'path';
import csvToJson from 'csvtojson';

import {
  READ_STREAM_OPTIONS,
  CONVERTING_OPTIONS,
  getTimeLabel,
  getLogger,
  onError,
} from '../task2';
import { mkdirSafelySync } from '../utils';

import './env';

const CSV_DIR = path.join(__dirname, 'csv');
const OUTPUT_DIR = path.join(__dirname, 'output');

const CSV_FILE_PATH = path.join(CSV_DIR, 'data.csv');

const readStream1 = createReadStream(CSV_FILE_PATH, READ_STREAM_OPTIONS);
const readStream2 = createReadStream(CSV_FILE_PATH, READ_STREAM_OPTIONS);
const readStream3 = createReadStream(CSV_FILE_PATH, READ_STREAM_OPTIONS);
mkdirSafelySync(OUTPUT_DIR);
const writeFile1Stream = createWriteStream(path.join(OUTPUT_DIR, 'data-file-1.txt'));
const writeFile2Stream = createWriteStream(path.join(OUTPUT_DIR, 'data-file-2.txt'));
const writeFile3Stream = createWriteStream(path.join(OUTPUT_DIR, 'data-file-3.txt'));
const writeDbMockStream = createWriteStream(path.join(OUTPUT_DIR, 'data-db-mock.json'));

const commonLabel = 'all 4 tasks';
console.time(getTimeLabel(commonLabel));
console.log('\n');


const promise1 = new Promise((resolve) => {
  const label = 'file (with 2 pipe)';
  console.time(getTimeLabel(label));
  let i = 0;
  readStream1
    .on('data', getLogger(label, i))
    .on('error', onError)
    .on('end', () => {
      console.timeEnd(getTimeLabel(label));
      resolve(0);
    })
    .pipe(csvToJson(CONVERTING_OPTIONS))
    .pipe(writeFile1Stream);
});

const promise2 = new Promise((resolve) => {
  const label = 'file (with csvToJson.fromStream and pipe)';
  console.time(getTimeLabel(label));
  let i = 0;
  csvToJson(CONVERTING_OPTIONS, READ_STREAM_OPTIONS)
    .on('data', getLogger(label, i))
    .on('error', onError)
    .on('end', () => {
      console.timeEnd(getTimeLabel(label));
      resolve(0);
    })
    .fromStream(readStream2)
    .pipe(writeFile2Stream);
});

const promise3 = (async () => {
  const label = 'csv file (by iterating over readable stream)';
  console.time(getTimeLabel(label));
  let i = 0;
  try {
    for await (const chunk of readStream3) {
      writeFile3Stream.write(chunk);
      getLogger(label, i++)();
    }
  } catch (e) {
    console.error(e)
  }
  console.timeEnd(getTimeLabel(label));
})();

const promise4 = new Promise(async (resolve) => {
  const label = 'DB';
  console.time(getTimeLabel(label));
  let i = 0;
  await csvToJson(CONVERTING_OPTIONS, READ_STREAM_OPTIONS)
    .fromFile(CSV_FILE_PATH, READ_STREAM_OPTIONS)
    .subscribe((json) => {
      return new Promise((resolveChunk, rejectChunk) => {
        writeDbMockStream.write(JSON.stringify(json) + '\n', (error) => {
          if (error) {
            rejectChunk(error);
          } else {
            resolveChunk(json);
            getLogger(label, i++)();
          }
        })
      })
    }, onError, () => {
      console.timeEnd(getTimeLabel(label));
      resolve(0);
    });
});

await Promise.allSettled([
  promise1,
  promise2,
  promise3,
  promise4,
]);

console.timeEnd(getTimeLabel(commonLabel));

console.log('\nAll tasks are finished...');
