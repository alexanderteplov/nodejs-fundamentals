import { createReadStream, createWriteStream, WriteStream } from 'fs';
import { Stream, StreamOptions, Transform, Writable } from 'stream';
import path from 'path';
import csvToJson from 'csvtojson';

import { mkdirSafelySync } from '../utils';
import './env';
import { CONVERTING_OPTIONS, getLogger, getTimeLabel, onError } from '../task2common';
import { pipeline } from 'stream/promises';

const CSV_DIR = path.join(__dirname, 'csv');
const OUTPUT_DIR = path.join(__dirname, 'output');

const CSV_FILE_PATH = path.join(CSV_DIR, 'data.csv');

const getStreamOptions = (highWaterMark: number) => ({
  highWaterMark,
  objectMode: true,
}) as StreamOptions<Stream>;

mkdirSafelySync(OUTPUT_DIR);
const readStream = createReadStream(CSV_FILE_PATH, getStreamOptions(16));
const writeFileStream = createWriteStream(path.join(OUTPUT_DIR, 'data-file.txt'), getStreamOptions(4));

const transformStream = new Transform({
  transform: (json: Record<string, string>, _, callback) => {
    const formattedJson = Object.entries(json).reduce((acc, [key, value]) => {
      acc[key.toLowerCase()] = value;
      return acc;
    }, {} as Record<string, string>);
    callback(null, JSON.stringify(formattedJson) + '\n');
  },
  highWaterMark: 4,
  writableObjectMode: true,
});

let j = 0;
const writeToDbPromisifiedMockFunction = (data: any) => new Promise((resolve, reject) => {
  setTimeout(() => {
    if (Math.random() < 0.1) {
      reject('Error writing DB');
    } else {
      resolve(data);
      console.log(getLogger('DB', j++)());
    }
  }, 2);
});

let lastDbWritePromise: Promise<any> | null = null;
const writeDbStream = new Transform({
  transform: async (chunk: any, _, callback) => {
    callback(null, chunk);
    lastDbWritePromise = writeToDbPromisifiedMockFunction(chunk).catch(onError);
  },
  highWaterMark: 4,
  readableObjectMode: true,
}).on('end', () => {
  lastDbWritePromise!
    .then(() => console.timeEnd(getTimeLabel('DB')));
});

let i = 0;
const writeFileAndLogToConsoleStream = new Writable({
  write(chunk, _, callback) {
    if (writeFileStream.write(chunk)) {
      console.log(getLogger('file', i++)());
      process.nextTick(callback);
    } else {
      writeFileStream.once('drain', this.write);
    }
  }
})
  .on('finish', () => console.timeEnd(getTimeLabel('file')))
  .on('error', onError);

console.log('\n');
console.time(getTimeLabel('all'));
console.time(getTimeLabel('file'));
console.time(getTimeLabel('DB'));

try {
  await pipeline(
    readStream,
    csvToJson(CONVERTING_OPTIONS, getStreamOptions(16)),
    transformStream,
    writeDbStream,
    writeFileAndLogToConsoleStream,
  )
} catch (error) {
  console.error(error);
};

console.log('\nAll tasks are finished...');
console.timeEnd(getTimeLabel('all'));
