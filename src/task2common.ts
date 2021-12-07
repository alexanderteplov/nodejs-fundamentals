export const READ_STREAM_OPTIONS = { highWaterMark: 16 };
export const CONVERTING_OPTIONS = {
  delimiter: ';',
  checkType: true,
};

export const getTimeLabel = (label: string) => `\nEND writing ${label}`;

export const onError = (e: Error | null | undefined) => {
  console.error(e);
};

export const getLogger = (label: string, counter: number) => () => console.log(`${label} chunk ${++counter}`);
