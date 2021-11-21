import { stdin, stdout } from 'process';
import { Transform } from 'stream';

const reverse = new Transform({
  transform: (chunk, _, done) => {
    const reversedSymbolsWithoutLineBreak = chunk.subarray(0, -1).reverse();
    chunk.set(reversedSymbolsWithoutLineBreak)
    done(null, chunk);
  }
});

stdin
  .pipe(reverse)
  .pipe(stdout)
