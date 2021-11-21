import { existsSync, mkdirSync } from 'fs';

export const mkdirSafelySync = (dir: string) => {
  if (!existsSync(dir)) {
    mkdirSync(dir);
  }
}
