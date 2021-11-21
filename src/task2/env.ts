import { fileURLToPath } from 'url';
import path from 'path';

global.__filename = fileURLToPath(import.meta.url); // eslint-disable-line
global.__dirname = path.dirname(__filename); // eslint-disable-line
