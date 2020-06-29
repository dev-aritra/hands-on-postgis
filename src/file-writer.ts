import fs from 'fs';
import { logCyan } from './logger';

export function writeFile(fileName: String, content : any) {
    const start = Date.now();
    fs.writeFileSync('./out/' +fileName + new Date().toLocaleString() + '.json',  content);
    logCyan('Wrote file ' + fileName + 'in ' + (Date.now() - start) + 'ms');
}