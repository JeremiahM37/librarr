// The Go file server caches assets for an hour. Version fixed bundle URLs by
// content so a refreshed page always requests the bundle built with its HTML.
import {readFile,writeFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
const entry=new URL('../../index.html',import.meta.url);
let html=await readFile(entry,'utf8');
for(const name of ['librarr.js','librarr.css']){
 const bytes=await readFile(new URL(`../../static/react/${name}`,import.meta.url));
 const hash=createHash('sha256').update(bytes).digest('hex').slice(0,16);
 const pattern=new RegExp(`/static/react/${name.replace('.', '\\.')}[^"']*`,'g');
 html=html.replace(pattern,`/static/react/${name}?v=${hash}`);
}
await writeFile(entry,html);
