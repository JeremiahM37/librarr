// Write the page the Go server serves: server.html with bundle URLs versioned
// by content. The Go file server caches assets for an hour, so a refreshed
// page always requests the bundle built with its HTML.
import {readFile,writeFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
const template=new URL('../server.html',import.meta.url);
const entry=new URL('../../static/react/index.html',import.meta.url);
let html=await readFile(template,'utf8');
for(const name of ['librarr.js','librarr.css']){
 const bytes=await readFile(new URL(`../../static/react/${name}`,import.meta.url));
 const hash=createHash('sha256').update(bytes).digest('hex').slice(0,16);
 const pattern=new RegExp(`/static/react/${name.replace('.', '\\.')}[^"']*`,'g');
 html=html.replace(pattern,`/static/react/${name}?v=${hash}`);
}
await writeFile(entry,html);
