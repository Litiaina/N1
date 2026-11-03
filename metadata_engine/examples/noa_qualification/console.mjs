/** Real browser/production Console upload path. No routes, responses, fetch or
 * XHR are mocked. Rust owns the payload oracle and post-restart verification. */
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
const root=process.env.N1_QUAL_ROOT;
const count=Number(process.env.N1_QUAL_COUNT);
const baseURL=process.env.N1_QUAL_CONSOLE_URL;
const fragment=process.env.N1_QUAL_FRAGMENT;
const auth=JSON.parse(process.env.N1_QUAL_AUTH);
const repo=process.env.N1_QUAL_CONSOLE_REPO;
const {chromium}=await import(pathToFileURL(path.join(repo,'node_modules/@playwright/test/index.mjs')).href);
const campaign=path.join(root,'console-campaign.json');
const identity={count,fragment,payload:'u32-xor-v1-eight-sizes'};
if(fs.existsSync(campaign))assert.deepEqual(JSON.parse(fs.readFileSync(campaign,'utf8')),identity);else fs.writeFileSync(campaign,JSON.stringify(identity),{mode:0o600,flag:'wx'});
const journalPath=path.join(root,'console-acks-00.jsonl');
const prior=fs.existsSync(journalPath)?fs.readFileSync(journalPath,'utf8'):'';
const journal=fs.openSync(journalPath,'a',0o600);
const samples=fs.openSync(path.join(root,'console-browser-resources.jsonl'),'a',0o600);
const latency=fs.openSync(path.join(root,'console-latency-00.jsonl'),'a',0o600);
const bits=new Uint8Array(Math.ceil(count/8));
let acknowledged=0,requests=0,chain=Promise.resolve();const failures=[];
for(const line of prior.split('\n').filter(Boolean))for(const ack of JSON.parse(line)){assert(ack.index>=0&&ack.index<count);assert.match(ack.version,/^[0-7][0-9A-HJKMNP-TV-Z]{25}$/);const byte=ack.index>>>3,mask=1<<(ack.index&7);assert.equal(bits[byte]&mask,0);bits[byte]|=mask;acknowledged++;}
const missing=[];for(let i=0;i<count;i++)if(!(bits[i>>>3]&(1<<(i&7))))missing.push(i);
const priorAcknowledged=acknowledged;
if(missing.length===0){
 const report=path.join(root,'console-browser-PASSED.json');
 if(fs.existsSync(report))fs.copyFileSync(report,path.join(root,`console-browser-prior-${Date.now()}.json`),fs.constants.COPYFILE_EXCL);
 fs.writeFileSync(report,JSON.stringify({passed:true,count,priorAcknowledged,acknowledged,requests:0,seconds:0,resumedWithoutUploads:true,transport:'All real console ACKs already journaled; Rust must still reopen, census and read every object'},null,2));
 fs.closeSync(journal);fs.closeSync(samples);fs.closeSync(latency);
}else{
const browser=await chromium.launch({headless:true,downloadsPath:path.join(root,'browser-downloads')});
try{
 const context=await browser.newContext({viewport:{width:1366,height:768},ignoreHTTPSErrors:true});
 const page=await context.newPage();
 page.on('pageerror',error=>{failures.push(error.message);});
 page.on('response',response=>{
  const uri=new URL(response.url());if(response.request().method()!=='POST'||!(uri.pathname==='/noa/v1/upload/batch'||uri.pathname.startsWith('/noa/v1/upload/one-shot/')))return;
  requests++;
  chain=chain.then(async()=>{
   const payload=await response.json();if(response.status()!==201){throw new Error(`Real upload returned ${response.status()}: ${JSON.stringify(payload)}`);}
   const rows=Array.isArray(payload.results)?payload.results:[payload];const acks=[];
   for(const row of rows){assert.equal(row.status??201,201);assert.match(row.object_key,/^qual\/console\/\d{9}\.bin$/);const index=Number(row.object_key.slice('qual/console/'.length,-4));assert(index>=0&&index<count);const byte=index>>>3,mask=1<<(index&7);assert.equal(bits[byte]&mask,0,'duplicate real object acknowledgement');bits[byte]|=mask;assert.match(row.version_id,/^[0-7][0-9A-HJKMNP-TV-Z]{25}$/);acks.push({index,version:row.version_id});}
   fs.writeSync(journal,JSON.stringify(acks)+'\n');fs.fsyncSync(journal);acknowledged+=acks.length;const timing=response.request().timing();fs.writeSync(latency,JSON.stringify({count:acks.length,milliseconds:timing.responseEnd})+'\n');
  }).catch(error=>{failures.push(error.stack||error.message);});
 });
 await page.addInitScript(({url,fragment,auth})=>{
  window.LITIAINA_CONSOLE_CONFIG={serverUrl:url,engine:'n1'};
  localStorage.setItem('litiaina-console-active-connection-v2',`n1|${location.origin}`);localStorage.setItem('litiaina-console-n1-upload-mode','standard');localStorage.setItem('current-fragment',fragment);localStorage.setItem('current-fragment-name','Real full stack qualification');
  localStorage.setItem('current-token',auth.access_token);localStorage.setItem('current-token-expires-at',String(Date.now()+auth.expires_in*1000));
  localStorage.setItem('current-refresh-token',auth.refresh_token);localStorage.setItem('current-refresh-token-expires-at',String(Date.now()+auth.refresh_expires_in*1000));
 },{url:baseURL,fragment,auth});
 await page.goto(baseURL+'/?engine=n1',{waitUntil:'domcontentloaded'});
 await page.waitForFunction(()=>Boolean(window.ConsoleUploads)&&Boolean(window.WindowManager)&&!document.querySelector('#boot-screen'),{timeout:60000});
 await page.evaluate(()=>window.WindowManager.open('file-explorer'));
 const cdp=await context.newCDPSession(page);await cdp.send('Runtime.enable');
 const job=await page.evaluate(({count,missing})=>{
  const sizes=[0,1,127,1024,4096,8193,32768,65536];let generated=0;
  window.__realQualification={generated:0,beats:0,maxGap:0,last:performance.now()};setInterval(()=>{const q=window.__realQualification;const now=performance.now();q.maxGap=Math.max(q.maxGap,now-q.last);q.last=now;q.beats++;},50);
  const files=new Proxy({length:missing.length},{get(target,property){if(property==='length')return missing.length;if(typeof property!=='string'||!/^\d+$/.test(property))return Reflect.get(target,property);const rank=Number(property);if(rank>=missing.length)return undefined;const index=missing[rank];generated++;window.__realQualification.generated=generated;
   const bytes=new Uint8Array(sizes[index%8]);const seed=(Math.imul(index,131)^((index<<17)|(index>>>15)))>>>0;for(let offset=0;offset<bytes.length;offset++)bytes[offset]=((seed^Math.imul(offset,17))>>>0)%251;
   const name=String(index).padStart(9,'0')+'.bin';const file=new File([bytes],name,{type:'application/octet-stream',lastModified:1700000000000});Object.defineProperty(file,'webkitRelativePath',{value:`qual/console/${name}`});return file;
  }});
  return window.ConsoleUploads.enqueue({files},'',{conflictPolicy:'versions',name:`${count} real HDD objects`});
 },{count,missing});
 assert.equal(typeof job,'string');const started=Date.now();let lastLog=0;
 for(;;){
  await new Promise(resolve=>setTimeout(resolve,1000));await chain;
  const state=await page.evaluate(()=>({stats:window.ConsoleUploads.stats(),probe:window.__realQualification,rows:document.querySelectorAll('#objectList .file-item').length,cards:document.querySelectorAll('#upload-items-container > div').length}));
  const heap=await cdp.send('Runtime.getHeapUsage');const sample={elapsedSeconds:(Date.now()-started)/1000,acknowledged,requests,heap,state};fs.writeSync(samples,JSON.stringify(sample)+'\n');
  assert.equal(failures.length,0,failures.join('\n'));const current=state.stats.jobs.find(j=>j.id===job);assert(current,'production upload job disappeared');assert.equal(current.failedCount,0,JSON.stringify(current));assert.equal(current.unknownCount,0,JSON.stringify(current));assert.equal(current.scanErrors,0,JSON.stringify(current));
  if(Date.now()-lastLog>=10000){console.log(JSON.stringify({event:'real-console-upload-progress',acknowledged,count,generated:state.probe.generated,requests,retained:state.stats.retainedItems,heapMiB:Math.round(heap.usedSize/1048576),elapsedSeconds:sample.elapsedSeconds}));lastLog=Date.now();}
  if(current.discoveryDone&&current.processedCount===missing.length&&state.stats.activeRequests===0&&state.stats.activeSources===0){assert.equal(acknowledged,count);assert.equal(state.probe.generated,missing.length);await page.screenshot({path:path.join(root,'console-completed.png')});fs.writeFileSync(path.join(root,'console-browser-PASSED.json'),JSON.stringify({passed:true,count,priorAcknowledged,acknowledged,requests,seconds:sample.elapsedSeconds,heap,state,transport:'real production XHR/FormData through fixed Console proxy -> metadata -> native LINK'},null,2));break;}
  assert(Date.now()-started<20*3600000,'full stack browser campaign exceeded twenty hours');
 }
 await chain;
}finally{fs.closeSync(journal);fs.closeSync(samples);fs.closeSync(latency);await browser.close();}
}
