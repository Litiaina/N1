// The original Console drives real NOA jobs; Rust owns payloads and the oracle.
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
const access=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
const phase=process.argv[3];
const repo=process.env.N1_CONSOLE_REPO;
if(!repo||!process.env.N1_CONSOLE_TEST_DIST)throw new Error('Explicit Console repository and built assets required');
const {chromium}=await import(pathToFileURL(path.join(repo,'node_modules/@playwright/test/index.mjs')).href);
const {serveConsole}=await import(pathToFileURL(path.join(repo,'scripts/serve-console.mjs')).href);
const host=await serveConsole({root:process.env.N1_CONSOLE_TEST_DIST,n1:access.metadata,bind:'127.0.0.1',httpPort:access.console_port});
const browser=await chromium.launch({headless:true});
try {
  const context=await browser.newContext({viewport:{width:1440,height:1000}});
  const page=await context.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
  const origin=`http://127.0.0.1:${access.console_port}`;
  await page.addInitScript(({access,origin})=>{
    localStorage.setItem('current-fragment',access.fragment);localStorage.setItem('current-fragment-name','Console maintenance witness');
    localStorage.setItem('current-token',access.token);localStorage.setItem('current-token-expires-at',String(Date.now()+3600000));
    localStorage.setItem('litiaina-console-active-connection-v2','n1|'+origin);localStorage.setItem('litiaina-console-server-url:n1',origin);
  },{access,origin});
  await page.goto(origin+'/?engine=n1');
  await page.waitForFunction(()=>Boolean(window.WindowManager)&&!document.querySelector('#boot-screen'));
  await page.evaluate(()=>{window.WindowManager.open('dashboard');(0,eval)('ResMonitor.showTab("health")');});
  await page.waitForSelector('[data-service-role="metadata"]');
  await page.evaluate(()=>(0,eval)('ResMonitor.showTab("maintenance")'));
  if(phase==='migration') {
    await page.waitForSelector(`#link-source-pool option[value="${access.source}"]`,{state:'attached'});
    await page.selectOption('#link-source-pool',access.source);await page.selectOption('#link-target-pool',access.target);
    await page.locator('#redist-dry-run').uncheck({force:true});
    await page.locator('#redist-concurrency').fill('1');
    await page.locator('#btn-trigger-redist').click();
  } else {await page.locator('#btn-trigger-gc').click();}
  const kind=phase==='migration'?'redistribution':'gc';
  const deadline=Date.now()+60000;
  let terminal;
  while(Date.now()<deadline) {
    terminal=await page.evaluate(async({kind,token})=>{const r=await fetch(`/noa/v1/maintenance/${kind}/status`,{headers:{authorization:`Bearer ${token}`}});if(!r.ok)throw new Error('status '+r.status);return r.json();},{kind,token:access.token});
    if(terminal.phase==='completed'&&!terminal.running)break;
    if(terminal.phase==='incomplete')throw new Error(JSON.stringify(terminal));
    await new Promise(resolve=>setTimeout(resolve,100));
  }
  if(terminal?.phase!=='completed')throw new Error('Job did not complete: '+JSON.stringify(terminal));
  if(errors.length)throw new Error(errors.join('\n'));
  await page.screenshot({path:path.join(access.output,`console-${phase}.png`),fullPage:true});
  fs.writeFileSync(path.join(access.output,`console-${phase}-PASSED.json`),JSON.stringify({passed:true,phase,terminal,errors},null,2));
  console.log('Real Console maintenance passed:',phase);
}finally {await browser.close();await host.close();}
