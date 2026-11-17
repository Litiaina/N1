// Browser adapter. Rust owns the private fault, processes and byte oracle.
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';

const access=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
const repo=process.env.N1_CONSOLE_REPO,dist=process.env.N1_CONSOLE_TEST_DIST;
if(!repo||!dist)throw new Error('Explicit Console repository and build required');
const {chromium,expect}=await import(pathToFileURL(path.join(repo,'node_modules/@playwright/test/index.mjs')).href);
const {serveConsole}=await import(pathToFileURL(path.join(repo,'scripts/serve-console.mjs')).href);
const host=await serveConsole({root:dist,n1:access.metadata,bind:'127.0.0.1',httpPort:access.console_port});
const origin=`http://127.0.0.1:${access.console_port}`;
const browser=await chromium.launch({headless:true});
try {
  const page=await browser.newPage({viewport:{width:1366,height:900}}),errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  await page.addInitScript(({access,origin})=>{
    localStorage.setItem('current-fragment',access.fragment);
    localStorage.setItem('current-token',access.token);
    localStorage.setItem('current-token-expires-at',String(Date.now()+3600000));
    localStorage.setItem('litiaina-console-active-connection-v2','n1|'+origin);
  },{access,origin});
  await page.goto(origin+'/?engine=n1');
  await page.waitForFunction(()=>Boolean(window.WindowManager)&&!document.querySelector('#boot-screen'));
  await page.evaluate(fragment=>{void window.showObjectRepairStatus(fragment,'status.bin');},access.fragment);
  const row=label=>page.locator('#objectStatusContent .grid').filter({has:page.locator('span').filter({hasText:new RegExp(`^${label}$`)})}).last();
  const panel=page.locator('#objectStatusContent');
  await expect(panel).toContainText('Degraded');
  await expect(row('Missing shards')).toContainText('1');
  await expect(row('Available shards')).toContainText('1');
  await expect(panel).toContainText('1+1');
  await expect(page.locator('#objectRepairBtn')).toBeEnabled();
  await page.screenshot({path:path.join(access.output,'object-status-missing.png')});
  await page.locator('#objectStatusMode').selectOption('checksum');
  await page.locator('#objectStatusModal').getByRole('button',{name:'Refresh',exact:true}).click();
  await expect(row('Check performed')).toContainText('Checksum');
  await expect(panel).toContainText('Degraded');
  await page.locator('#objectRepairBtn').click();
  await page.locator('#modalConfirm').click();
  await expect(panel).toContainText('checksum scan verified',{timeout:30000});
  await expect(row('Missing shards')).toContainText('0');
  await expect(row('Corrupt shards')).toContainText('0');
  await expect(row('Usable shards')).toContainText('2');
  await expect(row('Repaired shards')).toContainText('1');
  await expect(page.locator('#objectRepairBtn')).toBeDisabled();
  await page.screenshot({path:path.join(access.output,'object-status-repaired.png')});
  if(errors.length)throw new Error(errors.join('\n'));
  fs.writeFileSync(path.join(access.output,'object-status-browser-PASSED.json'),JSON.stringify({passed:true,actual_api:true,missing_slot_zero_counted:true,checksum_explicit:true,repair_refreshed:true}));
}finally {await browser.close();await host.close();}
