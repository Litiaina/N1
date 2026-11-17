// Browser automation drives the shipped UI. Rust owns services and recovery.
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {spawn} from 'node:child_process';
const access=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
const repo=process.env.N1_CONSOLE_REPO,dist=process.env.N1_CONSOLE_TEST_DIST;
if(!repo||!dist)throw new Error('Explicit original Console repository/build required');
const {chromium,expect}=await import(pathToFileURL(path.join(repo,'node_modules/@playwright/test/index.mjs')).href);
const {serveConsole}=await import(pathToFileURL(path.join(repo,'scripts/serve-console.mjs')).href);
const host=await serveConsole({root:dist,n1:access.metadata,bind:'127.0.0.1',httpPort:access.console_port});
const origin=`http://127.0.0.1:${access.console_port}`;
const browser=await chromium.launch({headless:true});
try {
  const context=await browser.newContext({viewport:{width:1366,height:900},acceptDownloads:true});
  const page=await context.newPage(),errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  await page.addInitScript(({access,origin})=>{
    localStorage.setItem('current-fragment',access.fragment);localStorage.setItem('current-fragment-name','Whole-product witness');
    localStorage.setItem('current-token',access.token);localStorage.setItem('current-token-expires-at',String(Date.now()+3600000));
    localStorage.setItem('litiaina-console-active-connection-v2','n1|'+origin);
  },{access,origin});
  await page.goto(origin+'/?engine=n1');
  await page.waitForFunction(()=>Boolean(window.WindowManager)&&!document.querySelector('#boot-screen'));
  await page.evaluate(()=>{window.WindowManager.open('dashboard');(0,eval)('ResMonitor.showTab("health")');});
  await expect(page.locator('#health-metadata-backups')).toBeVisible();
  await page.getByRole('button',{name:'List M1 snapshots',exact:true}).click();
  await expect(page.locator('#metadata-backup-status')).toContainText('Enter the M1 master key');
  await page.locator('#metadata-operator-key').fill('incorrect-test-master');
  await page.locator('#metadata-operator-authorize').click();
  await expect(page.locator('#metadata-operator-status')).toContainText('Master key was not accepted');
  expect(await page.evaluate(()=>localStorage.getItem('current-token'))).toBe(access.token);
  await page.locator('#metadata-operator-key').fill('test-only-master');
  await page.locator('#metadata-operator-authorize').click();
  await expect(page.locator('#metadata-operator-status')).toContainText('Operator authorized');
  await expect(page.locator('#metadata-backup-list')).toContainText('No completed metadata snapshots');
  page.on('dialog',dialog=>dialog.accept());
  await page.locator('#metadata-backup-create').click();
  await expect(page.locator('#metadata-backup-list')).toContainText('Download snapshot',{timeout:60000});
  await expect(page.locator('#metadata-backup-status')).toContainText('completed',{timeout:60000});
  const downloadPending=page.waitForEvent('download');
  await page.locator('#metadata-backup-list button').first().click();
  const download=await downloadPending;
  expect(await download.failure()).toBeNull();
  await download.saveAs(path.join(access.output,'console-downloaded.n1meta'));
  await expect(page.locator('#metadata-operator-key')).toHaveValue('');
  expect(await page.evaluate(()=>localStorage.getItem('current-token'))).toBe(access.token);
  expect(await page.evaluate(()=>JSON.stringify({...localStorage}))).not.toContain('test-only-master');
  await page.screenshot({path:path.join(access.output,'console-metadata-backup.png'),fullPage:true});
  await page.locator('#metadata-operator-lock').click();
  await expect(page.locator('#metadata-backup-list')).toBeEmpty();
  if(errors.length)throw new Error(errors.join('\n'));
  await context.close();
  // Run the existing complete-product browser cases without opt-in skips.
  const config=path.join(access.output,'console-live.config.mjs');
  fs.writeFileSync(config,`export default ${JSON.stringify({testDir:path.join(repo,'tests'),testMatch:['live-local.spec.ts','live-reload-resume.spec.ts'],timeout:180000,retries:0,workers:1,outputDir:path.join(access.output,'browser-live'),reporter:[['list'],['json',{outputFile:path.join(access.output,'browser-live.json')}]],use:{baseURL:origin,browserName:'chromium',screenshot:'only-on-failure',trace:'retain-on-failure'},projects:[{name:'desktop-1366x768',use:{viewport:{width:1366,height:768}}}]})};\n`);
  const log=fs.openSync(path.join(access.output,'console-original-live.log'),'w');
  try {
    await new Promise((resolve,reject)=>{
      const child=spawn(process.execPath,[path.join(repo,'node_modules/@playwright/test/cli.js'),'test','--config',config],{cwd:repo,env:{...process.env,N1_LIVE_URL:origin,N1_CONSOLE_LIVE_URL:origin,N1_LIVE_FRAGMENT:access.fragment,N1_LIVE_FRAGMENT_KEY:'test-only-fragment'},stdio:['ignore',log,log]});
      child.on('error',reject);child.on('exit',code=>code===0?resolve():reject(new Error('Original Console live checks failed: '+fs.readFileSync(path.join(access.output,'console-original-live.log'),'utf8'))));
    });
  }finally{fs.closeSync(log);}
  fs.writeFileSync(path.join(access.output,'console-workflows-PASSED.json'),JSON.stringify({passed:true,real_master_authorization:true,mounted_session_preserved:true,snapshot_created_listed_downloaded:true,original_browser_live_cases:4},null,2));
}finally{await browser.close();await host.close();}
