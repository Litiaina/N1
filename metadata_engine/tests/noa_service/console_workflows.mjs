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

async function checkNamespaceEvents(page, request, fragment, token) {
  const endpoint = new URL('/noa/v1/ws', access.metadata);
  endpoint.protocol = endpoint.protocol === 'https:' ? 'wss:' : 'ws:';
  endpoint.searchParams.set('token', token);
  await page.evaluate(url => new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    window.qualificationEvents = [];
    socket.onmessage = event => window.qualificationEvents.push(JSON.parse(event.data));
    socket.onopen = resolve;
    socket.onerror = () => reject(new Error('M1 event subscription failed'));
  }), endpoint.href);
  for (const [path, credential] of [['other', access.token], ['own', token]]) {
    const response = await request.post(origin + `/noa/v1/upload/one-shot/console-event-${path}.bin`, {
      headers: { authorization: `Bearer ${credential}`, 'content-type': 'application/octet-stream' },
      data: Buffer.from(path)
    });
    expect(response.status()).toBe(201);
  }
  await expect.poll(() => page.evaluate(() => window.qualificationEvents)).toEqual([{
    type: 'EVENT', payload: { event: 'NAMESPACE_CHANGED', fragment }
  }]);
}

async function checkFragmentCreation() {
  const context = await browser.newContext({ viewport: { width: 1366, height: 900 } });
  try {
    const page = await context.newPage();
    await page.goto(origin + '/?engine=n1');
    await page.waitForFunction(() => Boolean(window.loadFragments) && !document.querySelector('#boot-screen'));
    expect(await page.evaluate(() => localStorage.getItem('current-fragment'))).toBeNull();
    await page.evaluate(() => (0, eval)('openFragmentManager("create")'));
    const form = page.locator('#frag-tab-create');
    await form.locator('#mountFragmentNameInput').fill('Console-created fragment');
    await expect(form.locator('#createFragmentHashPreview')).toHaveText(/^[0-9a-f]{64}$/);
    await form.locator('#mountFragmentKeyInput').fill('test-only-created-fragment');
    const master = form.locator('#mountMasterSecretInput');
    const submit = form.locator('#mountFragmentActionBtn');
    const created = response =>
      new URL(response.url()).pathname === '/noa/v1/fragments' && response.request().method() === 'POST';
    await master.fill('incorrect-test-master');
    const denied = page.waitForResponse(created);
    await submit.click();
    const rejected = await denied;
    expect(rejected.status()).toBe(401);
    expect(await rejected.json()).toMatchObject({ error: 'Authentication required' });
    const createError = page.getByRole('alert').filter({
      hasText: /Failed to create disk \(401[^)]*\): Authentication required/
    });
    await expect(createError).toBeVisible();
    expect(await page.evaluate(() => localStorage.getItem('current-fragment'))).toBeNull();
    await master.fill('test-only-master');
    const accepted = page.waitForResponse(created);
    await submit.click();
    const response = await accepted;
    expect(response.status()).toBe(201);
    const identity = await response.json();
    expect(identity.fragment_hash).toMatch(/^[0-9a-f]{64}$/);
    await expect.poll(() => page.evaluate(() => localStorage.getItem('current-fragment'))).toBe(identity.fragment_hash);
    await expect(page.locator('#win-fragment-manager')).toBeHidden();
    const mount = await page.evaluate(() => ({
      token: localStorage.getItem('current-token'),
      state: JSON.stringify({ ...localStorage })
    }));
    expect(mount.token).toBeTruthy();
    expect(mount.state).not.toContain('test-only-master');
    const listing = await context.request.get(origin + '/noa/v1/fragments', {
      headers: { authorization: `Bearer ${mount.token}` }
    });
    expect(await listing.json()).toEqual([identity.fragment_hash]);
    await checkNamespaceEvents(page, context.request, identity.fragment_id, mount.token);
  } finally {
    await context.close();
  }
}

try {
  await checkFragmentCreation();
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
  fs.writeFileSync(path.join(access.output,'console-workflows-PASSED.json'),JSON.stringify({passed:true,fresh_fragment_created_and_mounted:true,fragment_scoped_websocket:true,real_master_authorization:true,mounted_session_preserved:true,snapshot_created_listed_downloaded:true,original_browser_live_cases:4},null,2));
}finally{await browser.close();await host.close();}
