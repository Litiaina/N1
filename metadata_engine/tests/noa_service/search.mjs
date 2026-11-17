// Browser adapter; Rust owns the services, ACKs, permission and byte checks.
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const access = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const repo = process.env.N1_CONSOLE_REPO;
const dist = process.env.N1_CONSOLE_TEST_DIST;
if (!repo || !dist) throw new Error('Explicit Console repository and build required');
const { chromium, expect } = await import(
  pathToFileURL(path.join(repo, 'node_modules/@playwright/test/index.mjs')).href
);
const { serveConsole } = await import(
  pathToFileURL(path.join(repo, 'scripts/serve-console.mjs')).href
);
const host = await serveConsole({
  root: dist, n1: access.metadata, bind: '127.0.0.1', httpPort: access.console_port
});
const browser = await chromium.launch({ headless: true });
try {
  const origin = `http://127.0.0.1:${access.console_port}`;
  const page = await browser.newPage({ viewport: { width: 1366, height: 900 } });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.addInitScript(({ access, origin }) => {
    localStorage.setItem('current-fragment', access.fragment);
    localStorage.setItem('current-token', access.token);
    localStorage.setItem('current-token-expires-at', String(Date.now() + 3600000));
    localStorage.setItem('litiaina-console-active-connection-v2', 'n1|' + origin);
  }, { access, origin });
  await page.goto(origin + '/?engine=n1');
  await page.waitForFunction(() => Boolean(window.WindowManager) && !document.querySelector('#boot-screen'));
  await page.evaluate(() => window.WindowManager.open('file-explorer'));
  await page.locator('#searchInput').fill('report');
  await expect(page.locator('#browser')).toContainText('Report.TXT');
  await expect(page.locator('#browser')).toContainText('report.PDF');
  await expect(page.locator('#browser')).not.toContainText('SecretReport');
  await expect(page.locator('#fs-page-summary')).toHaveText('1-3 of 3 results');
  await page.locator('#searchInput').fill('zero.bin');
  await expect(page.locator('#browser')).toContainText('zero.bin');
  await expect(page.locator('#fs-page-summary')).toHaveText('1-1 of 1 results');
  await page.screenshot({ path: path.join(access.output, 'native-search-console.png') });
  if (errors.length) throw new Error(errors.join('\n'));
  fs.writeFileSync(path.join(access.output, 'SEARCH-CONSOLE-PASSED.json'), JSON.stringify({
    passed: true, real_api: true, without_accel: true, fragment_isolation: true, empty_object: true
  }));
} finally {
  await browser.close();
  await host.close();
}
