// Isolated USB/filesystem proof. Never formats, mounts or touches existing pools.
// Uses the real release-proof corpus and actual Shell/engine network receiver.
import fs from 'node:fs/promises';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
const args=Object.fromEntries(process.argv.slice(2).reduce((a,v,i,all)=>v.startsWith('--')?[...a,[v.slice(2),all[i+1]]]:a,[]));
if(!args['test-root']||!args.binary||!args.proof)throw new Error('Pass --test-root EXISTING_DIRECTORY --binary ENGINE --proof PROOF_BINARY');
const base=await fs.mkdtemp(path.join(await fs.realpath(args['test-root']),'n1-owned-backup-'));
const binary=await fs.realpath(args.binary),proof=await fs.realpath(args.proof);
const roots=(args['disk-roots']||base).split(',').filter(Boolean);
const dataShards=Number(args['data-shards']||1),parityShards=Number(args['parity-shards']||0);
if(!Number.isInteger(dataShards)||!Number.isInteger(parityShards)||dataShards<1||parityShards<0||dataShards+parityShards!==roots.length)throw new Error('EC width must equal the number of test roots');
const devices=[];
for(const root of roots){const stat=await fs.stat(root);if(!stat.isDirectory())throw new Error('Test root must be a directory');devices.push(stat.dev);}
if(roots.length>1&&new Set(devices).size!==roots.length)throw new Error('Multi-device proof requires distinct filesystems, not multiple directories on one disk');
const source=path.join(base,'source.bin'),file=await fs.open(source,'wx',0o600);
const hash=createHash('sha256');
for(let i=0;i<64;i++){const data=Buffer.alloc(1024*1024);for(let j=0;j<data.length;j++)data[j]=(j*31+i*47)%251;await file.write(data);hash.update(data);}
await file.close();
const report={date:new Date().toISOString(),filesystem_root:base,source_bytes:64*1024*1024,source_sha256:hash.digest('hex'),ec:[dataShards,parityShards],distinct_devices:new Set(devices).size,transport:[],scope:roots.length===1?'single filesystem 1+0; not multi-device EC or physical power-loss qualification':'distinct-device EC; transport loopback; not physical power-loss qualification'};
async function run(mode,index){
  const out=path.join(base,mode);await fs.mkdir(out);
  const log=await fs.open(path.join(base,`${mode}.log`),'wx',0o600);
  const started=performance.now();
  const child=spawn(proof,['--binary',binary,'--source-file',source,'--disk-roots',roots.join(','),'--out-dir',out,'--data-shards',String(dataShards),'--parity-shards',String(parityShards),args.online==='true'?'--online-backup-proof':'--backup-proof','--backup-transport',mode,'--port',String(31831+index*2),'--native-port',String(31832+index*2)],{stdio:['ignore',log.fd,log.fd]});
  let peakRssKiB=0,peakFds=0;const processPeaks={};
  const sampler=setInterval(async()=>{const queue=[child.pid];for(let i=0;i<queue.length&&i<128;i++){try{const pid=queue[i],text=await fs.readFile(`/proc/${pid}/status`,'utf8'),name=text.match(/^Name:\s+(.*)/m)?.[1]||'unknown',rss=Number(text.match(/VmRSS:\s+(\d+)/)?.[1]||0),fds=(await fs.readdir(`/proc/${pid}/fd`)).length;const peak=processPeaks[name]||{rss_kib:0,fds:0};peak.rss_kib=Math.max(peak.rss_kib,rss);peak.fds=Math.max(peak.fds,fds);processPeaks[name]=peak;if(pid===child.pid){peakRssKiB=Math.max(peakRssKiB,rss);peakFds=Math.max(peakFds,fds);}const children=(await fs.readFile(`/proc/${pid}/task/${pid}/children`,'utf8')).trim().split(/\s+/).map(Number).filter(Boolean);queue.push(...children);}catch{}}},1000);
  const code=await new Promise(resolve=>child.once('exit',resolve));clearInterval(sampler);await log.close();
  let summary;try{summary=JSON.parse(await fs.readFile(path.join(out,'summary.json'),'utf8'));}catch{summary={status:'NO_SUMMARY'};}
  const result={mode,exit_code:code,elapsed_ms:Math.round(performance.now()-started),runner_peak_rss_kib:peakRssKiB,runner_peak_fds:peakFds,process_peaks:processPeaks,summary};
  report.transport.push(result);await fs.writeFile(path.join(base,'results.json'),JSON.stringify(report,null,2),{mode:0o600});
  console.log(JSON.stringify({mode,exit_code:code,elapsed_ms:result.elapsed_ms,runner_peak_rss_kib:peakRssKiB,runner_peak_fds:peakFds}));
  if(code!==0)throw new Error(`${mode} proof failed; inspect private evidence in ${base}`);
}
console.log(`Isolated evidence: ${base}`);
const modes=(args.modes||'http,native,https,native-tls').split(',');
for(let i=0;i<modes.length;i++)await run(modes[i],i);
console.log(args.online==='true'?'PASS: committed online capture, concurrent mutations, durable parts exclusion, native capture and relocated recovery.':'PASS: real-object offline backup, owned network replication, fenced interrupted restore and end-to-end integrity checks.');
