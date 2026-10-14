// Real SIGKILL of owned sender/receiver processes after actual block publication.
import fs from 'node:fs/promises';
import path from 'node:path';
import net from 'node:net';
import {spawn,spawnSync} from 'node:child_process';
const args=Object.fromEntries(process.argv.slice(2).reduce((a,v,i,all)=>v.startsWith('--')?[...a,[v.slice(2),all[i+1]]]:a,[]));
if(!args.evidence||!args.binary)throw new Error('Pass --evidence COMPLETED_PROOF --binary ENGINE');
const evidence=await fs.realpath(args.evidence),binary=await fs.realpath(args.binary),summary=JSON.parse(await fs.readFile(path.join(evidence,'summary.json'),'utf8'));
if(summary.status!=='PASS')throw new Error('Completed source proof required');
const base=await fs.mkdtemp(path.join(args['target-root']?await fs.realpath(args['target-root']):path.dirname(evidence),'hard-kill-owned-'));
const source=summary.repository,key=summary.key_file,snapshot=summary.second_snapshot.snapshot;
const tls={cert:path.join(base,'cert.pem'),key:path.join(base,'tls.key'),ca:path.join(base,'ca.pem')};
if((args.modes||'http,native').split(',').some(mode=>['https','native-tls'].includes(mode))) {
  const caKey=path.join(base,'ca.key'),csr=path.join(base,'leaf.csr');
  const openssl=argv=>{if(spawnSync('openssl',argv,{stdio:'ignore'}).status!==0)throw new Error('TLS fixture provisioning failed');};
  openssl(['req','-x509','-newkey','rsa:2048','-sha256','-days','1','-nodes','-keyout',caKey,'-out',tls.ca,'-subj','/CN=N1 interruption test CA','-addext','basicConstraints=critical,CA:TRUE','-addext','keyUsage=critical,keyCertSign,cRLSign']);
  openssl(['req','-new','-newkey','rsa:2048','-sha256','-nodes','-keyout',tls.key,'-out',csr,'-subj','/CN=localhost','-addext','basicConstraints=critical,CA:FALSE','-addext','subjectAltName=DNS:localhost,IP:127.0.0.1','-addext','extendedKeyUsage=serverAuth']);
  openssl(['x509','-req','-in',csr,'-CA',tls.ca,'-CAkey',caKey,'-CAcreateserial','-days','1','-sha256','-copy_extensions','copy','-out',tls.cert]);
  for(const file of [caKey,tls.key])await fs.chmod(file,0o600);
}
const report={scope:'actual process SIGKILL and retry on isolated filesystem repositories; not a power cut',checks:[]};
const allocate=async()=>{const s=net.createServer();await new Promise(r=>s.listen(0,'127.0.0.1',r));const p=s.address().port;await new Promise(r=>s.close(r));return p;};
const cli=(operation,repo,extra=[])=>spawnSync(binary,['backup',operation,'--repository',repo,'--key-file',key,...extra],{encoding:'utf8',timeout:180000});
async function files(directory){const result=[];async function visit(dir){for(const e of await fs.readdir(dir,{withFileTypes:true})){const p=path.join(dir,e.name);if(e.isDirectory())await visit(p);else if(e.isFile()&&/^[0-9a-f]{64}$/.test(e.name))result.push(p);}}await visit(directory);return result;}
function terminate(child){try{process.kill(-child.pid,'SIGKILL');}catch{}}
const wait=child=>child.exitCode!==null||child.signalCode!==null?Promise.resolve(child.exitCode):new Promise(resolve=>child.once('exit',resolve));
for(const mode of (args.modes||'http,native').split(','))for(const killed of ['sender','receiver']){
  if(!['http','native','https','native-tls'].includes(mode))throw new Error('Unsupported transport');
  const dir=path.join(base,`${mode}-${killed}`);await fs.mkdir(dir);
  const target=path.join(dir,'replica');if(cli('init',target).status!==0)throw new Error('Target initialization failed');
  const tokenFile=path.join(dir,'operator.token');const generated=spawnSync(binary,['backup','operator-keygen','--token-file',tokenFile]);if(generated.status!==0)throw new Error('Token generation failed');
  const httpPort=await allocate(),nativePort=await allocate(),config=path.join(dir,'operator.json');
  const secure=['https','native-tls'].includes(mode),native=['native','native-tls'].includes(mode);
  const settings={bind:`127.0.0.1:${httpPort}`,native_bind:`127.0.0.1:${nativePort}`,allow_plaintext:!secure,token_file:tokenFile,repositories:{replica:{path:target,key_file:key}}};
  if(secure){settings.tls_cert=tls.cert;settings.tls_key=tls.key;}
  await fs.writeFile(config,JSON.stringify(settings),{mode:0o600});
  const logs=await fs.open(path.join(dir,'process.log'),'wx',0o600);
  const startReceiver=async()=>{const c=spawn(binary,['backup','serve','--config',config],{detached:true,stdio:['ignore',logs.fd,logs.fd]});for(let i=0;i<100;i++){if(c.exitCode!==null)throw new Error('Receiver startup failed');try{await new Promise((resolve,reject)=>{const s=net.connect(httpPort,'127.0.0.1',()=>{s.destroy();resolve();});s.on('error',reject);});return c;}catch{await new Promise(r=>setTimeout(r,50));}}throw new Error('Receiver startup deadline');};
  let receiver=await startReceiver();let sender;
  const scheme={http:'http',native:'n1',https:'https','native-tls':'n1s'}[mode];
  const command=['backup','replicate','--repository',source,'--key-file',key,'--snapshot',snapshot,'--target-url',`${scheme}://127.0.0.1:${native?nativePort:httpPort}`,'--target-repository','replica','--token-file',tokenFile,...(secure?['--ca-file',tls.ca]:['--allow-plaintext'])];
  const started=performance.now();
  try{
    sender=spawn(binary,command,{detached:true,stdio:['ignore',logs.fd,logs.fd]});const deadline=performance.now()+120000;
    let before;
    while(performance.now()<deadline){const list=await files(path.join(target,'blocks'));if(list.length>=2){before=await Promise.all(list.map(async p=>[p,(await fs.stat(p)).mtimeMs]));break;}if(sender.exitCode!==null)throw new Error('Sender exited before the interruption gate');await new Promise(r=>setTimeout(r,5));}
    if(!before)throw new Error('No actual destination block publication');
    terminate(killed==='sender'?sender:receiver);await wait(killed==='sender'?sender:receiver);
    if(killed==='receiver'){await wait(sender);receiver=await startReceiver();}else await new Promise(r=>setTimeout(r,200));
    if((await fs.readdir(path.join(target,'snapshots'))).length!==0)throw new Error('Interrupted transfer published a completed snapshot');
    // Preserve the repository lease while a killed peer's in-flight fsync unwinds.
    let retryCode;for(let attempt=0;attempt<5;attempt++){const retried=spawn(binary,command,{detached:true,stdio:['ignore',logs.fd,logs.fd]});retryCode=await wait(retried);if(retryCode===0)break;await new Promise(r=>setTimeout(r,1000));}if(retryCode!==0)throw new Error('Network retry failed after bounded lease-safe retries');
    const verified=cli('verify',target,['--snapshot',snapshot]);if(verified.status!==0)throw new Error('Retry snapshot did not verify');
    for(const[p,time]of before){if((await fs.stat(p)).mtimeMs!==time)throw new Error('An accepted block was rewritten instead of reused');}
    report.checks.push({mode,killed,status:'PASS',published_blocks_before_kill:before.length,elapsed_ms:Math.round(performance.now()-started)});
    console.log(JSON.stringify(report.checks.at(-1)));
  }finally{if(sender)terminate(sender);terminate(receiver);await wait(receiver);await logs.close();}
}
report.status='PASS';await fs.writeFile(path.join(base,'results.json'),JSON.stringify(report,null,2),{mode:0o600});console.log(`PASS: owned process interruption/retry evidence in ${base}`);
