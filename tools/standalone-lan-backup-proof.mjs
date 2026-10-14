// N1-owned backup traffic across two real hosts. SSH is not used by this driver.
// Receiver provisioning is fixture-only; product TLS and transfer are built in.
import fs from 'node:fs/promises';
import path from 'node:path';
import https from 'node:https';
import http from 'node:http';
import {createHash} from 'node:crypto';
import {spawn,spawnSync} from 'node:child_process';
const args=Object.fromEntries(process.argv.slice(2).reduce((a,v,i,all)=>v.startsWith('--')?[...a,[v.slice(2),all[i+1]]]:a,[]));
const binary=await fs.realpath(args.binary||'');
const pause=ms=>new Promise(r=>setTimeout(r,ms));
if(args['verify-console']){
  const base=await fs.realpath(args['verify-console']),mapping=JSON.parse(await fs.readFile(path.join(base,'console-inventory.json'),'utf8'));
  const targets=new Map();for(const root of mapping.target_roots){const descriptor=JSON.parse(await fs.readFile(path.join(root,'.n1-pool.json'),'utf8'));targets.set(descriptor.member_id,root);}
  let matched=0;
  for(const source of mapping.source_roots){
    const descriptor=JSON.parse(await fs.readFile(path.join(source,'.n1-pool.json'),'utf8')),target=targets.get(descriptor.member_id);if(!target)throw new Error('Restored logical member missing');
    async function inventory(root){const records={};async function visit(dir){for(const entry of await fs.readdir(dir,{withFileTypes:true})){const full=path.join(dir,entry.name);if(entry.isDirectory())await visit(full);else if(entry.isFile()&&/\.(ncx|shard|parity)$/.test(entry.name))records[path.relative(root,full)]=createHash('sha256').update(await fs.readFile(full)).digest('hex');}}await visit(root);return records;}
    const before=await inventory(source),after=await inventory(target);if(Object.keys(before).length!==Object.keys(after).length)throw new Error('Restored member payload/NCX count differs');
    for(const [name,hash]of Object.entries(before)){if(after[name]!==hash)throw new Error(`Restored payload/NCX mismatch: ${name}`);matched++;}
  }
  const result={status:'PASS',logical_members:targets.size,matched_payload_and_ncx_files:matched};await fs.writeFile(path.join(base,'console-integrity-results.json'),JSON.stringify(result,null,2),{mode:0o600});console.log(JSON.stringify(result));
}else if(args['serve-root']){
  const base=await fs.realpath(args['serve-root']),key=await fs.realpath(args['key-file']),host=args.host;
  if(!host||!/^\d+\.\d+\.\d+\.\d+$/.test(host))throw new Error('Receiver fixture needs an explicit LAN IPv4 address');
  const tokenFile=path.join(base,'operator.token'),ca=path.join(base,'ca.pem'),caKey=path.join(base,'ca.key'),cert=path.join(base,'cert.pem'),tlsKey=path.join(base,'tls.key'),csr=path.join(base,'leaf.csr');
  const run=(cmd,argv)=>{const r=spawnSync(cmd,argv,{stdio:'ignore'});if(r.status!==0)throw new Error(`${cmd} fixture provisioning failed`);};
  run(binary,['backup','operator-keygen','--token-file',tokenFile]);
  run('openssl',['req','-x509','-newkey','rsa:2048','-sha256','-days','1','-nodes','-keyout',caKey,'-out',ca,'-subj','/CN=N1 LAN Proof CA','-addext','basicConstraints=critical,CA:TRUE','-addext','keyUsage=critical,keyCertSign,cRLSign']);
  run('openssl',['req','-new','-newkey','rsa:2048','-sha256','-nodes','-keyout',tlsKey,'-out',csr,'-subj',`/CN=${host}`,'-addext','basicConstraints=critical,CA:FALSE','-addext',`subjectAltName=IP:${host},IP:127.0.0.1`,'-addext','extendedKeyUsage=serverAuth','-addext','keyUsage=digitalSignature,keyEncipherment']);
  run('openssl',['x509','-req','-in',csr,'-CA',ca,'-CAkey',caKey,'-CAcreateserial','-days','1','-sha256','-copy_extensions','copy','-out',cert]);
  for(const p of [caKey,tlsKey])await fs.chmod(p,0o600);
  const processes=[],endpoints={};
  try{
    for(const secure of (args['console-evidence']?[true]:[false,true])){
      const port=secure?32922:32920,nativePort=port+1,repositories={};
      for(const alias of secure?['lan_https','lan_native_tls']:['lan_http','lan_native']){
        const repo=path.join(base,alias);run(binary,['backup','init','--repository',repo,'--key-file',key]);repositories[alias]={path:repo,key_file:key};
      }
      const config=path.join(base,secure?'secure.json':'plain.json');
      const settings={bind:`0.0.0.0:${port}`,native_bind:`0.0.0.0:${nativePort}`,token_file:tokenFile,allow_plaintext:!secure,repositories,...(secure?{tls_cert:cert,tls_key:tlsKey}:{})};
      if(args['console-evidence']){
        const sourceConfig=path.join(await fs.realpath(args['console-evidence']),'restored-control/n1_config.ini'),text=await fs.readFile(sourceConfig,'utf8');
        const sourceRoots=text.match(/^roots=(.*)$/m)?.[1].split(',');if(!sourceRoots?.length)throw new Error('Missing pool roots');
        const targetRoots=[];for(const sourceRoot of sourceRoots){const target=path.join(path.dirname(sourceRoot),`console-restored-${path.basename(base)}`);await fs.mkdir(target,{mode:0o700});targetRoots.push(target);}
        const targetConfig=path.join(base,'restore.ini');await fs.writeFile(targetConfig,text.replace(/^roots=.*$/m,`roots=${targetRoots.join(',')}`).replace(/^fs_storage_path=.*$/m,`fs_storage_path=${path.join(base,'restored-control')}`),{mode:0o600});
        settings.repositories={primary:{path:path.join(base,'primary'),key_file:key},'replica-http':{path:path.join(path.dirname(sourceRoots[1]||sourceRoots[0]),`console-http-${path.basename(base)}`),key_file:key},'replica-native':{path:path.join(path.dirname(sourceRoots[0]),`console-native-${path.basename(base)}`),key_file:key}};
        settings.engine_config=sourceConfig;settings.restore_targets={empty:targetConfig};
        settings.replicas={http:{url:`https://127.0.0.1:${port}`,repository:'replica-http',token_file:tokenFile,ca_file:ca},native:{url:`n1s://127.0.0.1:${nativePort}`,repository:'replica-native',token_file:tokenFile,ca_file:ca}};
        settings.allowed_origins=['http://127.0.0.1:34389'];
        if(args.online==='true') {
          const sourcePort=Number(text.match(/^https_port=(\d+)$/m)?.[1]);
          if(!sourcePort)throw new Error('Source HTTPS port missing');
          settings.online_source={url:`https://127.0.0.1:${sourcePort}`,repository:'primary',token_file:tokenFile,ca_file:path.join(path.dirname(sourceConfig),'cert.pem')};
        }
        await fs.writeFile(path.join(base,'console-client.json'),JSON.stringify({endpoint:`https://${host}:${port}`,console_port:34389,token_file:'operator.token',cert_file:'cert.pem',capture_mode:args.online==='true'?'online_committed':'offline_full'}),{mode:0o600});
        await fs.writeFile(path.join(base,'console-inventory.json'),JSON.stringify({source_roots:sourceRoots,target_roots:targetRoots}),{mode:0o600});
      }
      await fs.writeFile(config,JSON.stringify(settings),{mode:0o600});
      if(args.online==='true'&&args['console-evidence']) {
        const control=path.dirname(settings.engine_config),log=await fs.open(path.join(base,'source-engine.log'),'wx',0o600);
        const engine=spawn(binary,[],{cwd:control,env:{...process.env,N1_BACKUP_OPERATOR_CONFIG:config,N1_REFRESH_STORE_ROOT:path.join(control,'refresh')},detached:true,stdio:['ignore',log.fd,log.fd]});processes.push(engine);await log.close();
      }
      const log=await fs.open(path.join(base,secure?'secure.log':'plain.log'),'wx',0o600);
      const child=spawn(binary,['backup','serve','--config',config],{detached:true,stdio:['ignore',log.fd,log.fd]});processes.push(child);await log.close();
      if(secure){endpoints.https={url:`https://${host}:${port}`,control:`https://${host}:${port}`,repository:'lan_https'};endpoints['native-tls']={url:`n1s://${host}:${nativePort}`,control:`https://${host}:${port}`,repository:'lan_native_tls'};}
      else{endpoints.http={url:`http://${host}:${port}`,control:`http://${host}:${port}`,repository:'lan_http'};endpoints.native={url:`n1://${host}:${nativePort}`,control:`http://${host}:${port}`,repository:'lan_native'};}
    }
    await fs.writeFile(path.join(base,'client.json'),JSON.stringify({endpoints,token_file:'operator.token',ca_file:'ca.pem'}),{mode:0o600});
    console.log('N1-owned LAN receivers started; private client configuration written.');
    await new Promise(resolve=>{process.once('SIGTERM',resolve);process.once('SIGINT',resolve);});
  }finally{for(const c of processes)try{process.kill(-c.pid,'SIGKILL');}catch{}}
}else{
  if(!args.evidence||!args.config)throw new Error('Pass --evidence COMPLETED_SOURCE_PROOF --config CLIENT_CONFIG --binary ENGINE');
  const evidence=await fs.realpath(args.evidence),source=JSON.parse(await fs.readFile(path.join(evidence,'summary.json'),'utf8')),config=JSON.parse(await fs.readFile(args.config,'utf8'));
  if(source.status!=='PASS')throw new Error('A completed source proof is required');
  config.token_file=path.resolve(path.dirname(args.config),config.token_file);config.ca_file=path.resolve(path.dirname(args.config),config.ca_file);
  const token=(await fs.readFile(config.token_file,'utf8')).trim(),ca=await fs.readFile(config.ca_file),report={scope:'real LAN N1-owned replication to a separate host; no SSH transfer, no power-cut claim',checks:[]};
  const out=path.join(evidence,'lan-results.json');
  async function operation(endpoint,request,credential=token){
    const u=new URL(`${endpoint.control}/noa/v1/maintenance/backups/operation`),body=JSON.stringify(request);
    return new Promise((resolve,reject)=>{const r=(u.protocol==='https:'?https:http).request(u,{method:'POST',ca,headers:{Authorization:`Bearer ${credential}`,'Content-Type':'application/json','Content-Length':Buffer.byteLength(body)}},response=>{let data='';response.on('data',v=>{data+=v;if(data.length>65536)r.destroy(new Error('Oversized control response'));});response.on('end',()=>{try{resolve({status:response.statusCode,data:JSON.parse(data)});}catch(error){reject(error);}});});r.setTimeout(15000,()=>r.destroy(new Error('Control timeout')));r.on('error',reject);r.end(body);});
  }
  async function verified(endpoint,snapshot){
    const accepted=await operation(endpoint,{operation:'verify',repository:endpoint.repository,snapshot});
    if(accepted.status!==200||!accepted.data.operation_id)throw new Error('Remote verification refused');
    const end=Date.now()+180000;
    while(Date.now()<end){const status=await operation(endpoint,{operation:'operations'}),job=status.data.operations?.find(j=>j.id===accepted.data.operation_id);if(job?.state==='complete')return job.result;if(job?.state==='failed')throw new Error(`Remote verification failed: ${job.error}`);await pause(250);}
    throw new Error('Remote verification deadline');
  }
  try{
    for(const mode of ['http','native','https','native-tls']){
      const endpoint=config.endpoints[mode];
      const refused=await operation(endpoint,{operation:'capabilities'},'invalid-test-token');if(refused.status!==401&&refused.status!==403)throw new Error('Wrong operator token was accepted');
      const secure=['https','native-tls'].includes(mode),snapshot=source.second_snapshot.snapshot;
      const command=['backup','replicate','--repository',source.repository,'--key-file',source.key_file,'--snapshot',snapshot,'--target-url',endpoint.url,'--target-repository',endpoint.repository,'--token-file',config.token_file,...(secure?['--ca-file',config.ca_file]:['--allow-plaintext'])];
      const begin=performance.now();const transfer=spawnSync(binary,command,{encoding:'utf8',timeout:300000});if(transfer.status!==0)throw new Error(`${mode} replication failed: ${transfer.stderr}`);
      const elapsed=performance.now()-begin,detail=transfer.stdout.trim();if(detail!==`Replicated ${snapshot}`)throw new Error('Unexpected replication acknowledgement');const verification=await verified(endpoint,snapshot);
      const repeat=spawnSync(binary,command,{encoding:'utf8',timeout:180000});if(repeat.status!==0)throw new Error('Repeated LAN replication failed');
      if(repeat.stdout.trim()!==`Replicated ${snapshot}`)throw new Error('Unexpected repeat acknowledgement');
      report.checks.push({mode,status:'PASS',elapsed_ms:Math.round(elapsed),transfer:detail,verification,repeat:repeat.stdout.trim(),wrong_token_status:refused.status});
      await fs.writeFile(out,JSON.stringify(report,null,2),{mode:0o600});console.log(JSON.stringify({mode,status:'PASS',elapsed_ms:Math.round(elapsed),transfer:detail}));
    }
    report.status='PASS';
  }catch(error){report.status='FAIL';report.error=error.message;throw error;}
  finally{await fs.writeFile(out,JSON.stringify(report,null,2),{mode:0o600});}
}
