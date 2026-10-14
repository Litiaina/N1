// Real systemd user services, real engine, real capture and owned replication.
// All unit names and repositories are unique fixtures. Never controls deployed units.
import fs from 'node:fs/promises';
import path from 'node:path';
import net from 'node:net';
import {spawnSync} from 'node:child_process';
const args=Object.fromEntries(process.argv.slice(2).reduce((a,v,i,all)=>v.startsWith('--')?[...a,[v.slice(2),all[i+1]]]:a,[]));
if(!args.evidence||!args.binary)throw new Error('Pass --evidence COMPLETED_PROOF --binary ENGINE');
const evidence=await fs.realpath(args.evidence),binary=await fs.realpath(args.binary),shell=path.join(path.dirname(binary),'litiaina-n1-shell');
const summary=JSON.parse(await fs.readFile(path.join(evidence,'summary.json'),'utf8'));
if(summary.status!=='PASS')throw new Error('A completed source proof is required');
const base=await fs.mkdtemp(path.join(path.dirname(evidence),'operator-supervision-'));
const suffix=path.basename(base).split('-').at(-1),engineUnit=`n1-proof-engine-${suffix}.service`,operatorUnit=`n1-proof-operator-${suffix}.service`,recoverUnit=`n1-proof-recover-${suffix}.service`,peerUnit=`n1-proof-peer-${suffix}.service`;
const units=[engineUnit,operatorUnit,recoverUnit,peerUnit],key=summary.key_file,repo=path.join(base,'primary'),replica=path.join(base,'replica'),state=path.join(base,'job-state');
let control=path.join(evidence,'restored-control');
const online=args.online==='true',jobFile=path.join(base,'job.json'),configFile=path.join(base,'operator.json'),tokenFile=path.join(base,'operator.token'),sourceConfig=path.join(base,'source.json');
const report={scope:'real systemd user-unit supervision on isolated N1 data; no power-cut claim',checks:[]};
const ctl=(...argv)=>{const r=spawnSync('/usr/bin/systemctl',['--user',...argv],{encoding:'utf8',timeout:60000});if(r.status!==0)throw new Error(`systemctl ${argv.join(' ')} failed: ${r.stderr}`);return r.stdout.trim();};
const cli=(operation,repository,extra=[])=>{const r=spawnSync(shell,['backup',operation,'--repository',repository,'--key-file',key,...extra],{encoding:'utf8',timeout:180000});if(r.status!==0)throw new Error(`backup ${operation} failed: ${r.stderr}`);return r.stdout;};
const allocate=async()=>{const s=net.createServer();await new Promise(r=>s.listen(0,'127.0.0.1',r));const p=s.address().port;await new Promise(r=>s.close(r));return p;};
const port=await allocate(),origin=`http://127.0.0.1:${port}`,peerPort=await allocate(),peerConfig=path.join(base,'peer.json');
const pause=ms=>new Promise(r=>setTimeout(r,ms));
const until=async(label,fn,seconds=120)=>{const end=Date.now()+seconds*1000;while(Date.now()<end){if(await fn())return;await pause(100);}throw new Error(`Timed out: ${label}`);};
await fs.mkdir(state,{mode:0o700});cli('init',repo);cli('init',replica);
const generated=spawnSync(shell,['backup','operator-keygen','--token-file',tokenFile]);if(generated.status!==0)throw new Error('Token generation failed');
const token=(await fs.readFile(tokenFile,'utf8')).trim();
let onlineSource;
if(online) {
  const original=control;control=path.join(base,'engine-control');await fs.mkdir(control,{mode:0o700});
  for(const file of ['n1.env','cert.pem','key.pem'])await fs.copyFile(path.join(original,file),path.join(control,file));
  const httpsPort=await allocate(),nativePort=await allocate();
  let text=await fs.readFile(path.join(original,'n1_config.ini'),'utf8');text=text.replace(/^https_port=.*$/m,`https_port=${httpsPort}`).replace(/^native_port=.*$/m,`native_port=${nativePort}`).replace(/^cert_path=.*$/m,`cert_path=${path.join(control,'cert.pem')}`).replace(/^key_path=.*$/m,`key_path=${path.join(control,'key.pem')}`);
  await fs.writeFile(path.join(control,'n1_config.ini'),text,{mode:0o600});
  await fs.writeFile(sourceConfig,JSON.stringify({bind:'127.0.0.1:0',token_file:tokenFile,engine_config:path.join(control,'n1_config.ini'),repositories:{primary:{path:repo,key_file:key}}}),{mode:0o600});
  onlineSource={url:`n1://127.0.0.1:${nativePort}`,repository:'primary',token_file:tokenFile,allow_plaintext:true};
}
await fs.writeFile(jobFile,JSON.stringify({enabled:true,service:{unit:engineUnit,user:true},engine_config:path.join(control,'n1_config.ini'),repository:repo,key_file:key,state_dir:state,workers:4,capture_timeout_seconds:180,operation_timeout_seconds:180,service_timeout_seconds:60,replication_attempts:3,keep_last:2,max_age_seconds:3600,...(online?{online_source:onlineSource}:{}),replica:{repository:'replica',key_file:key,url:`http://127.0.0.1:${peerPort}`,token_file:tokenFile,allow_plaintext:true,keep_last:2}}),{mode:0o600});
await fs.writeFile(configFile,JSON.stringify({bind:`127.0.0.1:${port}`,allow_plaintext:true,token_file:tokenFile,repositories:{primary:{path:repo,key_file:key}},jobs:{proof:jobFile}}),{mode:0o600});
await fs.writeFile(peerConfig,JSON.stringify({bind:`127.0.0.1:${peerPort}`,allow_plaintext:true,token_file:tokenFile,repositories:{replica:{path:replica,key_file:key}}}),{mode:0o600});
// Mirror the product unit's ExecStopPost / OnFailure restart-intent handling.
await fs.writeFile(path.join(base,engineUnit),`[Unit]\nDescription=Isolated N1 proof engine\n[Service]\nType=simple\nWorkingDirectory=${control}\nEnvironment=N1_REFRESH_STORE_ROOT=${control}/refresh\n${online?`Environment=N1_BACKUP_OPERATOR_CONFIG=${sourceConfig}\n`:''}ExecStart=${binary}\nKillMode=control-group\nTimeoutStopSec=60\n`,{mode:0o600});
await fs.writeFile(path.join(base,operatorUnit),`[Unit]\nDescription=Isolated N1 proof operator\nOnFailure=${recoverUnit}\n[Service]\nType=simple\nExecStart=${shell} backup serve --config ${configFile}\nExecStopPost=${shell} backup recover-operator --config ${configFile}\nRestart=on-failure\nRestartSec=1\nKillMode=control-group\nUMask=0077\nTimeoutStopSec=120\n`,{mode:0o600});
await fs.writeFile(path.join(base,recoverUnit),`[Unit]\nDescription=Isolated N1 proof restart recovery\n[Service]\nType=oneshot\nExecStart=${shell} backup recover-operator --config ${configFile}\nUMask=0077\nTimeoutStartSec=120\n`,{mode:0o600});
await fs.writeFile(path.join(base,peerUnit),`[Unit]\nDescription=Isolated N1 proof receiver\n[Service]\nType=simple\nExecStart=${shell} backup serve --config ${peerConfig}\nKillMode=control-group\nUMask=0077\nTimeoutStopSec=60\n`,{mode:0o600});
const operation=async(request)=>{const response=await fetch(`${origin}/noa/v1/maintenance/backups/operation`,{method:'POST',headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:JSON.stringify(request),signal:AbortSignal.timeout(15000)});const data=await response.json();if(!response.ok)throw new Error(`Operation refused: ${JSON.stringify(data)}`);return data;};
const status=async()=>JSON.parse(await fs.readFile(path.join(state,'status.json'),'utf8'));
const note=(name,detail={})=>{const check={name,status:'PASS',...detail};report.checks.push(check);console.log(JSON.stringify(check));};
try{
  ctl('link',...units.map(u=>path.join(base,u)));ctl('daemon-reload');ctl('start',engineUnit,operatorUnit,peerUnit);
  await until('operator ready',async()=>{try{return(await operation({operation:'capabilities'})).repositories!==undefined;}catch{return false;}});
  if(online) await until('source native listener ready',async()=>{try{return await new Promise(resolve=>{const socket=net.connect(Number(new URL(onlineSource.url).port),'127.0.0.1',()=>{socket.destroy();resolve(true);});socket.on('error',()=>resolve(false));});}catch{return false;}});
  // Kill actual operator cgroup once capture has stopped the engine and fsynced restart intent.
  const accepted=await operation({operation:'run-job',job:'proof',confirm:true});
  await until('capture in progress with correct service intention',async()=>{try{const s=await status(),active=ctl('show',engineUnit,'-p','ActiveState','--value');return s.phase==='capturing'&&(online?s.restart===null&&active==='active':s.restart?.unit===engineUnit&&active==='inactive');}catch{return false;}});
  const enginePid=ctl('show',engineUnit,'-p','MainPID','--value');
  // Some user managers reject systemctl's auxiliary cgroup.kill request.
  // SIGKILL every owned PID in the actual fixture cgroup, not only Shell's parent.
  const group=ctl('show',operatorUnit,'-p','ControlGroup','--value');
  if(!group.startsWith('/user.slice/')||!group.endsWith(`/${operatorUnit}`))throw new Error('Unexpected fixture cgroup');
  const killed=[];
  async function killGroup(directory){for(const entry of await fs.readdir(directory,{withFileTypes:true}))if(entry.isDirectory())await killGroup(path.join(directory,entry.name));const ids=(await fs.readFile(path.join(directory,'cgroup.procs'),'utf8')).trim().split(/\s+/).map(Number).filter(Boolean);for(const pid of ids.reverse()){try{const status=await fs.readFile(`/proc/${pid}/status`,'utf8');if(Number(status.match(/^Uid:\s+(\d+)/m)?.[1])!==process.getuid())throw new Error('Fixture cgroup contains a foreign process');process.kill(pid,'SIGKILL');killed.push(pid);}catch(error){if(error.code!=='ESRCH'&&error.code!=='ENOENT')throw error;}}}
  await killGroup(`/sys/fs/cgroup${group}`);if(killed.length<2)throw new Error('Operator/Shell child tree was not actually interrupted');
  await until('engine recovered and job explicitly interrupted',async()=>{try{const s=await status();return ctl('show',engineUnit,'-p','ActiveState','--value')==='active'&&s.phase==='interrupted'&&s.restart===null;}catch{return false;}},180);
  const interrupted=await status();if(interrupted.last_success_at!==null)throw new Error('Interrupted job was labelled successful');
  if(online&&ctl('show',engineUnit,'-p','MainPID','--value')!==enginePid)throw new Error('Online controller loss restarted N1');
  note(online?'operator SIGKILL during online capture preserves engine PID without false success':'operator SIGKILL during capture restarts engine without false success',{accepted_job:accepted.operation_id,killed_processes:killed.length});
  await until('operator restarted',async()=>{try{await operation({operation:'capabilities'});return true;}catch{return false;}});
  if(online) await until('accepted source capture completes after controller loss',async()=>{try{return(await fs.readdir(path.join(repo,'snapshots'))).length>0;}catch{return false;}});
  const retry=await operation({operation:'run-job',job:'proof',confirm:true});
  await until('retry completed and protected',async()=>{try{const s=await status();return s.phase==='complete'&&s.last_success_snapshot&&s.restart===null;}catch{return false;}},360);
  const completed=await status();cli('verify',repo,['--snapshot',completed.last_success_snapshot]);cli('verify',replica,['--snapshot',completed.last_success_snapshot]);
  if(ctl('show',engineUnit,'-p','ActiveState','--value')!=='active')throw new Error('Successful capture left engine stopped');
  const protection=await operation({operation:'job-status',job:'proof'});
  if(protection.healthy!==true)throw new Error(`Protection did not report verified healthy state: ${JSON.stringify(protection)}`);
  note('retry capture, owned HTTP replication, retention, verification and healthy status',{snapshot:completed.last_success_snapshot,retry_job:retry.operation_id});
  report.status='PASS';
}catch(error){report.status='FAIL';report.error=error.message;throw error;}
finally{
  // ExecStopPost may legitimately restart the engine. Stop supervisors first,
  // then stop the recovered fixture engine; never unlink an active test unit.
  for(const unit of [operatorUnit,recoverUnit,engineUnit,peerUnit])spawnSync('/usr/bin/systemctl',['--user','stop',unit],{timeout:180000});
  // Remove only this harness's links, not any pre-existing units or engine data.
  for(const unit of units){const link=path.join(process.env.HOME,'.config/systemd/user',unit);try{if(await fs.readlink(link)===path.join(base,unit))await fs.unlink(link);}catch{}}
  spawnSync('/usr/bin/systemctl',['--user','daemon-reload']);
  await fs.writeFile(path.join(base,'results.json'),JSON.stringify(report,null,2),{mode:0o600});
  console.log(`Evidence: ${base}`);
}
