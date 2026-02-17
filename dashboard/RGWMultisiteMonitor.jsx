import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  RefreshCw, AlertTriangle, CheckCircle, Activity, TrendingUp,
  Database, Server, Settings, ChevronDown, ChevronUp, X, Zap,
  Clock, Shield, Eye, BarChart3, ArrowRight, Wifi, WifiOff,
  ArrowUpDown, Filter, AlertCircle, Globe, Layers, HardDrive
} from 'lucide-react';

/* ===== MOCK DATA ===== */
const ZONES = [
  { name: 'us-east-1', endpoints: ['https://rgw-east.example.com:8080'], is_master: true, zonegroup: 'us' },
  { name: 'us-west-2', endpoints: ['https://rgw-west.example.com:8080'], is_master: false, zonegroup: 'us' },
];
const BUCKET_NAMES = ['prod-media-assets','customer-backups','analytics-datalake','ml-training-data','application-logs','compliance-archives','cdn-origin-store','db-snapshots'];
const ERROR_MSGS = ['fetch of bucket instance info returned err=-2','failed to sync object retries exhausted','run failed with err=-110 Connection timed out','error syncing bucket shard'];
function seededRandom(seed) { let s = seed; return function() { s = (s * 16807) % 2147483647; return (s - 1) / 2147483646; }; }
function generateMockData() {
  const now = Date.now(), snaps = 4, iv = 300000, rng = seededRandom(now / 100000 | 0);
  const bm = {}, allErrors = [];
  const agentBktSync = {};
  BUCKET_NAMES.forEach((name, bi) => {
    const baseObj = 10000 + bi * 15000 + (rng() * 50000 | 0);
    const baseSize = baseObj * (1024 + rng() * 4096 | 0);
    const history = [];
    for (let s = 0; s < snaps; s++) {
      const ts = new Date(now - (snaps - 1 - s) * iv).toISOString();
      const pct = Math.min(99.9, 70 + bi * 3 + s * 2 + rng() * 10);
      const secObj = Math.floor(baseObj * pct / 100);
      history.push({ timestamp: ts, primary_zone: 'us-east-1',
        primary: { num_objects: baseObj, size_kb: baseSize / 1024, num_shards: 16, size_actual: baseSize },
        replicas: { 'us-west-2': { stats: { num_objects: secObj, size_kb: secObj, num_shards: 16, size_actual: secObj * 1024 }, delta_objects: baseObj - secObj, delta_size: (baseObj - secObj) * 1024, sync_progress_pct: Math.round(pct * 100) / 100 } },
        single_zone: false, sync_progress_pct: Math.round(pct * 100) / 100, delta_objects: baseObj - secObj, delta_size: (baseObj - secObj) * 1024 });
    }
    const errs = rng() < 0.3 ? [{ bucket: name, error_code: 13, message: ERROR_MSGS[(rng() * 4) | 0], shard_id: (rng() * 16) | 0, source_zone: 'us-west-2', timestamp: new Date().toISOString() }] : [];
    bm[name] = { history, errors: errs }; allErrors.push(...errs);
    /* mock per-bucket sync from agent */
    const isCaughtUp = rng() > 0.3;
    agentBktSync[name] = { bucket: name, zone: 'us-west-2', sync_disabled: false,
      sources: [{ source_zone: 'us-east-1', status: isCaughtUp?'caught up':'syncing', full_sync_done: isCaughtUp?16:Math.floor(rng()*16), full_sync_total: 16, incremental_sync_done: isCaughtUp?16:Math.floor(rng()*16), incremental_sync_total: 16, shard_count: 16, problem_shards: isCaughtUp?[]:[{shard_id:3,status:'behind by 42 seconds'}] }] };
  });
  /* mock agent errors */
  const agentErrs = allErrors.length > 0 ? [{ bucket: BUCKET_NAMES[0], error_code: 2, message: 'fetch of bucket instance info returned err=-2', shard_id: 5, source_zone: 'us-east-1', timestamp: new Date().toISOString(), _agent_zone: 'us-west-2', _source: 'zone_agent' }] : [];
  return {
    topology: { realm: 'production', zonegroups: ['us'], zones: ZONES, master_zone: 'us-east-1', is_single_zone: false, secondary_data_available: true, rest_validated_zones: [] },
    buckets: bm, global_errors: allErrors,
    global_sync: [{ status: 'ok', timestamp: new Date().toISOString(), realm: 'production', zone: 'us-east-1', metadata_sync: { status: 'caught up' }, data_sync: [{ source_zone: 'us-west-2', status: 'syncing', full_sync_done: 0, full_sync_total: 128, incremental_sync_done: 128, incremental_sync_total: 128 }] }],
    zone_agents: { 'us-west-2': { timestamp: new Date().toISOString(), agent_version: '1.0',
      sync_status: { status: 'ok', realm: 'production', zone: 'us-west-2', zonegroup: 'us', metadata_sync: { status: 'caught up', full_sync_done: 0, full_sync_total: 64, incremental_sync_done: 64, incremental_sync_total: 64 }, data_sync: [{ source_zone: 'us-east-1', status: 'syncing', full_sync_done: 0, full_sync_total: 128, incremental_sync_done: 120, incremental_sync_total: 128 }] },
      sync_errors: agentErrs, bucket_sync_status: agentBktSync }},
    zone_agent_sync_history: {},
    last_update: new Date().toISOString()
  };
}

/* ===== HELPERS ===== */
function formatNum(n) { if (n == null) return '0'; if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B'; if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M'; if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'; return String(n); }
function formatBytes(b) { if (!b) return '0 B'; const u = ['B','KB','MB','GB','TB']; let i = 0, v = b; while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; } return v.toFixed(i > 0 ? 1 : 0) + ' ' + u[i]; }
function formatTime(iso) { if (!iso) return '\u2014'; return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }
function formatDateTime(iso) { if (!iso) return '\u2014'; const d = new Date(iso); return d.toLocaleDateString([], { month: 'short', day: 'numeric' }) + ' ' + d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }
function agentAge(iso) { if (!iso) return null; try { return (Date.now() - new Date(iso).getTime()) / 1000; } catch { return null; } }
function agentAgeLabel(sec) { if (sec == null) return ''; if (sec < 60) return Math.round(sec) + 's ago'; if (sec < 3600) return Math.round(sec / 60) + 'm ago'; return Math.round(sec / 3600) + 'h ago'; }
const AGENT_STALE_SEC = 120; /* 2 minutes */
function isAgentStale(agent) { if (!agent) return true; const age = agentAge(agent.timestamp); return age == null || age > AGENT_STALE_SEC; }
function staleZones(zoneAgents) { const s = []; Object.entries(zoneAgents || {}).forEach(([zn, ag]) => { if (isAgentStale(ag)) s.push(zn); }); return s; }

function getPriority(pct) {
  if (pct === null || pct === undefined) return { label: 'N/A', color: '#6b7280', bg: '#f3f4f6', border: '#d1d5db' };
  if (pct >= 99) return { label: 'SYNCED', color: '#059669', bg: '#ecfdf5', border: '#6ee7b7' };
  if (pct >= 95) return { label: 'LOW', color: '#2563eb', bg: '#eff6ff', border: '#93c5fd' };
  if (pct >= 85) return { label: 'MEDIUM', color: '#d97706', bg: '#fffbeb', border: '#fcd34d' };
  return { label: 'HIGH', color: '#dc2626', bg: '#fef2f2', border: '#fca5a5' };
}
function getProgressColor(pct) { if (pct == null) return '#9ca3af'; if (pct >= 99) return '#059669'; if (pct >= 95) return '#2563eb'; if (pct >= 85) return '#d97706'; return '#dc2626'; }

/* ===== THEME ===== */
const fontStack = "'Inter','DM Sans','Segoe UI',system-ui,-apple-system,sans-serif";
const monoFont = "'IBM Plex Mono','JetBrains Mono','SF Mono','Consolas',monospace";
const T = { bg:'#f1f5f9', bgCard:'#ffffff', bgHover:'#f8fafc', bgSurf:'#f8fafc', bgAcc:'#e2e8f0', bdr:'#e2e8f0', bdrL:'#cbd5e1', txt:'#0f172a', txtM:'#475569', txtD:'#94a3b8', pri:'#4f46e5', priL:'#6366f1', priBg:'#eef2ff', ok:'#059669', okBg:'#ecfdf5', warn:'#d97706', warnBg:'#fffbeb', err:'#dc2626', errBg:'#fef2f2', cyan:'#0891b2' };

const RESPONSIVE_CSS = `
*{box-sizing:border-box}body{margin:0;background:${T.bg};color:${T.txt};font-family:${fontStack};-webkit-font-smoothing:antialiased}
@keyframes spin{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}
.g2{display:grid;grid-template-columns:1fr;gap:14px}
.g3{display:grid;grid-template-columns:1fr;gap:14px}
.g4{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.gc{display:grid;grid-template-columns:1fr;gap:14px}
.hr{flex-direction:column;align-items:flex-start!important;gap:8px!important}
@media(min-width:640px){.g4{grid-template-columns:repeat(4,1fr)}.gc{grid-template-columns:repeat(2,1fr)}.hr{flex-direction:row;align-items:center!important}}
@media(min-width:900px){.g2{grid-template-columns:repeat(2,1fr)}.g3{grid-template-columns:repeat(3,1fr)}}
@media(min-width:1200px){.gc{grid-template-columns:repeat(3,1fr)}}
.bc{transition:box-shadow .2s,border-color .2s;cursor:pointer}.bc:hover{box-shadow:0 6px 20px rgba(0,0,0,.08)!important;border-color:${T.bdrL}!important}
`;

/* ===== SMALL COMPONENTS ===== */
function StatCard({icon:Icon,label,value,sub,color,colorBg}){return(
<div style={{padding:'14px 16px',borderRadius:12,background:T.bgCard,border:'1px solid '+T.bdr,boxShadow:'0 1px 3px rgba(0,0,0,.04)'}}>
<div style={{display:'flex',alignItems:'center',gap:10,marginBottom:8}}>
<div style={{width:34,height:34,borderRadius:9,background:colorBg,display:'flex',alignItems:'center',justifyContent:'center',flexShrink:0}}><Icon size={17} color={color}/></div>
<span style={{fontSize:12,fontWeight:600,color:T.txtM,textTransform:'uppercase',letterSpacing:'.03em'}}>{label}</span>
</div>
<p style={{margin:0,fontSize:22,fontWeight:800,color:T.txt,fontFamily:monoFont,lineHeight:1.1}}>{value}</p>
{sub&&<p style={{margin:'4px 0 0',fontSize:12,color:T.txtD}}>{sub}</p>}
</div>);}

function SyncBar({pct,height}){const h=height||6;const isNull=pct==null;const color=getProgressColor(pct);return(
<div style={{height:h,borderRadius:h,background:T.bgAcc,overflow:'hidden',width:'100%'}}>
{isNull?null:<div style={{height:'100%',borderRadius:h,background:color,width:Math.min(pct,100)+'%',transition:'width .5s'}}/>}
</div>);}

function PriorityBadge({pct}){const p=getPriority(pct);return(
<span style={{display:'inline-flex',alignItems:'center',gap:4,padding:'3px 10px',borderRadius:6,fontSize:11,fontWeight:700,color:p.color,background:p.bg,border:'1px solid '+p.border,whiteSpace:'nowrap'}}>
{p.label==='HIGH'&&<AlertTriangle size={12}/>}{p.label==='SYNCED'&&<CheckCircle size={12}/>}{p.label}
</span>);}

function TopologyBar({topology}){if(!topology||!topology.zones)return null;return(
<div style={{display:'flex',alignItems:'center',gap:10,flexWrap:'wrap',padding:'10px 16px',background:T.bgCard,borderRadius:12,border:'1px solid '+T.bdr,marginBottom:18,boxShadow:'0 1px 3px rgba(0,0,0,.04)'}}>
<Globe size={17} color={T.pri}/>
<span style={{fontSize:13,fontWeight:600,color:T.txtM}}>Realm: <strong style={{color:T.pri}}>{topology.realm}</strong></span>
<span style={{color:T.bdrL}}>|</span>
{topology.zones.map(z=>(<span key={z.name} style={{display:'inline-flex',alignItems:'center',gap:4,padding:'3px 10px',borderRadius:7,fontSize:12,fontWeight:600,background:z.is_master?T.priBg:T.bgSurf,color:z.is_master?T.pri:T.txtM,border:'1px solid '+(z.is_master?'#c7d2fe':T.bdr)}}>{z.is_master&&<Shield size={13}/>}{z.name}{z.is_master&&<span style={{fontSize:10,fontWeight:700,marginLeft:3}}>MASTER</span>}</span>))}
{topology.is_single_zone&&<span style={{padding:'3px 10px',borderRadius:7,fontSize:11,fontWeight:700,background:T.warnBg,color:T.warn,border:'1px solid #fcd34d'}}>SINGLE ZONE</span>}
</div>);}

function SectionTitle({icon:Icon,children}){return(<h3 style={{margin:'0 0 14px',fontSize:15,fontWeight:700,color:T.txt,display:'flex',alignItems:'center',gap:8}}>{Icon&&<Icon size={18} color={T.pri}/>}{children}</h3>);}

/* Badge: zone agent status */
function AgentBadge({agent}){
  if(!agent)return <span style={{padding:'3px 8px',borderRadius:5,fontSize:10,fontWeight:700,background:T.bgAcc,color:T.txtD}}>NO AGENT</span>;
  const age=agentAge(agent.timestamp);const stale=isAgentStale(agent);
  return <span style={{padding:'3px 8px',borderRadius:5,fontSize:10,fontWeight:700,background:stale?T.errBg:'#d1fae5',color:stale?T.err:'#065f46',display:'inline-flex',alignItems:'center',gap:3,border:stale?'1px solid #fca5a5':'none'}}>
    {stale?<WifiOff size={10}/>:<Wifi size={10}/>}{stale?'AGENT LOST':'AGENT'} {agentAgeLabel(age)}
  </span>;
}

/* ===== OVERVIEW TAB ===== */
function OverviewTab({buckets,topology,globalSync,zoneAgents}){
  const list=Object.entries(buckets).map(([name,data])=>{const h=data.history||[];const latest=h[h.length-1]||{};const sz=latest.single_zone===true||latest.no_secondary_data===true;return{name,sz,pct:sz?null:(latest.sync_progress_pct??null),dObj:latest.delta_objects||0,dSize:latest.delta_size||0,pObj:latest.primary?.num_objects||0,pSize:latest.primary?.size_actual||0,replicas:latest.replicas||{},errs:(data.errors||[]).length};});
  const total=list.length;const ml=list.filter(b=>b.pct!==null);
  const hasSecData=topology?.secondary_data_available===true;
  const isSZ=!hasSecData&&ml.length===0;
  const synced=ml.filter(b=>b.pct>=99).length;const lagging=ml.filter(b=>b.pct<85).length;
  const withErr=list.filter(b=>b.errs>0).length;
  const avgPct=ml.length>0?ml.reduce((s,b)=>s+b.pct,0)/ml.length:null;
  const totPObj=list.reduce((s,b)=>s+b.pObj,0);const totPSize=list.reduce((s,b)=>s+b.pSize,0);
  const totDObj=list.reduce((s,b)=>s+b.dObj,0);
  const zones=topology?.zones||[];const master=zones.find(z=>z.is_master);const secondaries=zones.filter(z=>!z.is_master);
  const restZones=topology?.rest_validated_zones||[];
  const za={};
  secondaries.forEach(z=>{za[z.name]={obj:0,size:0,dObj:0,dSize:0,pctSum:0,pctN:0,errs:0};});
  list.forEach(b=>{Object.entries(b.replicas).forEach(([zn,info])=>{if(!za[zn])return;const a=za[zn];a.obj+=info.stats?.num_objects||0;a.size+=info.stats?.size_actual||0;a.dObj+=info.delta_objects||0;a.dSize+=info.delta_size||0;if(info.sync_progress_pct!=null){a.pctSum+=info.sync_progress_pct;a.pctN++;}});if(b.errs>0)secondaries.forEach(z=>{if(za[z.name])za[z.name].errs+=b.errs;});});

  return(<div>
  <div className="g4" style={{marginBottom:22}}>
    <StatCard icon={Database} label="Total Buckets" value={total} sub={isSZ?'Single zone':synced+' fully synced'} color={T.pri} colorBg={T.priBg}/>
    <StatCard icon={TrendingUp} label="Avg Sync %" value={avgPct!=null?avgPct.toFixed(1)+'%':'N/A'} sub={isSZ?'No secondaries':lagging+' critically lagging'} color={getProgressColor(avgPct)} colorBg={getProgressColor(avgPct)+'18'}/>
    <StatCard icon={Activity} label="Pending Obj" value={formatNum(totDObj)} sub={isSZ?'N/A':formatBytes(list.reduce((s,b)=>s+b.dSize,0))+' behind'} color={T.warn} colorBg={T.warnBg}/>
    <StatCard icon={AlertTriangle} label="Bucket Errors" value={withErr} sub={withErr>0?'Needs attention':'All clear'} color={withErr>0?T.err:T.ok} colorBg={withErr>0?T.errBg:T.okBg}/>
  </div>

  {isSZ&&(<div style={{padding:18,borderRadius:12,background:T.warnBg,border:'1px solid #fcd34d',display:'flex',alignItems:'flex-start',gap:12,marginBottom:22}}>
    <AlertCircle size={20} color={T.warn} style={{flexShrink:0,marginTop:2}}/>
    <div><p style={{margin:0,fontSize:14,fontWeight:700,color:'#92400e'}}>Single Zone Deployment</p>
    <p style={{margin:'4px 0 0',fontSize:13,color:'#a16207',lineHeight:1.5}}>No secondary zones configured or no secondary data available. Primary zone stats are collected. See Bucket Sync tab for details.</p></div>
  </div>)}

  <SectionTitle icon={Server}>Zone Details</SectionTitle>
  <div className="g2" style={{marginBottom:22}}>
    {master&&(()=>{const last=(!isSZ&&globalSync&&globalSync.length>0)?globalSync[globalSync.length-1]:null;return(
    <div style={{padding:20,borderRadius:14,background:T.bgCard,border:'2px solid #c7d2fe',boxShadow:'0 2px 10px rgba(79,70,229,.06)'}}>
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:16}}>
        <div style={{display:'flex',alignItems:'center',gap:8}}><Shield size={20} color={T.pri}/><h4 style={{margin:0,fontSize:16,fontWeight:700,color:T.pri}}>{master.name}</h4></div>
        <span style={{padding:'4px 10px',borderRadius:6,fontSize:11,fontWeight:700,background:T.priBg,color:T.pri,border:'1px solid #c7d2fe'}}>MASTER</span>
      </div>
      <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:16}}>
        <div><span style={{fontSize:11,color:T.txtD,textTransform:'uppercase',fontWeight:600}}>Buckets</span><p style={{margin:'4px 0 0',fontSize:24,fontWeight:800,color:T.txt,fontFamily:monoFont}}>{total}</p></div>
        <div><span style={{fontSize:11,color:T.txtD,textTransform:'uppercase',fontWeight:600}}>Objects</span><p style={{margin:'4px 0 0',fontSize:24,fontWeight:800,color:T.txt,fontFamily:monoFont}}>{formatNum(totPObj)}</p></div>
        <div><span style={{fontSize:11,color:T.txtD,textTransform:'uppercase',fontWeight:600}}>Total Size</span><p style={{margin:'4px 0 0',fontSize:24,fontWeight:800,color:T.txt,fontFamily:monoFont}}>{formatBytes(totPSize)}</p></div>
      </div>

      {/* Primary sync status — embedded in master card (symmetric with agent sync in secondary) */}
      {last&&(<div style={{marginTop:14,padding:14,borderRadius:10,background:T.bgSurf,border:'1px solid '+T.bdr}}>
        <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:8}}>
          <span style={{fontSize:12,fontWeight:700,color:T.txtM,textTransform:'uppercase',letterSpacing:'.03em'}}>Sync Status (primary perspective)</span>
          <span style={{fontSize:11,color:T.txtD,fontFamily:monoFont}}>{formatDateTime(last.timestamp)}</span>
        </div>
        {last.status==='error'?(<div style={{padding:8,borderRadius:7,background:T.errBg,color:T.err,fontSize:12,fontWeight:600,border:'1px solid #fca5a5'}}><AlertTriangle size={13} style={{verticalAlign:'middle',marginRight:4}}/>Failed: {last.error}</div>
        ):(<div>
          <div style={{display:'flex',flexWrap:'wrap',gap:10,fontSize:13,color:T.txtM,marginBottom:8}}>
            <span>Metadata: <strong style={{color:last.metadata_sync?.status==='caught up'?T.ok:T.warn}}>{last.metadata_sync?.status||'?'}</strong></span>
          </div>
          {(last.data_sync||[]).map((ds,i)=>{
            const dsTotal=ds.incremental_sync_total||1;const dsDone=ds.incremental_sync_done||0;const dsPct=dsTotal>0?Math.round(dsDone/dsTotal*100):0;
            return(<div key={i} style={{padding:'8px 12px',borderRadius:7,background:T.bgCard,border:'1px solid '+T.bdr,marginBottom:4}}>
              <div style={{display:'flex',flexWrap:'wrap',alignItems:'center',gap:10,marginBottom:5}}>
                <span style={{fontSize:12,fontWeight:600,color:T.txtM}}>Data from <strong style={{color:T.txt}}>{ds.source_zone}</strong></span>
                <span style={{fontSize:12,fontWeight:700,color:ds.status==='caught up'?T.ok:T.warn}}>{ds.status}</span>
                <span style={{fontSize:11,fontFamily:monoFont,color:T.txtD}}>full: {ds.full_sync_done}/{ds.full_sync_total} | incr: {dsDone}/{dsTotal} shards</span>
              </div>
              <SyncBar pct={dsPct} height={5}/>
            </div>);
          })}
        </div>)}
      </div>)}

      {master.endpoints?.[0]&&<p style={{margin:'14px 0 0',fontSize:12,color:T.txtD,fontFamily:monoFont,wordBreak:'break-all',padding:'8px 10px',background:T.bgSurf,borderRadius:7}}>Endpoint: {master.endpoints[0]}</p>}
    </div>);})()}

    {/* Secondary zone cards — with zone agent data */}
    {secondaries.map(z=>{const a=za[z.name]||{};const avg=a.pctN>0?a.pctSum/a.pctN:null;const p=getPriority(avg);const isRest=restZones.includes(z.name);
      const agent=zoneAgents?.[z.name];const agSync=agent?.sync_status;const agErrs=agent?.sync_errors||[];
      return(
      <div key={z.name} style={{padding:20,borderRadius:14,background:T.bgCard,border:'2px solid '+p.border,boxShadow:'0 2px 10px '+p.color+'0a'}}>
        <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:16,flexWrap:'wrap',gap:6}}>
          <div style={{display:'flex',alignItems:'center',gap:8}}><Server size={20} color={p.color}/><h4 style={{margin:0,fontSize:16,fontWeight:700,color:T.txt}}>{z.name}</h4></div>
          <div style={{display:'flex',alignItems:'center',gap:6,flexWrap:'wrap'}}>
            <AgentBadge agent={agent}/>
            {isRest&&<span style={{padding:'3px 7px',borderRadius:5,fontSize:10,fontWeight:700,background:'#dbeafe',color:'#2563eb'}}>REST</span>}
            <PriorityBadge pct={avg}/>
          </div>
        </div>
        {/* Bucket stats summary from primary collector */}
        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr 1fr',gap:12,marginBottom:14}}>
          <div><span style={{fontSize:11,color:T.txtD,textTransform:'uppercase',fontWeight:600}}>Replicated</span><p style={{margin:'4px 0 0',fontSize:20,fontWeight:800,color:T.txt,fontFamily:monoFont}}>{formatNum(a.obj)}</p></div>
          <div><span style={{fontSize:11,color:T.txtD,textTransform:'uppercase',fontWeight:600}}>Size</span><p style={{margin:'4px 0 0',fontSize:20,fontWeight:800,color:T.txt,fontFamily:monoFont}}>{formatBytes(a.size)}</p></div>
          <div><span style={{fontSize:11,color:T.txtD,textTransform:'uppercase',fontWeight:600}}>Pending</span><p style={{margin:'4px 0 0',fontSize:20,fontWeight:800,color:a.dObj>0?T.warn:T.ok,fontFamily:monoFont}}>{formatNum(a.dObj)}</p></div>
          <div><span style={{fontSize:11,color:T.txtD,textTransform:'uppercase',fontWeight:600}}>Errors</span><p style={{margin:'4px 0 0',fontSize:20,fontWeight:800,color:a.errs>0?T.err:T.ok,fontFamily:monoFont}}>{a.errs}</p></div>
        </div>
        <div style={{display:'flex',justifyContent:'space-between',marginBottom:6}}>
          <span style={{fontSize:13,color:T.txtM,fontWeight:600}}>Average Sync</span>
          <span style={{fontSize:14,fontWeight:700,color:p.color,fontFamily:monoFont}}>{avg!=null?avg.toFixed(1)+'%':'N/A'}</span>
        </div>
        <SyncBar pct={avg} height={8}/>

        {/* Agent sync status — the real sync state from the secondary zone */}
        {agSync&&agSync.status==='ok'&&(()=>{const stale=isAgentStale(agent);return(
        <div style={{marginTop:14,padding:14,borderRadius:10,background:T.bgSurf,border:'1px solid '+(stale?'#fca5a5':T.bdr),position:'relative',opacity:stale?0.6:1}}>
          {stale&&<div style={{position:'absolute',top:8,right:10,padding:'3px 8px',borderRadius:5,background:T.errBg,border:'1px solid #fca5a5',display:'flex',alignItems:'center',gap:4}}>
            <WifiOff size={11} color={T.err}/><span style={{fontSize:10,fontWeight:700,color:T.err}}>STALE — last push {agentAgeLabel(agentAge(agent.timestamp))}</span>
          </div>}
          <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:8}}>
            <span style={{fontSize:12,fontWeight:700,color:T.txtM,textTransform:'uppercase',letterSpacing:'.03em'}}>Sync Status (from zone agent)</span>
            <span style={{fontSize:11,color:T.txtD,fontFamily:monoFont}}>{formatDateTime(agent.timestamp)}</span>
          </div>
          <div style={{display:'flex',flexWrap:'wrap',gap:10,fontSize:13,color:T.txtM,marginBottom:8}}>
            <span>Metadata: <strong style={{color:agSync.metadata_sync?.status==='caught up'?T.ok:T.warn}}>{agSync.metadata_sync?.status||'?'}</strong></span>
            {agSync.metadata_sync?.incremental_sync_total>0&&<span style={{fontSize:12,fontFamily:monoFont,color:T.txtD}}>incr: {agSync.metadata_sync.incremental_sync_done}/{agSync.metadata_sync.incremental_sync_total}</span>}
          </div>
          {(agSync.data_sync||[]).map((ds,i)=>{
            const dsTotal=ds.incremental_sync_total||1;const dsDone=ds.incremental_sync_done||0;const dsPct=Math.round(dsDone/dsTotal*100);
            return(<div key={i} style={{padding:'8px 12px',borderRadius:7,background:T.bgCard,border:'1px solid '+T.bdr,marginBottom:4}}>
              <div style={{display:'flex',flexWrap:'wrap',alignItems:'center',gap:10,marginBottom:5}}>
                <span style={{fontSize:12,fontWeight:600,color:T.txtM}}>Data from <strong style={{color:T.txt}}>{ds.source_zone}</strong></span>
                <span style={{fontSize:12,fontWeight:700,color:ds.status==='caught up'?T.ok:T.warn}}>{ds.status}</span>
                <span style={{fontSize:11,fontFamily:monoFont,color:T.txtD}}>full: {ds.full_sync_done}/{ds.full_sync_total} | incr: {dsDone}/{dsTotal} shards</span>
              </div>
              <SyncBar pct={dsPct} height={5}/>
            </div>);
          })}
          {agErrs.length>0&&<div style={{marginTop:6,padding:'5px 10px',borderRadius:6,background:T.errBg,display:'flex',alignItems:'center',gap:5,border:'1px solid #fca5a5'}}>
            <AlertTriangle size={13} color={T.err}/><span style={{fontSize:12,fontWeight:700,color:T.err}}>{agErrs.length} sync error(s) from agent</span>
          </div>}
        </div>);})()}

        {z.endpoints?.[0]&&<p style={{margin:'12px 0 0',fontSize:12,color:T.txtD,fontFamily:monoFont,wordBreak:'break-all',padding:'8px 10px',background:T.bgSurf,borderRadius:7}}>Endpoint: {z.endpoints[0]}</p>}
      </div>);})}
  </div>

  </div>);
}

/* ===== BUCKET SYNC TAB ===== */
function BucketSyncTab({buckets,onSelect,zoneAgents}){
  const[filter,setFilter]=useState('ALL');const[sortBy,setSortBy]=useState('progress');const[sortAsc,setSortAsc]=useState(true);const[search,setSearch]=useState('');
  /* build per-bucket agent sync map: bucket -> [{zone, sources, _stale}] */
  const agentBktMap={};const staleZ=staleZones(zoneAgents);
  Object.entries(zoneAgents||{}).forEach(([zn,ag])=>{
    const zStale=isAgentStale(ag);
    Object.entries(ag.bucket_sync_status||{}).forEach(([bkt,info])=>{
      if(!agentBktMap[bkt])agentBktMap[bkt]=[];
      agentBktMap[bkt].push({zone:zn,_stale:zStale,_agentTs:ag.timestamp,...info});
    });
  });

  const bl=Object.entries(buckets).map(([name,data])=>{const h=data.history||[];const latest=h[h.length-1]||{};const sz=latest.single_zone===true||latest.no_secondary_data===true;return{name,history:h,errors:data.errors||[],sz,pct:sz?null:(latest.sync_progress_pct??null),dObj:latest.delta_objects||0,dSize:latest.delta_size||0,pObj:latest.primary?.num_objects||0,pSize:latest.primary?.size_actual||0,shards:latest.primary?.num_shards||0,replicas:latest.replicas||{},agentSync:agentBktMap[name]||[]};});
  let fl=bl;
  if(search)fl=fl.filter(b=>b.name.toLowerCase().includes(search.toLowerCase()));
  if(filter!=='ALL')fl=fl.filter(b=>getPriority(b.pct).label===filter);
  fl.sort((a,b)=>{let av,bv;if(sortBy==='progress'){av=a.pct??-1;bv=b.pct??-1;}else if(sortBy==='delta'){av=a.dObj;bv=b.dObj;}else if(sortBy==='errors'){av=a.errors.length;bv=b.errors.length;}else{av=a.name.toLowerCase();bv=b.name.toLowerCase();}return sortAsc?(av<bv?-1:av>bv?1:0):(av>bv?-1:av<bv?1:0);});
  const toggleSort=c=>{if(sortBy===c)setSortAsc(!sortAsc);else{setSortBy(c);setSortAsc(true);}};
  return(<div>
    <div className="hr" style={{display:'flex',alignItems:'center',justifyContent:'space-between',gap:12,marginBottom:16}}>
      <div style={{display:'flex',alignItems:'center',gap:8}}><BarChart3 size={18} color={T.pri}/><h2 style={{margin:0,fontSize:17,fontWeight:700,color:T.txt}}>Bucket Sync Details</h2><span style={{fontSize:12,padding:'2px 9px',borderRadius:6,background:T.priBg,color:T.pri,fontWeight:700,fontFamily:monoFont}}>{fl.length}/{bl.length}</span></div>
      <div style={{display:'flex',alignItems:'center',gap:6,flexWrap:'wrap'}}>
        <input value={search} onChange={e=>setSearch(e.target.value)} placeholder="Search bucket..." style={{padding:'5px 10px',borderRadius:7,border:'1px solid '+T.bdr,fontSize:12,fontFamily:fontStack,background:T.bgSurf,width:140,outline:'none',color:T.txt}}/>
        <span style={{color:T.bdrL}}>|</span>
        {['ALL','HIGH','MEDIUM','LOW','SYNCED','N/A'].map(f=>(<button key={f} onClick={()=>setFilter(f)} style={{padding:'4px 10px',borderRadius:6,border:'none',fontSize:11,fontWeight:600,cursor:'pointer',background:filter===f?T.priBg:'transparent',color:filter===f?T.pri:T.txtD,fontFamily:fontStack}}>{f}</button>))}
        <span style={{color:T.bdrL}}>|</span>
        {[{id:'name',l:'Name'},{id:'progress',l:'Sync%'},{id:'delta',l:'Delta'},{id:'errors',l:'Err'}].map(s=>(<button key={s.id} onClick={()=>toggleSort(s.id)} style={{padding:'4px 8px',borderRadius:6,border:'none',fontSize:11,fontWeight:600,cursor:'pointer',background:sortBy===s.id?T.bgAcc:'transparent',color:sortBy===s.id?T.txt:T.txtD,fontFamily:fontStack}}>{s.l}{sortBy===s.id?(sortAsc?'\u2191':'\u2193'):''}</button>))}
      </div>
    </div>
    {/* Stale agent warning banner */}
    {staleZ.length>0&&(<div style={{padding:'10px 16px',borderRadius:10,background:T.errBg,border:'1px solid #fca5a5',display:'flex',alignItems:'center',gap:10,marginBottom:14}}>
      <WifiOff size={16} color={T.err} style={{flexShrink:0}}/>
      <div><span style={{fontSize:13,fontWeight:700,color:T.err}}>Zone agent connection lost</span>
      <span style={{fontSize:12,color:'#991b1b',marginLeft:6}}>— shard sync data from <strong>{staleZ.join(', ')}</strong> is outdated (no push for 5+ min). Bucket shard sync details below may not reflect current state.</span></div>
    </div>)}
    <div className="gc">
      {fl.map(b=>{const p=getPriority(b.pct);const hasR=Object.keys(b.replicas).length>0;return(
        <div key={b.name} className="bc" onClick={()=>onSelect(b.name)} style={{padding:18,borderRadius:14,background:T.bgCard,border:'1px solid '+T.bdr,boxShadow:'0 1px 3px rgba(0,0,0,.04)'}}>
          <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:12,gap:8}}>
            <div style={{display:'flex',alignItems:'center',gap:8,minWidth:0}}><HardDrive size={16} color={T.txtD} style={{flexShrink:0}}/><span style={{fontSize:14,fontWeight:700,color:T.txt,whiteSpace:'nowrap',overflow:'hidden',textOverflow:'ellipsis'}}>{b.name}</span></div>
            <PriorityBadge pct={b.pct}/>
          </div>
          <div style={{display:'flex',gap:16,marginBottom:12,fontSize:13,color:T.txtM}}>
            <span><strong style={{color:T.txt,fontFamily:monoFont}}>{formatNum(b.pObj)}</strong> obj</span>
            <span><strong style={{color:T.txt,fontFamily:monoFont}}>{formatBytes(b.pSize)}</strong></span>
            <span><strong style={{color:T.txt,fontFamily:monoFont}}>{b.shards}</strong> shards</span>
          </div>
          {/* Object-count based sync bars (from primary collector) */}
          {hasR?Object.entries(b.replicas).map(([zone,info])=>{const zp=getPriority(info.sync_progress_pct);return(
            <div key={zone} style={{marginBottom:6,padding:'8px 12px',borderRadius:9,background:T.bgSurf,border:'1px solid '+T.bdr}}>
              <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:5}}>
                <span style={{fontSize:12,fontWeight:600,color:T.txtM}}><ArrowRight size={12} style={{verticalAlign:'middle',marginRight:3}}/>{zone} <span style={{fontSize:10,color:T.txtD}}>(obj count)</span></span>
                <span style={{fontSize:13,fontWeight:700,color:zp.color,fontFamily:monoFont}}>{info.sync_progress_pct!=null?info.sync_progress_pct.toFixed(1)+'%':'N/A'}</span>
              </div>
              <SyncBar pct={info.sync_progress_pct} height={5}/>
              <div style={{display:'flex',gap:10,marginTop:3,fontSize:11,color:T.txtD}}>
                <span>{'\u0394'} {formatNum(info.delta_objects)} obj</span><span>{'\u0394'} {formatBytes(info.delta_size)}</span>
              </div>
            </div>);})
          :(<div style={{padding:'8px 12px',borderRadius:9,background:T.bgSurf,border:'1px solid '+T.bdr}}><p style={{margin:0,fontSize:12,color:T.txtD,fontStyle:'italic'}}>No replicas \u2014 single zone</p></div>)}

          {/* Per-bucket shard sync from zone agent */}
          {b.agentSync.length>0&&b.agentSync.map((as,ai)=>{
            const src=as.sources?.[0];if(!src)return null;
            const isCU=src.status==='caught up';const probs=src.problem_shards||[];
            const shTotal=src.full_sync_total||1;const shDone=src.incremental_sync_done||0;const shPct=Math.round(shDone/shTotal*100);
            const zStale=as._stale;
            return(<div key={ai} style={{marginTop:6,padding:'8px 12px',borderRadius:9,background:zStale?T.bgAcc:(isCU?T.okBg:'#fffbeb'),border:'1px solid '+(zStale?'#fca5a5':(isCU?'#a7f3d0':'#fde68a')),opacity:zStale?0.55:1}}>
              <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:4}}>
                <span style={{fontSize:12,fontWeight:600,color:T.txtM}}>{zStale?<WifiOff size={11} style={{verticalAlign:'middle',marginRight:3}}/>:<Wifi size={11} style={{verticalAlign:'middle',marginRight:3}}/>}{as.zone} <span style={{fontSize:10,color:T.txtD}}>(shard sync{zStale?' — stale':''})</span></span>
                <span style={{fontSize:12,fontWeight:700,color:zStale?T.txtD:(isCU?T.ok:T.warn),fontFamily:monoFont}}>{src.status}{zStale?' ?':''}</span>
              </div>
              <div style={{display:'flex',gap:10,fontSize:11,color:T.txtD}}>
                <span>full: {src.full_sync_done}/{src.full_sync_total}</span>
                <span>incr: {shDone}/{shTotal} shards</span>
                {probs.length>0&&!zStale&&<span style={{color:T.err,fontWeight:600}}>{probs.length} shard(s) behind</span>}
                {zStale&&<span style={{color:T.err,fontWeight:600,fontSize:10}}>last: {agentAgeLabel(agentAge(as._agentTs))}</span>}
              </div>
            </div>);})}

          {b.errors.length>0&&(<div style={{marginTop:8,padding:'6px 10px',borderRadius:7,background:T.errBg,display:'flex',alignItems:'center',gap:6,border:'1px solid #fca5a5'}}><AlertTriangle size={14} color={T.err}/><span style={{fontSize:12,fontWeight:700,color:T.err}}>{b.errors.length} error(s)</span></div>)}
          {b.history.length>1&&!b.sz&&(<div style={{marginTop:10,display:'flex',gap:3,alignItems:'flex-end',height:22}}>
            {b.history.map((snap,idx)=>{const v=snap.sync_progress_pct??0;return <div key={idx} title={formatTime(snap.timestamp)+': '+v.toFixed(1)+'%'} style={{flex:1,height:Math.max(v*.22,2)+'px',borderRadius:2,background:getProgressColor(snap.sync_progress_pct),opacity:.35+(idx/b.history.length)*.65}}/>;})}</div>)}
        </div>);})}
    </div>
    {fl.length===0&&<div style={{padding:40,textAlign:'center',color:T.txtD,fontSize:14,background:T.bgCard,borderRadius:14,border:'1px solid '+T.bdr}}>No buckets match {search?'search "'+search+'"':'filter "'+filter+'"'}.</div>}
  </div>);
}

/* ===== BUCKET DETAIL MODAL ===== */
function BucketDetailModal({name,data,onClose,zoneAgents}){if(!data)return null;const{history=[],errors=[]}=data;const latest=history[history.length-1]||{};const replicas=latest.replicas||{};
  /* agent shard sync for this bucket — with staleness */
  const agentBktSyncs=[];
  Object.entries(zoneAgents||{}).forEach(([zn,ag])=>{const bsync=(ag.bucket_sync_status||{})[name];if(bsync)agentBktSyncs.push({zone:zn,_stale:isAgentStale(ag),_agentTs:ag.timestamp,...bsync});});
return(<div onClick={onClose} style={{position:'fixed',inset:0,background:'rgba(0,0,0,.3)',display:'flex',alignItems:'center',justifyContent:'center',zIndex:1000,padding:20,backdropFilter:'blur(3px)'}}>
<div onClick={e=>e.stopPropagation()} style={{width:'100%',maxWidth:760,maxHeight:'90vh',overflowY:'auto',background:T.bgCard,borderRadius:16,border:'1px solid '+T.bdr,boxShadow:'0 20px 60px rgba(0,0,0,.12)'}}>
  <div style={{padding:'16px 22px',borderBottom:'1px solid '+T.bdr,display:'flex',justifyContent:'space-between',alignItems:'center',position:'sticky',top:0,background:T.bgCard,zIndex:2,borderRadius:'16px 16px 0 0'}}>
    <div style={{display:'flex',alignItems:'center',gap:10,minWidth:0}}><HardDrive size={20} color={T.pri}/>
      <div style={{minWidth:0}}><h3 style={{margin:0,fontSize:17,fontWeight:700,color:T.txt,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{name}</h3>
      <span style={{fontSize:12,color:T.txtM}}>{formatNum(latest.primary?.num_objects||0)} obj | {formatBytes(latest.primary?.size_actual||0)} | {latest.primary?.num_shards||0} shards</span></div>
    </div>
    <button onClick={onClose} style={{width:32,height:32,borderRadius:8,border:'1px solid '+T.bdr,background:T.bgSurf,color:T.txtM,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center',flexShrink:0}}><X size={16}/></button>
  </div>
  <div style={{padding:22}}>
    <div className="g2" style={{marginBottom:22}}>
      <div style={{padding:16,borderRadius:12,border:'2px solid #c7d2fe',background:T.priBg}}>
        <div style={{display:'flex',alignItems:'center',gap:6,marginBottom:10}}><Shield size={16} color={T.pri}/><h4 style={{margin:0,fontSize:14,fontWeight:700,color:T.pri}}>Primary: {latest.primary_zone}</h4></div>
        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:8}}>
          <div><span style={{fontSize:11,color:T.txtD,textTransform:'uppercase'}}>Objects</span><p style={{margin:'3px 0 0',fontSize:18,fontWeight:700,color:T.txt,fontFamily:monoFont}}>{formatNum(latest.primary?.num_objects||0)}</p></div>
          <div><span style={{fontSize:11,color:T.txtD,textTransform:'uppercase'}}>Size</span><p style={{margin:'3px 0 0',fontSize:18,fontWeight:700,color:T.txt,fontFamily:monoFont}}>{formatBytes(latest.primary?.size_actual||0)}</p></div>
        </div>
      </div>
      {Object.entries(replicas).map(([zone,info])=>{const rp=getPriority(info.sync_progress_pct);return(
        <div key={zone} style={{padding:16,borderRadius:12,border:'2px solid '+rp.border,background:rp.bg}}>
          <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:10}}>
            <div style={{display:'flex',alignItems:'center',gap:6}}><Server size={16} color={rp.color}/><h4 style={{margin:0,fontSize:14,fontWeight:700,color:rp.color}}>{zone}</h4></div>
            <PriorityBadge pct={info.sync_progress_pct}/>
          </div>
          <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:8,marginBottom:10}}>
            <div><span style={{fontSize:11,color:T.txtD,textTransform:'uppercase'}}>Objects</span><p style={{margin:'3px 0 0',fontSize:18,fontWeight:700,color:T.txt,fontFamily:monoFont}}>{formatNum(info.stats?.num_objects||0)}</p></div>
            <div><span style={{fontSize:11,color:T.txtD,textTransform:'uppercase'}}>Size</span><p style={{margin:'3px 0 0',fontSize:18,fontWeight:700,color:T.txt,fontFamily:monoFont}}>{formatBytes(info.stats?.size_actual||0)}</p></div>
          </div>
          <div style={{display:'flex',justifyContent:'space-between',marginBottom:5}}>
            <span style={{fontSize:12,color:T.txtM}}>{'\u0394'} {formatNum(info.delta_objects)} obj / {formatBytes(info.delta_size)}</span>
            <span style={{fontSize:14,fontWeight:700,color:rp.color,fontFamily:monoFont}}>{info.sync_progress_pct!=null?info.sync_progress_pct.toFixed(1)+'%':'N/A'}</span>
          </div>
          <SyncBar pct={info.sync_progress_pct??0} height={6}/>
        </div>);})}
    </div>

    {/* Agent per-bucket shard sync */}
    {agentBktSyncs.length>0&&(<div style={{marginBottom:22}}>
      <h4 style={{margin:'0 0 10px',fontSize:13,fontWeight:700,color:T.txtM,textTransform:'uppercase'}}>Shard Sync (from zone agent)</h4>
      {agentBktSyncs.map((as,ai)=>{const zStale=as._stale;return(<div key={ai} style={{padding:14,borderRadius:10,background:zStale?T.bgAcc:T.bgSurf,border:'1px solid '+(zStale?'#fca5a5':T.bdr),marginBottom:8,opacity:zStale?0.6:1}}>
        <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:8}}>
          <span style={{fontSize:13,fontWeight:600,color:T.txt}}>{zStale?<WifiOff size={13} style={{verticalAlign:'middle',marginRight:4}}/>:<Wifi size={13} style={{verticalAlign:'middle',marginRight:4}}/>}{as.zone}</span>
          <div style={{display:'flex',alignItems:'center',gap:6}}>
            {zStale&&<span style={{fontSize:10,fontWeight:700,color:T.err,padding:'2px 7px',borderRadius:4,background:T.errBg,border:'1px solid #fca5a5'}}>STALE — {agentAgeLabel(agentAge(as._agentTs))}</span>}
            {as.sync_disabled&&<span style={{fontSize:11,fontWeight:700,color:T.warn}}>SYNC DISABLED</span>}
          </div>
        </div>
        {(as.sources||[]).map((src,si)=>{const isCU=src.status==='caught up';const probs=src.problem_shards||[];
          return(<div key={si} style={{padding:'8px 12px',borderRadius:7,background:T.bgCard,border:'1px solid '+T.bdr,marginBottom:4}}>
            <div style={{display:'flex',flexWrap:'wrap',alignItems:'center',gap:10,marginBottom:4}}>
              <span style={{fontSize:12,color:T.txtM}}>Source: <strong style={{color:T.txt}}>{src.source_zone}</strong></span>
              <span style={{fontSize:12,fontWeight:700,color:zStale?T.txtD:(isCU?T.ok:T.warn)}}>{src.status}{zStale?' ?':''}</span>
              <span style={{fontSize:11,fontFamily:monoFont,color:T.txtD}}>full: {src.full_sync_done}/{src.full_sync_total} | incr: {src.incremental_sync_done}/{src.incremental_sync_total}</span>
            </div>
            {probs.length>0&&!zStale&&(<div style={{marginTop:4}}>
              {probs.map((ps,pi)=>(<div key={pi} style={{fontSize:11,fontFamily:monoFont,color:T.err,padding:'2px 0'}}>shard {ps.shard_id}: {ps.status}</div>))}
            </div>)}
          </div>);
        })}
      </div>);})}
    </div>)}

    {history.length>0&&!latest.single_zone&&(<div style={{marginBottom:22}}>
      <h4 style={{margin:'0 0 10px',fontSize:13,fontWeight:700,color:T.txtM,textTransform:'uppercase'}}>Sync History</h4>
      <div style={{display:'flex',flexDirection:'column',gap:6}}>
        {history.map((snap,idx)=>{const pct=snap.sync_progress_pct;const isNull=pct==null;const color=getProgressColor(pct);return(
          <div key={idx} style={{display:'flex',alignItems:'center',gap:10,padding:'7px 12px',borderRadius:8,background:T.bgSurf}}>
            <span style={{fontSize:12,color:T.txtD,fontFamily:monoFont,minWidth:50}}>{formatTime(snap.timestamp)}</span>
            <div style={{flex:1,height:20,borderRadius:4,background:T.bgAcc,overflow:'hidden'}}>
              {isNull?<span style={{fontSize:10,color:T.txtD,paddingLeft:8}}>N/A</span>:(
              <div style={{height:'100%',borderRadius:4,background:color,width:pct+'%',display:'flex',alignItems:'center',justifyContent:'flex-end',paddingRight:8}}>
                <span style={{fontSize:10,fontWeight:700,color:'#fff',fontFamily:monoFont}}>{pct.toFixed(1)}%</span>
              </div>)}
            </div>
            <span style={{fontSize:12,color:T.txtD,fontFamily:monoFont,minWidth:75,textAlign:'right'}}>{'\u0394'} {formatNum(snap.delta_objects)} obj</span>
          </div>);})}
      </div>
    </div>)}
    {errors.length>0&&(<div>
      <h4 style={{margin:'0 0 10px',fontSize:13,fontWeight:700,color:T.err,textTransform:'uppercase'}}>Errors ({errors.length})</h4>
      {errors.slice(0,10).map((err,i)=>(<div key={i} style={{padding:'7px 12px',borderRadius:7,background:T.errBg,border:'1px solid #fca5a5',fontSize:12,fontFamily:monoFont,color:T.err,marginBottom:5,wordBreak:'break-all'}}>[{err.error_code}] {err.message} <span style={{color:T.txtD}}>shard:{err.shard_id}</span></div>))}
      {errors.length>10&&<span style={{fontSize:12,color:T.txtD}}>+ {errors.length-10} more</span>}
    </div>)}
  </div>
</div></div>);}

/* ===== ERROR PANEL — merges primary + zone agent errors ===== */
function ErrorPanel({globalErrors,zoneAgents}){
  /* merge agent errors into combined list */
  const agentErrs=[];
  Object.values(zoneAgents||{}).forEach(ag=>{(ag.sync_errors||[]).forEach(e=>{agentErrs.push(e);});});
  const allErrors=[...globalErrors,...agentErrs];

  const byBucket={};allErrors.forEach(e=>{const b=e.bucket||'_global';(byBucket[b]=byBucket[b]||[]).push(e);});
  const byCode={};allErrors.forEach(e=>{const c=e.error_code||'?';byCode[c]=(byCode[c]||0)+1;});
  const bySource={'primary':globalErrors.length};
  Object.entries(zoneAgents||{}).forEach(([zn,ag])=>{bySource[zn]=(ag.sync_errors||[]).length;});

  if(!allErrors.length)return(<div style={{background:T.bgCard,borderRadius:14,border:'1px solid '+T.bdr,padding:30,display:'flex',alignItems:'center',gap:12,boxShadow:'0 1px 3px rgba(0,0,0,.04)'}}><CheckCircle size={22} color={T.ok}/><span style={{fontSize:15,color:T.txtM,fontWeight:500}}>No sync errors detected.</span></div>);
  return(<div style={{background:T.bgCard,borderRadius:14,border:'1px solid '+T.bdr,overflow:'hidden',boxShadow:'0 1px 3px rgba(0,0,0,.04)'}}>
    <div style={{padding:'14px 20px',borderBottom:'1px solid '+T.bdr,display:'flex',alignItems:'center',gap:8,background:T.errBg}}><AlertTriangle size={18} color={T.err}/><h2 style={{margin:0,fontSize:17,fontWeight:700,color:T.txt}}>Sync Errors</h2><span style={{fontSize:13,padding:'2px 8px',borderRadius:5,background:T.errBg,color:T.err,fontWeight:700,fontFamily:monoFont,border:'1px solid #fca5a5'}}>{allErrors.length}</span></div>
    <div style={{padding:20}}>
      {/* Source breakdown */}
      <div style={{display:'flex',flexWrap:'wrap',gap:8,marginBottom:12}}>
        {Object.entries(bySource).filter(([,c])=>c>0).map(([src,cnt])=>(<div key={src} style={{padding:'5px 10px',borderRadius:7,background:src==='primary'?T.priBg:'#d1fae5',border:'1px solid '+(src==='primary'?'#c7d2fe':'#a7f3d0'),display:'flex',alignItems:'center',gap:5}}>
          <span style={{fontSize:12,fontWeight:600,color:T.txt}}>{src}</span>
          <span style={{fontSize:11,fontWeight:700,color:T.err,padding:'1px 5px',borderRadius:4,background:T.errBg}}>{cnt}</span>
        </div>))}
      </div>
      <div style={{display:'flex',flexWrap:'wrap',gap:8,marginBottom:16}}>{Object.entries(byCode).sort((a,b)=>b[1]-a[1]).map(([code,cnt])=>(<div key={code} style={{padding:'6px 12px',borderRadius:7,background:T.bgSurf,border:'1px solid '+T.bdr,display:'flex',alignItems:'center',gap:6}}><Zap size={13} color={T.warn}/><span style={{fontSize:13,fontWeight:600,color:T.txt,fontFamily:monoFont}}>Code {code}</span><span style={{fontSize:12,fontWeight:700,color:T.err,padding:'1px 6px',borderRadius:4,background:T.errBg}}>{cnt}</span></div>))}</div>
      <div className="gc">{Object.entries(byBucket).map(([bucket,errs])=>(<div key={bucket} style={{padding:'12px 14px',borderRadius:10,background:T.bgSurf,border:'1px solid '+T.bdr}}>
        <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:8}}><span style={{fontSize:14,fontWeight:700,color:T.txt}}>{bucket==='_global'?'Global':bucket}</span><span style={{fontSize:13,fontWeight:700,color:T.err,fontFamily:monoFont}}>{errs.length}</span></div>
        {errs.slice(0,3).map((err,i)=>(<div key={i} style={{fontSize:12,color:T.txtM,fontFamily:monoFont,padding:'3px 0',borderTop:i>0?'1px solid '+T.bdr:'none',overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>
          [{err.error_code}] {err.message} {err._source==='zone_agent'&&<span style={{color:T.cyan,fontSize:10,fontWeight:700}}> [{err._agent_zone}]</span>}
        </div>))}
        {errs.length>3&&<span style={{fontSize:11,color:T.txtD}}>+ {errs.length-3} more</span>}
      </div>))}</div>
    </div>
  </div>);
}

/* ===== SETUP WIZARD ===== */
function SetupWizard({onConnect,onSkipDemo,healthData}){
  const[ak,setAk]=useState('');const[sk,setSk]=useState('');const[useRest,setUseRest]=useState(false);const[error,setError]=useState('');const[busy,setBusy]=useState(false);
  const cephOk=healthData?.ceph_access===true;const collRunning=healthData?.collector_running===true;
  useEffect(()=>{if(collRunning)onConnect();},[collRunning]);
  const handleStart=async()=>{if(useRest&&(!ak||!sk)){setError('Keys required for REST mode.');return;}setBusy(true);setError('');try{const cfg={collection_interval:60,use_rest_for_bucket_stats:useRest,...(useRest?{access_key:ak,secret_key:sk}:{})};const resp=await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});const d=await resp.json();if(resp.ok)onConnect();else setError(d.error||'Failed');}catch{setError('Cannot reach API server.');}finally{setBusy(false);}};
  return(<div style={{minHeight:'100vh',background:T.bg,display:'flex',alignItems:'center',justifyContent:'center',fontFamily:fontStack,padding:20}}>
    <div style={{width:'100%',maxWidth:500,background:T.bgCard,borderRadius:16,border:'1px solid '+T.bdr,padding:28,boxShadow:'0 4px 24px rgba(0,0,0,.06)'}}>
      <div style={{display:'flex',alignItems:'center',gap:10,marginBottom:22}}><div style={{width:40,height:40,borderRadius:11,background:'linear-gradient(135deg,'+T.pri+','+T.cyan+')',display:'flex',alignItems:'center',justifyContent:'center'}}><Database size={20} color="#fff"/></div><div><h1 style={{margin:0,fontSize:19,fontWeight:700,color:T.txt}}>RGW Multisite Monitor</h1><p style={{margin:0,fontSize:12,color:T.txtD}}>Setup</p></div></div>
      <div style={{padding:12,borderRadius:9,background:cephOk?T.okBg:T.errBg,border:'1px solid '+(cephOk?'#6ee7b7':'#fca5a5'),marginBottom:18,display:'flex',alignItems:'center',gap:8}}>{cephOk?<CheckCircle size={16} color={T.ok}/>:<AlertCircle size={16} color={T.err}/>}<span style={{fontSize:13,fontWeight:600,color:cephOk?T.ok:T.err}}>{cephOk?'Ceph cluster verified':(healthData?.ceph_error||'Cannot reach cluster')}</span></div>
      <label style={{display:'flex',alignItems:'center',gap:8,padding:'10px 0',fontSize:13,color:T.txt,cursor:'pointer'}}><input type="checkbox" checked={useRest} onChange={e=>setUseRest(e.target.checked)} style={{width:16,height:16}}/>Use REST API for secondary zone bucket stats</label>
      {useRest&&<div style={{display:'flex',flexDirection:'column',gap:8,marginTop:6}}><input value={ak} onChange={e=>setAk(e.target.value)} placeholder="Access Key" style={{padding:'9px 12px',borderRadius:7,border:'1px solid '+T.bdr,fontSize:13,fontFamily:monoFont,background:T.bgSurf,color:T.txt}}/><input value={sk} onChange={e=>setSk(e.target.value)} placeholder="Secret Key" type="password" style={{padding:'9px 12px',borderRadius:7,border:'1px solid '+T.bdr,fontSize:13,fontFamily:monoFont,background:T.bgSurf,color:T.txt}}/></div>}
      {error&&<div style={{padding:10,borderRadius:7,background:T.errBg,color:T.err,fontSize:12,fontWeight:600,marginTop:10,border:'1px solid #fca5a5'}}>{error}</div>}
      <div style={{display:'flex',gap:8,marginTop:18}}><button onClick={handleStart} disabled={!cephOk||busy} style={{flex:1,padding:'11px 18px',borderRadius:9,border:'none',fontSize:14,fontWeight:700,cursor:cephOk?'pointer':'not-allowed',background:cephOk?T.pri:T.bgAcc,color:cephOk?'#fff':T.txtD,fontFamily:fontStack}}>{busy?'Starting...':'Start Monitor'}</button><button onClick={onSkipDemo} style={{padding:'11px 18px',borderRadius:9,border:'1px solid '+T.bdr,fontSize:14,fontWeight:600,cursor:'pointer',background:T.bgCard,color:T.txtM,fontFamily:fontStack}}>Demo</button></div>
    </div>
  </div>);
}

/* ===== MAIN DASHBOARD ===== */
function Dashboard({data,onRefresh}){
  const[selBucket,setSelBucket]=useState(null);const[tab,setTab]=useState('overview');
  const buckets=data.buckets||{};const topo=data.topology||{};const gErrors=data.global_errors||[];const gSync=data.global_sync||[];
  const zoneAgents=data.zone_agents||{};
  /* total error count including agents */
  const agentErrCount=Object.values(zoneAgents).reduce((s,a)=>s+(a.sync_errors||[]).length,0);
  const totalErrCount=gErrors.length+agentErrCount;
  return(<div style={{minHeight:'100vh',background:T.bg,fontFamily:fontStack,color:T.txt}}>
    <style>{RESPONSIVE_CSS}</style>
    <div style={{padding:'12px 20px',borderBottom:'1px solid '+T.bdr,background:T.bgCard,display:'flex',alignItems:'center',justifyContent:'space-between',position:'sticky',top:0,zIndex:100,boxShadow:'0 1px 4px rgba(0,0,0,.04)',flexWrap:'wrap',gap:8}}>
      <div style={{display:'flex',alignItems:'center',gap:10}}><div style={{width:34,height:34,borderRadius:9,background:'linear-gradient(135deg,'+T.pri+','+T.cyan+')',display:'flex',alignItems:'center',justifyContent:'center',flexShrink:0}}><Database size={18} color="#fff"/></div><div><h1 style={{margin:0,fontSize:16,fontWeight:700}}>RGW Multisite Monitor</h1><p style={{margin:0,fontSize:11,color:T.txtD}}>Replication sync tracking</p></div></div>
      <div style={{display:'flex',alignItems:'center',gap:8}}><span style={{fontSize:11,color:T.txtD,fontFamily:monoFont}}>{formatDateTime(data.last_update)}</span><button onClick={onRefresh} style={{padding:'6px 14px',borderRadius:7,border:'1px solid '+T.bdr,background:T.bgCard,color:T.txtM,fontSize:12,fontWeight:600,cursor:'pointer',fontFamily:fontStack,display:'inline-flex',alignItems:'center',gap:5}}><RefreshCw size={14}/> Refresh</button></div>
    </div>
    <div style={{maxWidth:1400,margin:'0 auto',padding:'18px 16px'}}>
      <TopologyBar topology={topo}/>
      <div style={{display:'flex',gap:3,marginBottom:18,padding:3,background:T.bgCard,borderRadius:9,border:'1px solid '+T.bdr,width:'fit-content',boxShadow:'0 1px 3px rgba(0,0,0,.04)',flexWrap:'wrap'}}>
        {[{id:'overview',l:'Overview',ic:Layers},{id:'buckets',l:'Bucket Sync',ic:BarChart3},{id:'errors',l:'Errors',ic:AlertTriangle}].map(t=>(<button key={t.id} onClick={()=>setTab(t.id)} style={{padding:'8px 18px',borderRadius:7,border:'none',fontSize:13,fontWeight:600,cursor:'pointer',fontFamily:fontStack,display:'flex',alignItems:'center',gap:6,background:tab===t.id?T.priBg:'transparent',color:tab===t.id?T.pri:T.txtD,transition:'all .15s'}}><t.ic size={15}/>{t.l}{t.id==='errors'&&totalErrCount>0&&<span style={{fontSize:10,fontWeight:700,color:T.err,background:T.errBg,padding:'1px 6px',borderRadius:4,border:'1px solid #fca5a5'}}>{totalErrCount}</span>}</button>))}
      </div>
      {tab==='overview'&&<OverviewTab buckets={buckets} topology={topo} globalSync={gSync} zoneAgents={zoneAgents}/>}
      {tab==='buckets'&&<BucketSyncTab buckets={buckets} onSelect={setSelBucket} zoneAgents={zoneAgents}/>}
      {tab==='errors'&&<ErrorPanel globalErrors={gErrors} zoneAgents={zoneAgents}/>}
    </div>
    {selBucket&&buckets[selBucket]&&<BucketDetailModal name={selBucket} data={buckets[selBucket]} onClose={()=>setSelBucket(null)} zoneAgents={zoneAgents}/>}
  </div>);
}

/* ===== ROOT APP ===== */
export default function App(){
  const[mode,setMode]=useState(null);const[data,setData]=useState(null);const[health,setHealth]=useState(null);
  const fetchLive=async()=>{try{setData(await(await fetch('/api/dashboard?last_n=6')).json());}catch{}};
  useEffect(()=>{fetch('/api/health').then(r=>r.json()).then(d=>{setHealth(d);if(d.collector_running){setMode('live');fetchLive();}else setMode('setup');}).catch(()=>{setMode('demo');setData(generateMockData());});},[]);
  useEffect(()=>{if(mode==='demo'){const iv=setInterval(()=>setData(generateMockData()),30000);return()=>clearInterval(iv);}if(mode==='live'){const iv=setInterval(fetchLive,15000);return()=>clearInterval(iv);}},[mode]);
  if(mode==='setup')return<SetupWizard healthData={health} onConnect={()=>{setMode('live');fetchLive();}} onSkipDemo={()=>{setMode('demo');setData(generateMockData());}}/>;
  if(!data)return<div style={{minHeight:'100vh',background:T.bg,display:'flex',alignItems:'center',justifyContent:'center',fontFamily:fontStack}}><div style={{textAlign:'center'}}><RefreshCw size={26} color={T.pri} style={{animation:'spin 1s linear infinite'}}/><p style={{color:T.txtM,marginTop:12,fontSize:14}}>Loading...</p></div></div>;
  return<Dashboard data={data} onRefresh={()=>{if(mode==='demo')setData(generateMockData());else fetchLive();}}/>;
}