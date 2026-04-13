/**
 * bobo — MySQL & PostgreSQL 管理後端 v3
 */
const express = require('express');
const mysql   = require('mysql2/promise');
const { Client: PgClient } = require('pg');
const cors    = require('cors');
const path    = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── 工具 ───────────────────────────────────────────
function esc(name) {
  const safe = String(name).replace(/[^a-zA-Z0-9_\-. ]/g, '');
  if (!safe) throw new Error(`識別符包含非法字元: ${name}`);
  return safe;
}
function pgId(name) { return `"${String(name).replace(/"/g,'""')}"` }

// ── MySQL 連線 ─────────────────────────────────────
async function getMysqlConn(cfg) {
  return mysql.createConnection({
    host:'localhost',port:3306,...cfg,
    port:parseInt(cfg.port)||3306,
    password:cfg.password||'',
    database:cfg.database||undefined,
    multipleStatements:false,connectTimeout:8000,
  });
}
function wrapMysql(fn) {
  return async(req,res)=>{
    let conn;
    try { conn=await getMysqlConn(req.body.config||{}); await fn(req,res,conn); }
    catch(err){ console.error('[mysql]',err.message); res.status(500).json({error:err.message}); }
    finally { if(conn) conn.end().catch(()=>{}); }
  };
}
function buildMysqlColDef(col) {
  let def=`\`${esc(col.name)}\` ${esc(col.type)}`;
  if(col.length) def+=`(${parseInt(col.length)})`;
  if(col.unsigned) def+=' UNSIGNED';
  if(col.notNull) def+=' NOT NULL';
  if(col.default!==undefined&&col.default!==''){
    const d=String(col.default).toUpperCase();
    def+=d==='NULL'?' DEFAULT NULL':d==='CURRENT_TIMESTAMP'?' DEFAULT CURRENT_TIMESTAMP':` DEFAULT '${String(col.default).replace(/'/g,"''")}'`;
  }
  if(col.autoIncrement) def+=' AUTO_INCREMENT';
  if(col.comment) def+=` COMMENT '${String(col.comment).replace(/'/g,"''")}'`;
  return def;
}

// ── PostgreSQL 連線 ────────────────────────────────
async function getPgConn(cfg) {
  const client=new PgClient({
    host:cfg.host||'localhost',port:parseInt(cfg.port)||5432,
    user:cfg.user,password:cfg.password||'',
    database:cfg.database||'postgres',
    connectionTimeoutMillis:8000,
    ssl:cfg.ssl?{rejectUnauthorized:false}:false,
  });
  await client.connect();
  return client;
}
function wrapPg(fn) {
  return async(req,res)=>{
    let conn;
    try { conn=await getPgConn(req.body.config||{}); await fn(req,res,conn); }
    catch(err){ console.error('[pg]',err.message); res.status(500).json({error:err.message}); }
    finally { if(conn) conn.end().catch(()=>{}); }
  };
}
function buildPgColDef(col) {
  let type=col.type.toUpperCase();
  if(col.autoIncrement){ type=type==='BIGINT'?'BIGSERIAL':type==='SMALLINT'?'SMALLSERIAL':'SERIAL'; }
  let def=`${pgId(col.name)} ${type}`;
  if(col.length&&['VARCHAR','CHAR','DECIMAL','NUMERIC'].includes(col.type.toUpperCase())) def+=`(${parseInt(col.length)})`;
  if(col.notNull&&!col.autoIncrement) def+=' NOT NULL';
  if(col.default!==undefined&&col.default!==''&&!col.autoIncrement){
    const d=String(col.default).toUpperCase();
    def+=d==='NULL'?' DEFAULT NULL':d==='CURRENT_TIMESTAMP'?' DEFAULT CURRENT_TIMESTAMP':` DEFAULT '${String(col.default).replace(/'/g,"''")}'`;
  }
  return def;
}

// ════════════════════════════════════════════════════
//  MySQL 路由 /api/*
// ════════════════════════════════════════════════════
app.post('/api/connect',wrapMysql(async(req,res,conn)=>{
  const [rows]=await conn.query('SELECT VERSION() AS version,USER() AS user,NOW() AS now');
  res.json({ok:true,version:rows[0].version,user:rows[0].user,now:rows[0].now});
}));
app.post('/api/databases',wrapMysql(async(req,res,conn)=>{
  const [rows]=await conn.query('SHOW DATABASES');
  const skip=['information_schema','performance_schema','sys','mysql'];
  res.json({databases:rows.map(r=>r.Database).filter(d=>!skip.includes(d))});
}));
app.post('/api/tables',wrapMysql(async(req,res,conn)=>{
  const{database}=req.body; if(!database) return res.status(400).json({error:'請指定 database'});
  const[rows]=await conn.query(
    `SELECT TABLE_NAME,TABLE_ROWS,ROUND((DATA_LENGTH+INDEX_LENGTH)/1024,1) AS size_kb,CREATE_TIME,ENGINE,TABLE_COMMENT
     FROM information_schema.TABLES WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME`,[database]);
  res.json({tables:rows});
}));
app.post('/api/columns',wrapMysql(async(req,res,conn)=>{
  const{database,table}=req.body; if(!database||!table) return res.status(400).json({error:'缺少參數'});
  await conn.query(`USE \`${esc(database)}\``);
  const[rows]=await conn.query(`SHOW FULL COLUMNS FROM \`${esc(table)}\``);
  res.json({columns:rows});
}));
app.post('/api/rows',wrapMysql(async(req,res,conn)=>{
  const{database,table,page=1,pageSize=50,search=''}=req.body;
  if(!database||!table) return res.status(400).json({error:'缺少參數'});
  const db=esc(database),tbl=esc(table);
  await conn.query(`USE \`${db}\``);
  const[cols]=await conn.query(`SHOW COLUMNS FROM \`${tbl}\``);
  const colNames=cols.map(c=>c.Field);
  let where='',params=[];
  if(search.trim()){
    const s=search.replace(/\\/g,'\\\\').replace(/%/g,'\\%').replace(/_/g,'\\_');
    where=`WHERE (${colNames.map(c=>`\`${esc(c)}\` LIKE ?`).join(' OR ')})`;
    colNames.forEach(()=>params.push(`%${s}%`));
  }
  const[[{total}]]=await conn.query(`SELECT COUNT(*) AS total FROM \`${tbl}\` ${where}`,params);
  const offset=(parseInt(page)-1)*parseInt(pageSize);
  const[rows]=await conn.query(`SELECT * FROM \`${tbl}\` ${where} LIMIT ? OFFSET ?`,[...params,parseInt(pageSize),offset]);
  res.json({rows,total:Number(total),columns:colNames});
}));
app.post('/api/insert',wrapMysql(async(req,res,conn)=>{
  const{database,table,data}=req.body; if(!database||!table||!data) return res.status(400).json({error:'缺少參數'});
  await conn.query(`USE \`${esc(database)}\``);
  const cols=Object.keys(data).map(c=>`\`${esc(c)}\``).join(', ');
  const ph=Object.keys(data).map(()=>'?').join(', ');
  const vals=Object.values(data).map(v=>v===''?null:v);
  const[result]=await conn.query(`INSERT INTO \`${esc(table)}\` (${cols}) VALUES (${ph})`,vals);
  res.json({ok:true,insertId:result.insertId});
}));
app.post('/api/update',wrapMysql(async(req,res,conn)=>{
  const{database,table,data,where}=req.body; if(!database||!table||!data||!where) return res.status(400).json({error:'缺少參數'});
  await conn.query(`USE \`${esc(database)}\``);
  if(!Object.keys(where).length) return res.status(400).json({error:'WHERE 條件不可為空'});
  if(!Object.keys(data).length) return res.status(400).json({error:'更新欄位不可為空'});
  const sets=Object.keys(data).map(c=>`\`${esc(c)}\` = ?`).join(', ');
  const cond=Object.keys(where).map(c=>`\`${esc(c)}\` = ?`).join(' AND ');
  const[result]=await conn.query(`UPDATE \`${esc(table)}\` SET ${sets} WHERE ${cond}`,[...Object.values(data).map(v=>v===''?null:v),...Object.values(where)]);
  res.json({ok:true,affectedRows:result.affectedRows});
}));
app.post('/api/delete',wrapMysql(async(req,res,conn)=>{
  const{database,table,where}=req.body; if(!database||!table||!where) return res.status(400).json({error:'缺少參數'});
  await conn.query(`USE \`${esc(database)}\``);
  if(!Object.keys(where).length) return res.status(400).json({error:'WHERE 條件不可為空'});
  const cond=Object.keys(where).map(c=>`\`${esc(c)}\` = ?`).join(' AND ');
  const[result]=await conn.query(`DELETE FROM \`${esc(table)}\` WHERE ${cond}`,Object.values(where));
  res.json({ok:true,affectedRows:result.affectedRows});
}));
app.post('/api/sql',wrapMysql(async(req,res,conn)=>{
  const{database,sql}=req.body; if(!sql) return res.status(400).json({error:'請輸入 SQL'});
  if(database) await conn.query(`USE \`${esc(database)}\``);
  const norm=sql.trim().toUpperCase();
  const isRead=['SELECT','SHOW','DESCRIBE','DESC','EXPLAIN'].some(k=>norm.startsWith(k));
  if(isRead){
    const[rows,fields]=await conn.query(sql);
    return res.json({type:'select',rows,columns:(fields||[]).map(f=>f.name)});
  } else {
    const[result]=await conn.query(sql);
    const affected=result?.affectedRows??0,insertId=result?.insertId??null;
    const isDDL=/^\s*(CREATE|DROP|ALTER|RENAME|TRUNCATE)/i.test(sql.trim());
    return res.json({type:'write',affectedRows:affected,insertId,message:isDDL?'執行成功':`執行成功 — 影響 ${affected} 筆資料`});
  }
}));
app.post('/api/database/create',wrapMysql(async(req,res,conn)=>{
  const{database,charset='utf8mb4',collation='utf8mb4_unicode_ci'}=req.body;
  if(!database) return res.status(400).json({error:'請輸入資料庫名稱'});
  await conn.query(`CREATE DATABASE \`${esc(database)}\` CHARACTER SET ${esc(charset)} COLLATE ${esc(collation)}`);
  res.json({ok:true});
}));
app.post('/api/database/drop',wrapMysql(async(req,res,conn)=>{
  const{database}=req.body; if(!database) return res.status(400).json({error:'請指定資料庫'});
  await conn.query(`DROP DATABASE \`${esc(database)}\``);
  res.json({ok:true});
}));
app.post('/api/table/create',wrapMysql(async(req,res,conn)=>{
  const{database,table,columns,engine='InnoDB',charset='utf8mb4'}=req.body;
  if(!database||!table||!columns?.length) return res.status(400).json({error:'缺少必要參數'});
  const colDefs=columns.map(col=>{
    let def=`\`${esc(col.name)}\` ${esc(col.type)}`;
    if(col.length) def+=`(${parseInt(col.length)})`;
    if(col.unsigned) def+=' UNSIGNED';
    if(col.notNull) def+=' NOT NULL';
    if(col.default!==undefined&&col.default!==''){
      const d=col.default.toUpperCase();
      def+=d==='NULL'?' DEFAULT NULL':d==='CURRENT_TIMESTAMP'?' DEFAULT CURRENT_TIMESTAMP':` DEFAULT '${col.default.replace(/'/g,"''")}'`;
    }
    if(col.autoIncrement) def+=' AUTO_INCREMENT';
    if(col.comment) def+=` COMMENT '${col.comment.replace(/'/g,"''")}'`;
    return def;
  });
  const pks=columns.filter(c=>c.primaryKey).map(c=>`\`${esc(c.name)}\``);
  if(pks.length) colDefs.push(`PRIMARY KEY (${pks.join(', ')})`);
  columns.filter(c=>c.unique&&!c.primaryKey).forEach(u=>colDefs.push(`UNIQUE KEY \`uq_${esc(u.name)}\` (\`${esc(u.name)}\`)`));
  columns.filter(c=>c.index&&!c.primaryKey&&!c.unique).forEach(ix=>colDefs.push(`KEY \`idx_${esc(ix.name)}\` (\`${esc(ix.name)}\`)`));
  const sql=`CREATE TABLE \`${esc(database)}\`.\`${esc(table)}\` (\n  ${colDefs.join(',\n  ')}\n) ENGINE=${esc(engine)} DEFAULT CHARSET=${esc(charset)}`;
  await conn.query(sql); res.json({ok:true,sql});
}));
app.post('/api/table/drop',wrapMysql(async(req,res,conn)=>{
  const{database,table}=req.body; if(!database||!table) return res.status(400).json({error:'缺少參數'});
  await conn.query(`DROP TABLE \`${esc(database)}\`.\`${esc(table)}\``); res.json({ok:true});
}));
app.post('/api/table/truncate',wrapMysql(async(req,res,conn)=>{
  const{database,table}=req.body; if(!database||!table) return res.status(400).json({error:'缺少參數'});
  await conn.query(`TRUNCATE TABLE \`${esc(database)}\`.\`${esc(table)}\``); res.json({ok:true});
}));
app.post('/api/table/rename',wrapMysql(async(req,res,conn)=>{
  const{database,table,newName}=req.body; if(!database||!table||!newName) return res.status(400).json({error:'缺少參數'});
  await conn.query(`RENAME TABLE \`${esc(database)}\`.\`${esc(table)}\` TO \`${esc(database)}\`.\`${esc(newName)}\``); res.json({ok:true});
}));
app.post('/api/table/ddl',wrapMysql(async(req,res,conn)=>{
  const{database,table}=req.body; if(!database||!table) return res.status(400).json({error:'缺少參數'});
  await conn.query(`USE \`${esc(database)}\``);
  const[rows]=await conn.query(`SHOW CREATE TABLE \`${esc(table)}\``);
  res.json({ddl:rows[0]['Create Table']});
}));
app.post('/api/column/add',wrapMysql(async(req,res,conn)=>{
  const{database,table,column}=req.body; if(!database||!table||!column) return res.status(400).json({error:'缺少參數'});
  await conn.query(`USE \`${esc(database)}\``);
  const def=buildMysqlColDef(column);
  const pos=column.after?`AFTER \`${esc(column.after)}\``:column.first?'FIRST':'';
  await conn.query(`ALTER TABLE \`${esc(table)}\` ADD COLUMN ${def} ${pos}`); res.json({ok:true});
}));
app.post('/api/column/modify',wrapMysql(async(req,res,conn)=>{
  const{database,table,oldName,column}=req.body; if(!database||!table||!oldName||!column) return res.status(400).json({error:'缺少參數'});
  await conn.query(`USE \`${esc(database)}\``);
  await conn.query(`ALTER TABLE \`${esc(table)}\` CHANGE \`${esc(oldName)}\` ${buildMysqlColDef(column)}`); res.json({ok:true});
}));
app.post('/api/column/drop',wrapMysql(async(req,res,conn)=>{
  const{database,table,column}=req.body; if(!database||!table||!column) return res.status(400).json({error:'缺少參數'});
  await conn.query(`USE \`${esc(database)}\``);
  await conn.query(`ALTER TABLE \`${esc(table)}\` DROP COLUMN \`${esc(column)}\``); res.json({ok:true});
}));
app.post('/api/indexes',wrapMysql(async(req,res,conn)=>{
  const{database,table}=req.body; if(!database||!table) return res.status(400).json({error:'缺少參數'});
  await conn.query(`USE \`${esc(database)}\``);
  const[rows]=await conn.query(`SHOW INDEX FROM \`${esc(table)}\``);
  res.json({indexes:rows});
}));
app.post('/api/index/add',wrapMysql(async(req,res,conn)=>{
  const{database,table,indexName,columns,unique=false}=req.body;
  if(!database||!table||!indexName||!columns?.length) return res.status(400).json({error:'缺少參數'});
  await conn.query(`USE \`${esc(database)}\``);
  const cols=columns.map(c=>`\`${esc(c)}\``).join(', ');
  await conn.query(`ALTER TABLE \`${esc(table)}\` ADD ${unique?'UNIQUE ':''}INDEX \`${esc(indexName)}\` (${cols})`); res.json({ok:true});
}));
app.post('/api/index/drop',wrapMysql(async(req,res,conn)=>{
  const{database,table,indexName}=req.body; if(!database||!table||!indexName) return res.status(400).json({error:'缺少參數'});
  await conn.query(`USE \`${esc(database)}\``);
  await conn.query(`ALTER TABLE \`${esc(table)}\` DROP INDEX \`${esc(indexName)}\``); res.json({ok:true});
}));

// ════════════════════════════════════════════════════
//  PostgreSQL 路由 /api/pg/*
// ════════════════════════════════════════════════════
app.post('/api/pg/connect',wrapPg(async(req,res,conn)=>{
  const r=await conn.query('SELECT version(),current_user,now()');
  const row=r.rows[0];
  const ver=(row.version||'').match(/PostgreSQL\s+([\d.]+)/)?.[1]||row.version;
  res.json({ok:true,version:ver,user:row.current_user,now:row.now});
}));
app.post('/api/pg/databases',wrapPg(async(req,res,conn)=>{
  const r=await conn.query(
    `SELECT schema_name FROM information_schema.schemata
     WHERE schema_name NOT IN ('information_schema','pg_catalog','pg_toast')
       AND schema_name NOT LIKE 'pg_%'
     ORDER BY schema_name`);
  res.json({databases:r.rows.map(row=>row.schema_name)});
}));
app.post('/api/pg/tables',wrapPg(async(req,res,conn)=>{
  const{database:schema}=req.body; if(!schema) return res.status(400).json({error:'請指定 schema'});
  const r=await conn.query(
    `SELECT t.table_name AS "TABLE_NAME",
       COALESCE(s.n_live_tup,0) AS "TABLE_ROWS",
       ROUND(COALESCE(pg_total_relation_size(c.oid),0)::numeric/1024,1) AS "size_kb",
       NULL AS "CREATE_TIME",'heap' AS "ENGINE",
       obj_description(c.oid,'pg_class') AS "TABLE_COMMENT"
     FROM information_schema.tables t
     LEFT JOIN pg_class c ON c.relname=t.table_name AND c.relnamespace=(SELECT oid FROM pg_namespace WHERE nspname=$1)
     LEFT JOIN pg_stat_user_tables s ON s.relname=t.table_name AND s.schemaname=t.table_schema
     WHERE t.table_schema=$1 AND t.table_type='BASE TABLE'
     ORDER BY t.table_name`,[schema]);
  res.json({tables:r.rows});
}));
app.post('/api/pg/columns',wrapPg(async(req,res,conn)=>{
  const{database:schema,table}=req.body; if(!schema||!table) return res.status(400).json({error:'缺少參數'});
  const r=await conn.query(
    `SELECT c.column_name AS "Field",
       c.udt_name||CASE WHEN c.character_maximum_length IS NOT NULL THEN '('||c.character_maximum_length||')'
         WHEN c.numeric_precision IS NOT NULL AND c.data_type IN ('numeric','decimal') THEN '('||c.numeric_precision||','||COALESCE(c.numeric_scale,0)||')'
         ELSE '' END AS "Type",
       c.is_nullable AS "Null",
       CASE WHEN pk.column_name IS NOT NULL THEN 'PRI' WHEN uq.column_name IS NOT NULL THEN 'UNI' ELSE '' END AS "Key",
       c.column_default AS "Default",
       CASE WHEN c.column_default LIKE 'nextval%' THEN 'auto_increment' ELSE '' END AS "Extra",
       '' AS "Comment"
     FROM information_schema.columns c
     LEFT JOIN (SELECT kcu.column_name FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema
       WHERE tc.constraint_type='PRIMARY KEY' AND tc.table_schema=$1 AND tc.table_name=$2) pk ON pk.column_name=c.column_name
     LEFT JOIN (SELECT kcu.column_name FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema
       WHERE tc.constraint_type='UNIQUE' AND tc.table_schema=$1 AND tc.table_name=$2) uq ON uq.column_name=c.column_name
     WHERE c.table_schema=$1 AND c.table_name=$2 ORDER BY c.ordinal_position`,[schema,table]);
  res.json({columns:r.rows});
}));
app.post('/api/pg/rows',wrapPg(async(req,res,conn)=>{
  const{database:schema,table,page=1,pageSize=50,search=''}=req.body;
  if(!schema||!table) return res.status(400).json({error:'缺少參數'});
  const colR=await conn.query(`SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 ORDER BY ordinal_position`,[schema,table]);
  const colNames=colR.rows.map(r=>r.column_name);
  let whereClause='',params=[];
  if(search.trim()){
    const conds=colNames.map((_,i)=>`${pgId(colNames[i])}::text ILIKE $${i+1}`);
    whereClause=`WHERE (${conds.join(' OR ')})`;
    colNames.forEach(()=>params.push(`%${search}%`));
  }
  const countR=await conn.query(`SELECT COUNT(*) AS total FROM ${pgId(schema)}.${pgId(table)} ${whereClause}`,params);
  const total=parseInt(countR.rows[0].total);
  const offset=(parseInt(page)-1)*parseInt(pageSize);
  const dataR=await conn.query(`SELECT * FROM ${pgId(schema)}.${pgId(table)} ${whereClause} LIMIT $${params.length+1} OFFSET $${params.length+2}`,[...params,parseInt(pageSize),offset]);
  res.json({rows:dataR.rows,total,columns:colNames});
}));
app.post('/api/pg/insert',wrapPg(async(req,res,conn)=>{
  const{database:schema,table,data}=req.body; if(!schema||!table||!data) return res.status(400).json({error:'缺少參數'});
  const keys=Object.keys(data),vals=Object.values(data).map(v=>v===''?null:v);
  const cols=keys.map(k=>pgId(k)).join(', '),phs=keys.map((_,i)=>`$${i+1}`).join(', ');
  const r=await conn.query(`INSERT INTO ${pgId(schema)}.${pgId(table)} (${cols}) VALUES (${phs}) RETURNING *`,vals);
  res.json({ok:true,insertId:r.rows[0]});
}));
app.post('/api/pg/update',wrapPg(async(req,res,conn)=>{
  const{database:schema,table,data,where}=req.body; if(!schema||!table||!data||!where) return res.status(400).json({error:'缺少參數'});
  if(!Object.keys(where).length) return res.status(400).json({error:'WHERE 條件不可為空'});
  if(!Object.keys(data).length) return res.status(400).json({error:'更新欄位不可為空'});
  const dk=Object.keys(data),dv=Object.values(data).map(v=>v===''?null:v);
  const sets=dk.map((k,i)=>`${pgId(k)} = $${i+1}`).join(', ');
  const wk=Object.keys(where),wv=Object.values(where);
  const cond=wk.map((k,i)=>`${pgId(k)} = $${dk.length+i+1}`).join(' AND ');
  const r=await conn.query(`UPDATE ${pgId(schema)}.${pgId(table)} SET ${sets} WHERE ${cond}`,[...dv,...wv]);
  res.json({ok:true,affectedRows:r.rowCount});
}));
app.post('/api/pg/delete',wrapPg(async(req,res,conn)=>{
  const{database:schema,table,where}=req.body; if(!schema||!table||!where) return res.status(400).json({error:'缺少參數'});
  if(!Object.keys(where).length) return res.status(400).json({error:'WHERE 條件不可為空'});
  const keys=Object.keys(where),vals=Object.values(where);
  const cond=keys.map((k,i)=>`${pgId(k)} = $${i+1}`).join(' AND ');
  const r=await conn.query(`DELETE FROM ${pgId(schema)}.${pgId(table)} WHERE ${cond}`,vals);
  res.json({ok:true,affectedRows:r.rowCount});
}));
app.post('/api/pg/sql',wrapPg(async(req,res,conn)=>{
  const{database:schema,sql}=req.body; if(!sql) return res.status(400).json({error:'請輸入 SQL'});
  if(schema) await conn.query(`SET search_path TO ${pgId(schema)},public`);
  const norm=sql.trim().toUpperCase();
  const isRead=['SELECT','TABLE ','EXPLAIN','WITH ','SHOW '].some(k=>norm.startsWith(k));
  if(isRead){
    const r=await conn.query(sql);
    return res.json({type:'select',rows:r.rows,columns:r.fields?r.fields.map(f=>f.name):[]});
  } else {
    const r=await conn.query(sql);
    const affected=r.rowCount??0;
    const isDDL=/^\s*(CREATE|DROP|ALTER|RENAME|TRUNCATE)/i.test(sql.trim());
    return res.json({type:'write',affectedRows:affected,insertId:null,message:isDDL?'執行成功':`執行成功 — 影響 ${affected} 筆資料`});
  }
}));
app.post('/api/pg/database/create',wrapPg(async(req,res,conn)=>{
  const{database:schema}=req.body; if(!schema) return res.status(400).json({error:'請輸入 Schema 名稱'});
  await conn.query(`CREATE SCHEMA ${pgId(schema)}`); res.json({ok:true});
}));
app.post('/api/pg/database/drop',wrapPg(async(req,res,conn)=>{
  const{database:schema}=req.body; if(!schema) return res.status(400).json({error:'請指定 Schema'});
  await conn.query(`DROP SCHEMA ${pgId(schema)} CASCADE`); res.json({ok:true});
}));
app.post('/api/pg/table/create',wrapPg(async(req,res,conn)=>{
  const{database:schema,table,columns}=req.body;
  if(!schema||!table||!columns?.length) return res.status(400).json({error:'缺少必要參數'});
  const colDefs=columns.map(col=>buildPgColDef(col));
  const pks=columns.filter(c=>c.primaryKey).map(c=>pgId(c.name));
  if(pks.length) colDefs.push(`PRIMARY KEY (${pks.join(', ')})`);
  columns.filter(c=>c.unique&&!c.primaryKey).forEach(u=>colDefs.push(`UNIQUE (${pgId(u.name)})`));
  const sql=`CREATE TABLE ${pgId(schema)}.${pgId(table)} (\n  ${colDefs.join(',\n  ')}\n)`;
  await conn.query(sql); res.json({ok:true,sql});
}));
app.post('/api/pg/table/drop',wrapPg(async(req,res,conn)=>{
  const{database:schema,table}=req.body; if(!schema||!table) return res.status(400).json({error:'缺少參數'});
  await conn.query(`DROP TABLE ${pgId(schema)}.${pgId(table)}`); res.json({ok:true});
}));
app.post('/api/pg/table/truncate',wrapPg(async(req,res,conn)=>{
  const{database:schema,table}=req.body; if(!schema||!table) return res.status(400).json({error:'缺少參數'});
  await conn.query(`TRUNCATE TABLE ${pgId(schema)}.${pgId(table)}`); res.json({ok:true});
}));
app.post('/api/pg/table/rename',wrapPg(async(req,res,conn)=>{
  const{database:schema,table,newName}=req.body; if(!schema||!table||!newName) return res.status(400).json({error:'缺少參數'});
  await conn.query(`ALTER TABLE ${pgId(schema)}.${pgId(table)} RENAME TO ${pgId(newName)}`); res.json({ok:true});
}));
app.post('/api/pg/table/ddl',wrapPg(async(req,res,conn)=>{
  const{database:schema,table}=req.body; if(!schema||!table) return res.status(400).json({error:'缺少參數'});
  const colR=await conn.query(
    `SELECT column_name,udt_name,character_maximum_length,numeric_precision,numeric_scale,is_nullable,column_default
     FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 ORDER BY ordinal_position`,[schema,table]);
  const pkR=await conn.query(
    `SELECT kcu.column_name FROM information_schema.table_constraints tc
     JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema
     WHERE tc.constraint_type='PRIMARY KEY' AND tc.table_schema=$1 AND tc.table_name=$2`,[schema,table]);
  const pks=new Set(pkR.rows.map(r=>r.column_name));
  const lines=colR.rows.map(c=>{
    let t=c.udt_name.toUpperCase();
    if(c.character_maximum_length) t+=`(${c.character_maximum_length})`;
    else if(c.numeric_precision&&['NUMERIC','DECIMAL'].includes(t)) t+=`(${c.numeric_precision},${c.numeric_scale||0})`;
    let def=`  ${pgId(c.column_name)} ${t}`;
    if(c.is_nullable==='NO') def+=' NOT NULL';
    if(c.column_default) def+=` DEFAULT ${c.column_default}`;
    return def;
  });
  if(pks.size) lines.push(`  PRIMARY KEY (${[...pks].map(pgId).join(', ')})`);
  res.json({ddl:`CREATE TABLE ${pgId(schema)}.${pgId(table)} (\n${lines.join(',\n')}\n);`});
}));
app.post('/api/pg/column/add',wrapPg(async(req,res,conn)=>{
  const{database:schema,table,column}=req.body; if(!schema||!table||!column) return res.status(400).json({error:'缺少參數'});
  await conn.query(`ALTER TABLE ${pgId(schema)}.${pgId(table)} ADD COLUMN ${buildPgColDef(column)}`); res.json({ok:true});
}));
app.post('/api/pg/column/modify',wrapPg(async(req,res,conn)=>{
  const{database:schema,table,oldName,column}=req.body; if(!schema||!table||!oldName||!column) return res.status(400).json({error:'缺少參數'});
  const tbl=`${pgId(schema)}.${pgId(table)}`;
  if(oldName!==column.name) await conn.query(`ALTER TABLE ${tbl} RENAME COLUMN ${pgId(oldName)} TO ${pgId(column.name)}`);
  let pgType=column.type.toUpperCase();
  if(['VARCHAR','CHAR'].includes(pgType)&&column.length) pgType+=`(${parseInt(column.length)})`;
  await conn.query(`ALTER TABLE ${tbl} ALTER COLUMN ${pgId(column.name)} TYPE ${pgType} USING ${pgId(column.name)}::${pgType}`);
  if(column.notNull){ await conn.query(`ALTER TABLE ${tbl} ALTER COLUMN ${pgId(column.name)} SET NOT NULL`); }
  else { await conn.query(`ALTER TABLE ${tbl} ALTER COLUMN ${pgId(column.name)} DROP NOT NULL`); }
  if(column.default!==undefined&&column.default!==''){
    const d=String(column.default).toUpperCase();
    const defVal=d==='NULL'?'NULL':d==='CURRENT_TIMESTAMP'?'CURRENT_TIMESTAMP':`'${String(column.default).replace(/'/g,"''")}'`;
    await conn.query(`ALTER TABLE ${tbl} ALTER COLUMN ${pgId(column.name)} SET DEFAULT ${defVal}`);
  } else { await conn.query(`ALTER TABLE ${tbl} ALTER COLUMN ${pgId(column.name)} DROP DEFAULT`); }
  res.json({ok:true});
}));
app.post('/api/pg/column/drop',wrapPg(async(req,res,conn)=>{
  const{database:schema,table,column}=req.body; if(!schema||!table||!column) return res.status(400).json({error:'缺少參數'});
  await conn.query(`ALTER TABLE ${pgId(schema)}.${pgId(table)} DROP COLUMN ${pgId(column)}`); res.json({ok:true});
}));
app.post('/api/pg/indexes',wrapPg(async(req,res,conn)=>{
  const{database:schema,table}=req.body; if(!schema||!table) return res.status(400).json({error:'缺少參數'});
  const r=await conn.query(
    `SELECT i.relname AS "Key_name",a.attname AS "Column_name",
       ix.indisunique AS is_unique,ix.indisprimary AS is_primary,a.attnum AS "Seq_in_index",
       s.idx_scan AS "Cardinality"
     FROM pg_index ix
     JOIN pg_class i ON i.oid=ix.indexrelid JOIN pg_class t ON t.oid=ix.indrelid
     JOIN pg_namespace n ON n.oid=t.relnamespace
     JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=ANY(ix.indkey)
     LEFT JOIN pg_stat_user_indexes s ON s.indexrelname=i.relname AND s.schemaname=n.nspname
     WHERE n.nspname=$1 AND t.relname=$2 ORDER BY i.relname,a.attnum`,[schema,table]);
  const indexes=r.rows.map(row=>({Key_name:row.Key_name,Column_name:row.Column_name,Non_unique:row.is_unique?0:1,Seq_in_index:row.Seq_in_index,Cardinality:row.Cardinality,_isPrimary:row.is_primary}));
  res.json({indexes});
}));
app.post('/api/pg/index/add',wrapPg(async(req,res,conn)=>{
  const{database:schema,table,indexName,columns,unique=false}=req.body;
  if(!schema||!table||!indexName||!columns?.length) return res.status(400).json({error:'缺少參數'});
  const cols=columns.map(c=>pgId(c)).join(', ');
  await conn.query(`CREATE ${unique?'UNIQUE ':''}INDEX ${pgId(indexName)} ON ${pgId(schema)}.${pgId(table)} (${cols})`); res.json({ok:true});
}));
app.post('/api/pg/index/drop',wrapPg(async(req,res,conn)=>{
  const{database:schema,table,indexName}=req.body; if(!schema||!table||!indexName) return res.status(400).json({error:'缺少參數'});
  await conn.query(`DROP INDEX ${pgId(schema)}.${pgId(indexName)}`); res.json({ok:true});
}));

app.listen(PORT,()=>{
  console.log(`\n🐡 bobo 啟動！  http://localhost:${PORT}`);
  console.log('  MySQL  → /api/*\n  PgSQL  → /api/pg/*\n');
});
