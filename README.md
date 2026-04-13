# 🐡 bobo — MySQL Web 管理介面

輕量、快速的 MySQL 資料庫管理工具，透過瀏覽器進行資料的新增、刪除、修改與查詢。

---

## 快速開始

### 1. 安裝依賴

```bash
npm install
```

### 2. 啟動伺服器

```bash
node server.js
```

或使用 nodemon 開發模式（自動重啟）：

```bash
npm run dev
```

### 3. 打開瀏覽器

```
http://localhost:3000
```

---

## 目錄結構

```
bobo/
├── server.js          # Express 後端 API
├── package.json
├── README.md
└── public/
    └── index.html     # 前端 Web UI
```

---

## API 說明

所有 API 均為 `POST`，請求 body 需包含 `config` 連線資訊：

```json
{
  "config": {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "yourpassword",
    "database": "yourdb"   // 選填
  }
}
```

| 端點             | 功能             | 額外參數                                          |
|-----------------|-----------------|--------------------------------------------------|
| `/api/connect`  | 測試連線         | —                                                |
| `/api/databases`| 列出所有資料庫   | —                                                |
| `/api/tables`   | 列出資料表       | `database`                                       |
| `/api/columns`  | 取得欄位結構     | `database`, `table`                              |
| `/api/rows`     | 查詢資料（分頁） | `database`, `table`, `page`, `pageSize`, `search`|
| `/api/insert`   | 新增一筆資料     | `database`, `table`, `data`                      |
| `/api/update`   | 更新一筆資料     | `database`, `table`, `data`, `where`             |
| `/api/delete`   | 刪除一筆資料     | `database`, `table`, `where`                     |
| `/api/sql`      | 執行 SQL         | `database`, `sql`（僅 SELECT/SHOW/DESCRIBE）     |

---

## 安全性說明

- SQL 識別符（資料庫名、資料表名、欄位名）均經過白名單過濾
- 所有資料值使用 Prepared Statement，防止 SQL Injection
- `/api/sql` 端點僅允許 `SELECT`、`SHOW`、`DESCRIBE`、`EXPLAIN`，禁止寫入操作
- 每次請求獨立建立連線，不共用 connection pool

---

## 修改 Port

```bash
PORT=8080 node server.js
```

或直接修改 `server.js` 第 10 行：

```js
const PORT = process.env.PORT || 3000;
```
