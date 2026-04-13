# ── Stage 1: 安裝依賴 ──────────────────────────────
FROM node:20-alpine AS deps

WORKDIR /app

# 只複製 package.json，利用 Docker layer cache
COPY package.json ./

RUN npm install --omit=dev

# ── Stage 2: 最終 image ────────────────────────────
FROM node:20-alpine

LABEL maintainer="bobo"
LABEL description="bobo — MySQL & PostgreSQL Web 管理介面"

# 建立非 root 使用者，提高安全性
RUN addgroup -S bobo && adduser -S bobo -G bobo

WORKDIR /app

# 從 deps stage 複製 node_modules
COPY --from=deps /app/node_modules ./node_modules

# 複製應用程式檔案
COPY server.js  ./
COPY package.json ./
COPY public/    ./public/

# 修改檔案擁有者
RUN chown -R bobo:bobo /app

USER bobo

EXPOSE 3000

ENV NODE_ENV=production \
    PORT=3000

CMD ["node", "server.js"]
