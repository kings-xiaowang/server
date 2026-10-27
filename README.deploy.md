---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: fea8a7787a0816123c9ca50cee296149_51e9c24ad21311f18068525400de85a5
    ReservedCode1: B7gMgyaJPyhpqta8rzd+vefgVI3BvJ5mO+gmLQpjJ4iY0lur4qAJcwt42yP/GRBxMZR25f9Xhx/EAM7EUxZu+ZC1Wea5pEZAO+TNqgLYFSx0ki+9t1BuviJ6IrcYTcoDh3mqf6ug2wTNqesy9eODe8sYuEJJXEoTDOBioi31m0wlXhACf8/UzuGrpUU=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: fea8a7787a0816123c9ca50cee296149_51e9c24ad21311f18068525400de85a5
    ReservedCode2: B7gMgyaJPyhpqta8rzd+vefgVI3BvJ5mO+gmLQpjJ4iY0lur4qAJcwt42yP/GRBxMZR25f9Xhx/EAM7EUxZu+ZC1Wea5pEZAO+TNqgLYFSx0ki+9t1BuviJ6IrcYTcoDh3mqf6ug2wTNqesy9eODe8sYuEJJXEoTDOBioi31m0wlXhACf8/UzuGrpUU=
---

# Furry Talking 后端公网部署指南（路线 B）

后端已完成 IM 第一阶段改造：账号云化（JWT）、好友、单聊/群聊统一模型、消息可靠性（msg_id 幂等去重 + seq 增量拉取）、双模式存储。

- 本地跑：`python main.py`（默认 `0.0.0.0:8000`，SQLite）
- 云端跑：设置环境变量后自动使用 PostgreSQL

## 环境变量

| 变量 | 必填 | 说明 |
|------|------|------|
| `DATABASE_URL` | 云端必填 | PostgreSQL 连接串，例如 `postgresql://user:pass@host:5432/dbname`。不设则用本地 SQLite |
| `JWT_SECRET` | 建议 | 令牌签名密钥，云端请设置随机长字符串，不要用默认值 |
| `PORT` | 云端自动 | 平台注入的监听端口，默认 8000 |

## 平台一：Zeabur（推荐，国内节点访问快）

1. 把 `server` 目录推到一个 GitHub 仓库（仓库根放 `main.py`、`requirements.txt`、`Dockerfile`）。
2. 在 [zeabur.com](https://zeabur.com) 新建项目 → 添加服务 → 从 GitHub 导入该仓库。
3. 再添加一个「PostgreSQL」服务。
4. 在 Web 服务的环境变量里把 PostgreSQL 的 `DATABASE_URL` 填进去，并设置 `JWT_SECRET`。
5. 部署完成后得到公网域名，形如 `https://xxx.zeabur.app`。

## 平台二：Render（免费 750 小时/月 + 免费 PostgreSQL）

1. 推 GitHub 仓库同上。
2. 在 [render.com](https://render.com) → New → Blueprint，粘贴 `render.yaml`，或手动新建 Web Service + PostgreSQL。
3. Web Service 构建命令 `pip install -r requirements.txt`，启动命令 `uvicorn main:app --host 0.0.0.0 --port $PORT`，绑定免费 PostgreSQL 连接串到 `DATABASE_URL`。
4. 部署完成后得到公网域名，形如 `https://xxx.onrender.com`。

## 部署后改 App 地址

改 `app/src/main/java/com/furryspace/app/net/ServerConfig.kt`：

```
BASE_URL = "https://你的公网域名"
WS_URL   = "wss://你的公网域名/ws"
```

改完重新编译 `assembleDebug` 出 APK，安装到手机即可公网实时通信。

## 说明

- 云端必须用 PostgreSQL：免费平台磁盘重启会清空，SQLite 会丢数据。
- 本地 SQLite 已开 WAL + 索引，自测两账号注册/好友/单聊/去重/增量拉取全部通过。
*（内容由AI生成，仅供参考）*
