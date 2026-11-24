# 单支路送电服务（FastAPI）

机房复电时，多台服务器同时启动的峰值电流可能让一条供电支路过载。
本服务模拟**单支路合闸送电的值班受理与额度管理**：预设支路上限和每台服务器的
启动峰值 / 稳定电流，值班员申请合闸，余量够才受理，受理即按峰值占用额度。

## 业务规则与状态机

```
                 申请合闸(余量够)
   OFF ────────────────────────────► STARTING ──回执正常──► STARTING(待查实稳定)
    ▲                                  │
    │                                  └──回执超时/丢失──► UNKNOWN(结果未明)
    │                                                        │
    │              按合闸单查实 PDU: 已通电且 stable          │
    │                                  ┌─────────────────────┘
    │                                  ▼
    │                               RUNNING  (占用改为稳定电流)
    │                                  │
    │              断电 + 按单查实 PDU: powered=false         │
    │              且断电围栏 > 本单合闸代号                   │
    └──────────────────────────── RELEASED (释放额度，终态)
```

额度规则：

- `STARTING` / `UNKNOWN`：按该服务器**启动峰值**占用（回执丢了也保留峰值，宁紧勿松）；
- `RUNNING`：查实稳定后改按**稳定电流**占用，差额释放给支路；
- `RELEASED`：查实设备断电后占用归零；
- 支路剩余 = 上限 − 所有活动合闸单占用之和；
- 同一服务器存在活动单（STARTING/UNKNOWN/RUNNING）时重复申请，返回 **200 + 原单**，不重复占用；
- 查余量与开单在同一把 `asyncio.Lock` 临界区内，避免并发申请超额受理；
- 等 PDU（回执、状态查询）全部在锁外异步进行，`close` 接口立即返回，
  **不阻塞其他服务器合闸**。

### 迟到指令 / 迟到结果的防护（本轮修复重点）

值班员断电并释放额度后，**已经在途的两类消息**绝不能把插座重新通电、
也不能让旧单重新占额：

1. **迟到的合闸指令**：每次合闸/断电都带一个严格单调递增的指令代号
   （`generation`，以微秒时间戳为主，进程重启后仍递增）。
   PDU 对每个插座记录 `last_close_fence` / `last_open_fence`；
   断电代号**只升不降**，任何代号早于当前围栏的合闸都会被 PDU 拒绝
   （HTTP 409 `stale_close`），插座保持断电。
2. **迟到的稳定状态查询**：PDU 的 `/pdu/status` 在收到请求时先快照再按
   配置延迟返回，所以返回的可能是“断电前”的旧状态。服务端在结果返回后
   重新进锁复核，只有同时满足以下条件才改状态/占额：
   - 单据仍是该服务器唯一的活动单（没有被释放、也没有被新单取代）；
   - 单据在查询期间没有下发过断电（`open_generation is None`）；
   - PDU 报告的 `last_close_fence` 正是本单的合闸代号。

   任一不满足即返回 409、丢弃该结果，**不改变新旧单状态或支路占额**。

3. **释放条件（无法确认就保留占额）**：只有 PDU 明确报告 `powered == false`，
   且 `last_open_fence` **严格晚于**本单 `close_generation` 时，才释放额度。
   这证明本单的在途合闸已经被更晚的断电作废、不可能再让插座通电。
   围栏缺失或不晚于合闸代号 -> 409，峰值额度继续保留。

4. **迟到回执**：合闸回执在本单已断电释放后才返回时，只写入
   `history`（事件 `RECEIPT_LATE_IGNORED`），不改状态与占额。

旧单释放后，按原规则可以立即开新单；旧单的迟到查询结果不会影响新单。

## 目录结构

```
.
├── app/                    # 送电服务
│   ├── main.py             # FastAPI 路由 + create_app（可注入 PDU 客户端）
│   ├── config.py           # config/servers.json 加载与校验
│   ├── models.py           # 合闸单 / 状态枚举 / 指令代号
│   ├── store.py            # 内存台账、额度计算、围栏复核、状态流转、锁
│   ├── pdu_client.py       # httpx PDU 客户端（合闸/断电携带代号）
│   ├── requirements.txt
│   └── Dockerfile
├── pdu/                    # PDU 模拟器（独立 FastAPI 服务）
│   ├── main.py             # 指令围栏 + normal/lost-receipt/fail/flaky/delayed-close
│   ├── requirements.txt
│   └── Dockerfile
├── config/servers.json     # 支路上限 + 4 台服务器的峰值/稳定电流
├── tests/test_api.py       # 20 个流程 + 并发/迟到场景测试（FakePDU，无需 Docker）
├── scripts/demo.sh         # 端到端演示（容量、丢回执、在途不阻塞）
├── scripts/demo_late.sh    # 端到端演示（迟到合闸指令、迟到稳定查询）
├── scripts/verify_concurrency.py  # 并发/迟到场景自动验收（对真实进程，可重复跑）
└── docker-compose.yml
```

## 用 Docker Compose 启动

```bash
docker compose up --build
```

- API 服务：<http://localhost:8000>（Swagger 文档：<http://localhost:8000/docs>）
- PDU 模拟器：<http://localhost:9000>
- 支路配置通过卷挂载 `config/servers.json`，改完重启 `api` 即可。

## 本地直接运行（不用 Docker）

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

# 终端 1：PDU 模拟器
PDU_BOOT_SECONDS=5 uvicorn pdu.main:app --port 9000

# 终端 2：送电服务
PDU_BASE_URL=http://127.0.0.1:9000 \
APP_CONFIG=$PWD/config/servers.json \
uvicorn app.main:app --port 8000

# 终端 3：一键演示
bash scripts/demo.sh        # 基础流程
bash scripts/demo_late.sh   # 迟到指令 / 迟到查询
```

## 配置：`config/servers.json`

示例：支路上限 `100A`，4 台服务器（峰值 30/30/40/25A，稳定 12/12/15/10A）。

```json
{
  "branch": { "name": "A-01", "limit_amps": 100.0 },
  "receipt_timeout_seconds": 3.0,
  "servers": [
    { "id": "srv-1", "peak_amps": 30.0, "stable_amps": 12.0 }
  ]
}
```

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/servers/{id}/close` | 申请合闸；201 新受理，200 重复申请返回原单，409 余量不足 |
| POST | `/api/servers/{id}/open` | 指令 PDU 断开该服务器插座（携带更高断电代号，作废在途合闸） |
| POST | `/api/orders/{oid}/verify-stable` | 按本单向 PDU 查实已稳定 → 改按稳定电流计；迟到/过期结果 409 |
| POST | `/api/orders/{oid}/release` | 查实设备已断电且断电围栏晚于本单合闸 → 释放；无法确认 409 |
| GET | `/api/branch` | **各机状态 + 支路占用/余量总览** |
| GET | `/api/orders` / `/api/orders/{oid}` | 合闸单列表 / 详情（含 `close_generation` / `open_generation` / 状态流转历史） |
| GET | `/api/health` | 健康检查 |

## API 调用示例

### A. 基础流程（含“合闸成功却丢回执”）

```bash
API=http://localhost:8000
PDU=http://localhost:9000

# 1) 看初始支路
curl -s $API/api/branch | python3 -m json.tool

# 2) 申请合闸（受理即按 30A 峰值占用，立即返回合闸单号）
curl -s -X POST $API/api/servers/srv-1/close
# -> 201 {"order_id":"...","state":"STARTING","occupied_amps":30.0,
#         "close_generation":1795532681518797,...}

# 3) 余量不够时 -> 409
curl -s -X POST $API/api/servers/srv-4/close

# 4) 重复申请 -> 200 + 同一个 order_id，"deduplicated": true
curl -s -X POST $API/api/servers/srv-1/close

# 5) 让 PDU 对 srv-4 模拟“合闸成功但丢回执”，再申请合闸
curl -s -X POST "$PDU/pdu/admin/mode/srv-4?mode=lost-receipt"
OID=$(curl -s -X POST $API/api/servers/srv-4/close | python3 -c 'import sys,json;print(json.load(sys.stdin)["order_id"])')

#    超过 receipt_timeout_seconds(3s) 后查询：state=UNKNOWN，25A 峰值仍占用
sleep 4; curl -s $API/api/orders/$OID

# 6) 按本次合闸单查实稳定（PDU 合闸后 PDU_BOOT_SECONDS 秒才报 stable）
curl -s -X POST $API/api/orders/$OID/verify-stable
# -> state=RUNNING, occupied_amps=10.0（25A 峰值改为 10A 稳定电流）

# 7) 设备仍带电时不能释放
curl -s -X POST $API/api/orders/$OID/release        # 409

# 8) 断电 -> 查实 -> 释放
curl -s -X POST $API/api/servers/srv-4/open
curl -s -X POST $API/api/orders/$OID/release        # state=RELEASED, occupied_amps=0
```

### B. 合闸指令迟到：断电释放后不得重新通电

```bash
API=http://localhost:8000
PDU=http://localhost:9000

# 1) 让 PDU 把 srv-1 的合闸指令延迟 3 秒才真正执行
curl -s -X POST "$PDU/pdu/admin/mode/srv-1?mode=delayed-close"
curl -s -X POST "$PDU/pdu/admin/delays/srv-1?close_seconds=3"

# 2) 申请合闸：close 立即返回（不干等迟到的 PDU）
OID=$(curl -s -X POST $API/api/servers/srv-1/close | python3 -c 'import sys,json;print(json.load(sys.stdin)["order_id"])')
#    返回体含 close_generation=<本次合闸代号>

# 3) 合闸指令还在 PDU 侧排队时，值班员断电：open 立即生效
curl -s -X POST $API/api/servers/srv-1/open
# -> {"powered": false, "generation": <断电代号>, "last_open_fence": <断电代号>}

# 4) 查实释放：断电围栏 > 本单合闸代号，确认在途合闸已作废 -> 允许释放
curl -s -X POST $API/api/orders/$OID/release
# -> state=RELEASED, occupied_amps=0

# 5) 等迟到合闸真正到达 PDU：PDU 以 409 stale_close 拒绝，插座保持断电
sleep 4
curl -s $PDU/pdu/status/srv-1
# -> {"powered": false, "last_close_fence": null, "last_open_fence": <断电代号>}
curl -s $API/api/orders/$OID
# -> state=RELEASED；history 末尾为 RECEIPT_LATE_IGNORED

# 6) 按原规则可立即开新单
curl -s -X POST $API/api/servers/srv-1/close   # 201 新单号
```

### C. 稳定状态查询迟到：旧单已释放/已开新单，旧结果不得改动任何状态或占额

```bash
API=http://localhost:8000
PDU=http://localhost:9000

# 1) srv-2 正常合闸，等 PDU 稳定
curl -s -X POST "$PDU/pdu/admin/mode/srv-2?mode=normal"
OID=$(curl -s -X POST $API/api/servers/srv-2/close | python3 -c 'import sys,json;print(json.load(sys.stdin)["order_id"])')
sleep 6

# 2) 让 PDU 状态查询延迟 1.5s 返回；请求到达时先快照（此刻已稳定）
curl -s -X POST "$PDU/pdu/admin/delays/srv-2?status_seconds=1.5"

# 3) 发起 verify-stable，趁它在途立即断电并释放
curl -s -X POST $API/api/orders/$OID/verify-stable &
sleep 0.4
curl -s -X POST $API/api/servers/srv-2/open
curl -s -X POST $API/api/orders/$OID/release        # 200 RELEASED

# 4) 按原规则开新单
NEW=$(curl -s -X POST $API/api/servers/srv-2/close | python3 -c 'import sys,json;print(json.load(sys.stdin)["order_id"])')
curl -s $API/api/orders/$NEW      # STARTING / 30A

# 5) 迟到的稳定查询返回：409，旧单保持 RELEASED，新单保持 STARTING/30A
wait
# -> {"detail":{"error":"该单已下发断电指令，断电前发出的稳定查询结果已过期，占用维持峰值",...}}
curl -s $API/api/orders/$OID       # RELEASED / 0A
curl -s $API/api/orders/$NEW       # STARTING / 30A
curl -s $API/api/branch            # 占用只含新单 30A
```

## PDU 模拟器

| 接口 | 说明 |
|---|---|
| `POST /pdu/close/{sid}?generation=N` | 合闸并回回执；行为由模式决定；迟到/过期代号返回 409 `stale_close` |
| `POST /pdu/open/{sid}?generation=N` | 断开插座，断电围栏只升不降 |
| `GET  /pdu/status/{sid}` | `powered` / `stable` / 通电时长 / `last_close_fence` / `last_open_fence` |
| `GET  /pdu/outlets` | 所有插座状态 |
| `POST /pdu/admin/mode/{sid}?mode=...` | 设置模拟模式 |
| `POST /pdu/admin/delays/{sid}?status_seconds=&close_seconds=` | 人为延迟状态查询 / 合闸指令 |

模式：

- `normal`（默认）：合闸后延迟 `PDU_RECEIPT_DELAY` 秒回正常回执；
- `lost-receipt`：**实际合闸并通电，但请求永不响应**，由 API 端超时判定为回执丢失；
- `fail`：直接返回 500；
- `flaky`：每次随机上述三种行为；
- `delayed-close`：合闸请求先等待 `close_seconds` 秒再尝试生效，复现“迟到的合闸指令”。

延迟与围栏语义：

- `status_seconds > 0`：状态查询**收到请求时先快照**、再等待后返回快照，
  复现“断电前发出的查询现在才返回”；
- `close_seconds > 0`：合闸请求在生效前先等待，复现“迟到的合闸指令”；
- 合闸代号早于插座当前围栏（`max(last_close_fence, last_open_fence)`）时，
  PDU 返回 **409 `stale_close`，绝不给插座通电**。

环境变量：`PDU_BOOT_SECONDS=5`（合闸后多久算稳定）、`PDU_RECEIPT_DELAY=0.2`、
`PDU_DEFAULT_MODE=normal`。

## 测试与实测结果

单元/并发流程测试（FakePDU，覆盖容量、幂等、丢回执、稳定、释放、404，
以及迟到回执、迟到稳定查询、迟到合闸指令、围栏无法确认时保留占额等）：

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
# 20 passed
```

实测环境：Python 3.11 + FastAPI 0.165 + httpx 0.28 + uvicorn 0.54，
真实起两个 uvicorn 进程（PDU 与 API），按 `scripts/demo.sh`、
`scripts/demo_late.sh` 和 `scripts/verify_concurrency.py` 实跑，结果如下：

- **合闸指令迟到**：PDU 延迟 3s 执行合闸，`close` 接口 **0.014s 返回** 201；
  在指令排队期间 `open` 成功，`release` 返回 200（PDU 报告的断电围栏严格晚于
  本单合闸代号，证明在途合闸已作废）；
  3.5s 后 PDU 状态 `powered=false, last_close_fence=null`，旧单
  `RELEASED / 0A`，历史
  `ACCEPTED -> OPEN_SUBMITTED -> OPEN_CONFIRMED -> RELEASED -> RECEIPT_LATE_IGNORED`；
  随后按原规则开新单 201 成功，且新单以自己的围栏正常通电。
- **稳定查询迟到**：查询在途时断电并释放，再开新单；1.5s 后旧查询返回
  **409**（“断电前发出的稳定查询结果已过期，占用维持峰值”），
  旧单保持 `RELEASED / 0A`，新单保持 `STARTING / 30A`，支路占用只含新单 30A。
- **合闸回执在途时断电**：`open` 与 `release` 均立即成功，迟到回执只写审计，
  占额保持 0。
- **无法确认断电围栏**：PDU 缺 `last_open_fence` 或围栏不晚于合闸代号时，
  `release` 返回 409，峰值额度保留。
- **等 PDU 不阻塞**：PDU 状态查询卡住 1.5s 期间，另一台服务器申请合闸
  **0.005s 返回 201**。
- `scripts/verify_concurrency.py` 在真实双进程上连续跑两次均为
  **24/24 checks passed**（脚本自带清理，可重复执行）。
- 基础 `scripts/demo.sh` 全流程（占满 100A -> 409、查实后 39A、丢回执保留峰值、
  带电拒释放、断电后释放）在真实进程上实跑通过，末态占用 82A / 余量 18A。
- `GET /api/branch` 各机状态与余量计算始终一致；OpenAPI schema 正常生成全部接口。

## 设计取舍 / 还没完成的事项

- **内存台账、单实例**：重启进程合闸单丢失，也不支持多副本。生产化需要换
  SQLite/Postgres 并处理多实例互斥。指令代号用时间戳以便重启后仍递增，
  但 PDU 侧围栏目前也只在内存里。
- **没有真正的定时巡检任务**：UNKNOWN 单的后续状态依赖值班员调用
  `verify-stable` / `release` 查实；后台只负责等首轮回执。可加周期对账 PDU 的 worker。
- **PDU 围栏是模拟器语义**：方案依赖 PDU 支持“单调指令代号 + 过期合闸拒绝 +
  断电围栏上报”。真实 PDU（SNMP/Modbus/厂商 HTTP）未必提供，需要适配层在
  设备侧或网关侧实现同样的作废能力；否则只能退化为“断电后禁止立即释放”，
  或加一段固定的在途指令等待窗口。实现 `app/main.py` 的 `PDUClient` 三个方法即可替换客户端。
- **PDU 状态查询超时（3s）与人为延迟**：若 PDU 查询本身超过客户端超时，接口返回
  502，额度同样保留（保守）。未把“查询超时”与“结果过期”区分成不同错误码。
- **没有鉴权/审计落库**：目前只有合闸单内的状态流转历史，没有操作员身份与权限。
- **Docker Compose 未在本环境实际 `up`**：当前环境没有 docker / podman，
  镜像构建与 compose 启动未实跑；但已用与 Dockerfile 相同的依赖版本真实起
  uvicorn 双进程完成上述实测，仍建议在有 Docker 的机器上执行一次
  `docker compose up --build` 验收。
- 电流值按配置的固定峰值/稳定值记账，未采集 PDU 实时电流；PDU 的
  `stable` 目前用“通电时长 ≥ PDU_BOOT_SECONDS”近似。
