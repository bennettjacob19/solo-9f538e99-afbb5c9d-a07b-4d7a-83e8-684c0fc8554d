# 分区盘演练 API（FastAPI + SQLite）

模拟一个 **4 物理区 / 3 逻辑组** 的 append-only 分区盘，支持按块号读写定长数据、
查看物理槽位占用，并内置满区搬迁（compaction）与崩溃恢复。交互式文档：
服务启动后访问 `http://localhost:8000/docs`（Swagger UI）。

## 1. 物理布局与规则

| 角色 | 数量 | 说明 |
| --- | --- | --- |
| 物理区 region | 4 | 大小相同，每区 `SLOTS_PER_REGION` 个定长槽位 |
| 逻辑组 group | 3 | 组号 `0..2`，每组固定块号 `0..SLOTS_PER_REGION-1`，各占一个 active 区 |
| 搬迁备用区 spare | 1 | 初始为空，满区搬迁时作为目标区 |

核心规则（与题目一一对应）：

1. **尾部追加 + 写入版本**：写入只向本组 active 区的下一个空槽位 INSERT，
   物理槽位永不原地更新；每条记录带全局单调递增的 `version`，**每次写入都换
   版本，即使正文与上一版完全相同**。读取取 `(组, 块)` 序列号最大的记录，
   覆盖写 = 追加一条新版本。版本计数器持久化在 SQLite，重启后继续递增。
2. **整区重置才释放槽位**：搬迁收尾时对源区 `DELETE` 全部记录并翻转为 spare，
   槽位计数归零；此外没有任何回收路径（`/admin` 仅供演练）。
3. **满区覆盖也能写入**：active 区满后再写（即使区内全是有效块），系统把 spare
   区切成搬迁目标，**触发写入的新值第一个落盘**（拿到全新版本），再逐块拷贝源区
   有效块的最新版本（**搬迁沿用原版本，不换号**），最后整区重置源区。搬迁完成后
   新区 active、源区 spare，始终保持一个空区。
4. **搬迁不阻塞读、不阻塞其他组**：搬迁分多步执行，每步一个独立事务并提交，
   步间释放写锁。已落盘的块立即可读；其他组在自己的槽位未满时照常写入。
   本组在搬迁期间写入返回 `503 + Retry-After`，客户端重试即可。
5. **块丢弃（逻辑墓碑）**：`POST /groups/{gid}/discards` 单组批量丢弃，请求体为
   互不重复的块号 + 各自当前版本。丢弃只在 `discards` 表登记墓碑：
   - **整批原子**：只要有一块缺失（从未写入 / 已丢弃且无新写入）、版本不符，或本组
     存在“已登记但尚未生效的覆盖”（搬迁 pending 首步未落盘），整批返回 `409`，
     所有块维持原状；
   - **不占槽、不释放槽**：旧槽位等整区重置才回收；
   - 丢弃成功后原读口返回 `404`，**重启续跑后也一样**，直到该块出现新写入
     （新写在同一事务删除墓碑并分配新版本）；
   - 搬迁步间仍可丢弃符合条件的块；后续搬迁步骤**跳过已丢弃的块**，不拷进新区；
   - 触发搬迁的覆盖已生效时，即使回包前该块被丢弃，写接口的 `write` 字段仍给出
     那次写入真实的落盘结果（物理读取，不受墓碑影响）。
6. **崩溃安全**：
   - 每一步搬迁都已 `COMMIT`，停机不丢已确认写入，重启后仍可读到新值；
   - 未完成的搬迁状态持久化在 `migration` 表，重启后可 `POST /migrations/resume`
     幂等续跑（重复执行已完成的步骤不会产生重复槽位）；
   - 若恰好停在 finalize 事务中途（源记录已删、状态未翻转），启动时 `recover()`
     自动补完。

### 区状态机

```
unused ──首次写入──▶ active ──启动搬迁──▶ migrating_src（冻结只读）
                                   ▲              │ 拷贝完成
spare ◀────整区重置（释放全部槽位）──────────────┘
spare ──启动搬迁──▶ migrating（目标区，pending 先落盘）──完成──▶ active
```

## 2. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/config` | 布局参数（区数、组数、每区槽位、定长块大小） |
| GET | `/health` | 健康检查（Docker healthcheck 使用） |
| GET | `/groups/{gid}/blocks/{bid}` | 按组号+块号读取最新值（base64 + ASCII + 写入版本）；已丢弃返回 404 |
| PUT | `/groups/{gid}/blocks/{bid}` | 写入定长数据（每次写入版本递增）；满区自动触发整区搬迁 |
| POST | `/groups/{gid}/discards` | 单组批量丢弃：块号+版本，整批原子；冲突整批 409 |
| GET | `/regions` | 各物理区状态与槽位占用（used/free/有效块数，已丢弃不计有效块） |
| GET | `/regions/{rid}` | 某物理区每个槽位的实际内容 |
| GET | `/migrations` | 当前搬迁进度（无搬迁时 `in_progress=false`） |
| POST | `/migrations/resume` | 重启后续跑中断的搬迁 |
| POST | `/admin/regions/{rid}/reset` | 【演练】整区重置回收为 spare |
| POST | `/admin/reset-all` | 【演练】清空全部数据 |

写入请求体（二选一）：

```json
{ "data": "abcdefgh" }
```
```json
{ "data_base64": "AAECAwQFBgcICQ==" }
```

原始字符串按 UTF-8 编码，字节数必须恰好等于 `BLOCK_SIZE`；二进制内容用 base64。

批量丢弃请求体（`block_id` 必须互不重复，`version` 取自读口返回的 `version`）：

```json
{
  "blocks": [
    { "block_id": 0, "version": 4 },
    { "block_id": 2, "version": 6 }
  ]
}
```

### 响应约定

- `200` 写入成功（`status=written` 普通追加，`status=migrated` 触发了搬迁）；
  读 / 写响应均含单调递增的 `version` 字段
- `200` 批量丢弃成功（`status=discarded`，回显每个块的块号与版本）
- `400` 数据长度不符 / base64 非法 / 两个字段同时出现 / 丢弃清单块号重复
- `404` 逻辑块尚无数据，或已被丢弃且之后没有新写入
- `422` 组号、块号、区号越界，或丢弃清单为空
- `409` 存储冲突：已有搬迁进行中，或丢弃整批校验失败（块缺失 / 版本不符 /
  存在已登记未生效的覆盖）；事务已回滚，所有块维持原状
- `503` 本组搬迁中暂无 active 区，或 spare 被占用；带 `Retry-After: 1`

## 3. docker compose 启动

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

数据落在命名卷 `partition-data`（容器内 `/data/partition.db`），容器重建后数据保留。
环境变量（见 `docker-compose.yml`）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SLOTS_PER_REGION` | 8 | 每个物理区槽位数（= 每组块数） |
| `BLOCK_SIZE` | 16 | 每条块数据的固定字节数 |
| `DB_PATH` | /data/partition.db | SQLite 文件路径 |
| `MIGRATION_STEP_DELAY` | 0.05 | 搬迁步间延迟秒数，便于在 /docs 观察半搬迁；设 0 全速 |
| `ENABLE_ADMIN` | 1 | 置 0 关闭管理接口 |

不用 Docker 时：

```bash
pip install -r requirements.txt
uvicorn --factory app.main:create_app --host 0.0.0.0 --port 8000
```

## 4. 快速演练（/docs 或 curl）

```bash
# 1. 写满组0（默认每区 8 槽，这里用 SLOTS_PER_REGION=3 演示）
for i in 0 1 2; do
  curl -X PUT localhost:8000/groups/0/blocks/$i \
       -H 'Content-Type: application/json' -d '{"data":"aaaaaaaa"}'
done

# 2. 查看槽位占用：区1 used=3 free=0
curl localhost:8000/regions

# 3. 覆盖满区的块 0 -> 自动搬迁，返回 status=migrated
curl -X PUT localhost:8000/groups/0/blocks/0 \
     -H 'Content-Type: application/json' -d '{"data":"zzzzzzzz"}'

# 4. 搬迁后：新区 active 且只含有效块，旧区整区重置为 spare（used=0）
curl localhost:8000/regions
curl localhost:8000/groups/0/blocks/0     # 读到 zzzzzzzz，响应带 version

# 5. 按读口拿到的版本批量丢弃（块号互不重复）；成功后读口 404，重启后依旧
V=$(curl -s localhost:8000/groups/0/blocks/0 | python3 -c 'import json,sys;print(json.load(sys.stdin)["version"])')
curl -s -X POST localhost:8000/groups/0/discards \
     -H 'Content-Type: application/json' \
     -d "{\"blocks\":[{\"block_id\":0,\"version\":$V}]}"
curl -i localhost:8000/groups/0/blocks/0  # HTTP/1.1 404
```

## 5. 崩溃恢复演练

```bash
# 制造半搬迁（只跑第一步就退出）
python - <<'PY'
from app.store import PartitionStore
s = PartitionStore("/data/partition.db", 3, 8)
for b in range(3): s.try_write(0, b, b"a"*8)
s.start_migration(0, 0, b"z"*8)
print(s.migration_step())   # pending 新值已落盘并 COMMIT
s.close()                   # 模拟停机
PY

# 重启服务：新值仍可读，搬迁显示 in_progress=true
curl localhost:8000/groups/0/blocks/0
curl localhost:8000/migrations

# 续跑（也可直接重试原覆盖写入，接口会引导重试）
curl -X POST localhost:8000/migrations/resume
```

## 6. 存储设计要点

- **WAL + `synchronous=FULL` + `BEGIN IMMEDIATE`**：每步搬迁独立事务，提交即持久化。
- `records` 表只增不改，新增 `version` 列记录写入版本；`(group_id, block_id, seq)`
  索引保证读路径取最新版本。版本号由 `config.next_version` 全局分配，搬迁拷贝
  直接沿用源记录的版本号。
- `discards` 表 `(group_id, block_id)` 主键保存墓碑版本；读路径比对墓碑版本与
  最新记录版本判断是否 404，新写入在同一事务删除墓碑。丢弃不写 `records`，
  因此不占槽位，旧槽位仍由整区重置回收。
- 批量丢弃在单个 `BEGIN IMMEDIATE` 事务内逐块校验（pending 覆盖 / 缺失 / 版本），
  任一失败即回滚，天然整批原子。
- `migration` 单行表记录源/目标区、触发写入（pending）及拷贝进度；
  续跑时用“目标区已有的块号集合”判断进度，天然幂等；拷贝查询用 `NOT EXISTS`
  排除当前已丢弃的块。
- 应用层单把 `asyncio.Lock` 串行化写操作（含丢弃）；读不加锁，搬迁步间让出事件循环。
- 旧版 SQLite 卷（无 `version` 列 / 无 `discards` 表）启动时自动平滑升级：
  按 `seq` 回填版本号、补建墓碑表、初始化版本计数器。

## 7. 测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

覆盖：append-only 不原地更新、定长校验、写入版本单调递增（同正文也换号）、
版本计数器重启续号、满区搬迁（pending 新版本 / 拷贝沿用原版本 / 含历史版本）、
每一步停机后的重启恢复、finalize 中途崩溃自愈、停机后重试覆盖、
搬迁中其他组可写与本组 503、spare 占用约束、批量丢弃的 404/409/400/422 全路径、
整批原子性、墓碑重启持久化与新写恢复、pending 窗口拒绝丢弃、步间丢弃使搬迁跳过、
丢弃不占槽且整区重置才回收、搬迁回包在被丢弃后仍含落盘结果、
旧库 schema 平滑升级、管理接口与全部 HTTP 校验。
