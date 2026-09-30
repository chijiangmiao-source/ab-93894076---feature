# 深空地面站 · 32 位帧计数滑动窗口接收器

在重启、乱序投递与 32 位计数器回绕之后，保证同一链路上的旧帧绝不会被再次接受。
提供浏览器页面、HTTP 接口、健康检查，以及以退出码报告结果的 Compose `verify` 验收服务。

## 核心算法

线路上的计数是 32 位无符号整数（`0 .. 2³²−1`），到达时无法直接知道它经历了第几次回绕（纪元 / epoch）。
内部以 **64 位扩展序号** 维护状态，收到帧时在当前最高序号所在纪元的相邻三个候选中选择**距当前最高值最近**者：

```
candidate(k) = counter + k·2³²,   k ∈ (epoch−1, epoch, epoch+1)
```

- **距离并列**：两个候选恰好相距 `2³¹`，无法定位纪元 → `rejected`（拒绝，状态不变）；值班员也可以把这样的帧**登记为待锚定观测**（见下），等待后续帧打破半距歧义
- **首次帧**：直接以 counter 初始化窗口（epoch 0），位图位置 0
- **更新的序号**：位图按差值左移，位置 0 置位（大跨度跳跃会使旧记录自然滑出）
- **窗口内更旧的序号**：仅置位，最高值不变
- **位图已置位** → `duplicate`（重复）
- **偏移 ≥ 64**（落在 `highest−63 .. highest` 窗口之外）→ `expired`（过期）

位图与最高扩展序号持久化在 SQLite 中；重启后从同一行状态继续推导，回绕前后的乱序帧每帧只接受一次。

## 稳定回执标识

- 相同 `receipt_id` + 相同 `counter` + 完全相同 `payload` 的重传：**返回首次裁决**（accepted/duplicate/expired/rejected 原样回放）
- `receipt_id` 全局唯一；复用标识但改变**链路、计数或载荷** → HTTP `409` 拒绝，且不改动窗口
- 每次到达都在**同一个 SQLite `BEGIN IMMEDIATE` 事务**内读取并写入窗口状态与回执记录，配合写锁使并发到达串行化，窗口与回执不可能出现不一致

## 待锚定观测（半距回绕帧）

当一帧的计数恰与当前最高扩展序号相差半个回绕周期（`2³¹`）时，两个纪元候选距离相等、纪元无法定位。值班员不必直接拒绝它，可在详情页（或新接口）把它**登记为待锚定观测**：

- 登记携带**独立观测标识** `observation_id`，以及原帧的**回执标识** `receipt_id`、计数与载荷；
- 登记**不移动窗口、不写回执账本、不产生普通回执裁决**——此时窗口与账本与登记前完全一致；
- **相同观测标识重传**：关联数据（链路/计数/回执/载荷）完全一致 → 返回该观测当前状态（待定 / 已结算 + 最终扩展序号）；任一关联数据改换 → HTTP `409`；
- **回执占用冲突**：原帧回执标识在观测待定期被直接投到普通收帧入口（即使数据完全相同）→ HTTP `409`；已被普通账本或其他观测占用的回执标识也不能被新观测保留；
- **自动结算**：后续任一普通帧一旦推进最高序号，服务在**同一 SQLite 事务**内按 `observation_id` 稳定顺序反复重判所有待定观测：半距并列随最高序号前移而被打破，待定帧被放入更高纪元候选，可确定者**仅结算一次**——写入回执账本（沿用原帧回执标识）、更新窗口、观测翻为已结算并记录最终扩展序号；若结算本身又推动窗口前进，则继续循环直到收敛；仍保持等距的观测继续待定；
- **收敛一致性**：并发锚帧、进程重启恢复、重复触发都只能得到同一最终位置；每个观测至多产生一条账本记录。

## 运行（Docker Compose）

```bash
# 默认宿主端口 8080
docker compose up --build

# 可配置宿主端口
HOST_PORT=9090 docker compose up --build
```

- 页面：http://localhost:8080/
- 健康检查：http://localhost:8080/healthz
- 数据持久化在命名卷 `receiver-data`（容器内 `/data/receiver.db`）

## 验收（verify 服务）

`verify` 服务等待 `web` 健康后执行：构建检查（compileall）→ 全部代码测试（回绕/过期/重复/并列/回执/重启/并发/**半距观测保留与自动结算/回执占用冲突/普通接收回归**）→ 对运行中的服务做 HTTP 冒烟，然后退出：

```bash
docker compose up --build --abort-on-container-exit verify
# 或
docker compose run --rm verify
echo $?   # 0 = 验收通过，非 0 = 失败
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/healthz` | `200 {"status":"ok"}` |
| `GET` | `/` | 单页操作界面 |
| `POST` | `/api/links` | `{"name":"..."}` 创建链路 |
| `GET` | `/api/links` | 列出链路及窗口状态 |
| `GET` | `/api/links/{id}` | 当前最高扩展序号、位图、最近 64 个已接受位置、待定/已结算观测 |
| `POST` | `/api/links/{id}/frames` | 提交到达帧 |
| `POST` | `/api/links/{id}/observations` | 把半距回绕帧登记为待锚定观测 |

提交帧请求体：

```json
{ "counter": 4294967295, "receipt_id": "tx-0001", "payload": {"sensor": "temp"} }
```

登记观测请求体（`counter` 须恰距当前最高序号 `2³¹`，否则 `400`）：

```json
{ "counter": 2147483648, "observation_id": "obs-0001",
  "receipt_id": "tx-0042", "payload": {"sensor": "temp"} }
```

裁决响应：

```json
{
  "status": "accepted",          // accepted | duplicate | expired | rejected
  "retransmit": false,           // true = 命中回执记录、回放首次裁决
  "counter": 4294967295,
  "extended": 4294967295,        // 解析出的 64 位扩展序号
  "highest": 4294967295,         // 当前最高扩展序号（含同事务结算后的收敛值）
  "recent": [4294967295],        // 最近 64 个已接受位置（新→旧）
  "settled": [                   // 本次事务内一并结算的待锚定观测（可空）
    {"observation_id": "obs-0001", "receipt_id": "tx-0042",
     "counter": 2147483648, "status": "settled",
     "verdict": "accepted", "extended": 2147483648}
  ]
}
```

## 本地开发（无需 Docker，纯标准库）

```bash
python3 -m unittest discover -s tests -v          # 单元 + 持久化 + HTTP 测试
DB_PATH=/tmp/rx.db PORT=8080 python3 -m app.server
python3 scripts/smoke.py http://127.0.0.1:8080    # 端到端冒烟
```

## 目录结构

```
app/window.py        # 纯函数：纪元候选、距离并列、滑动窗口位图裁决
app/db.py            # SQLite：窗口状态 + 回执记录 + 待锚定观测，单事务一致推导
app/server.py        # 标准库 HTTP 服务与 JSON API
app/static/index.html# 单页界面（含观测登记与待定/已结算列表）
tests/               # 算法 / 持久化 / 回执 / 重启 / 并发 / 观测 / HTTP 测试
scripts/smoke.py     # 针对运行服务的端到端冒烟（含半距观测四类验收）
scripts/verify.sh    # Compose verify 入口（退出码即验收结论）
```
