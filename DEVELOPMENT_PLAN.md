# EasyRegister Development Plan

## 1. 项目定位

`EasyRegister` 是从 `RegisterService` 复制出来的独立优化开发仓库。

目标是：

- 在不干扰当前线上可用 `register-*` 容器的前提下，持续对编排层和运行时做结构性维护。
- 把现有“已经可跑”的系统，逐步提升为“更安全、更可测试、更可维护、更容易回滚”的系统。
- 所有优化必须以“可回退、可测试、可审计”为前提推进。

## 2. 当前状态

当前新仓已经包含以下已验证改动源码：

- 安全默认值收紧
  - 默认不再注入硬编码邮箱 API key
  - dashboard 默认仅监听本机
  - 缺少安全 control token 时不启动 dashboard
- cleanup 锁具备 stale lock recovery 能力
- 已建立新仓首个导入提交，并创建恢复基线 tag `easyregister-import-baseline`
- 已新增隔离测试 compose：`compose/docker-compose.test.yaml`
  - 默认容器名前缀为 `easyregister-test-*`
  - 默认 dashboard 宿主机端口为 `29790`
  - 默认测试输出目录位于仓库内 `tmp/easyregister-test-*`
- 已完成第一轮结构化错误码与 retry profile 收口
  - flow JSON 已使用 `retryProfile`
  - `dst_flow.py` 使用结构化 `code` 判定 step/task retry
- 已完成第一轮 typed config 收口与 runtime preflight
  - 核心运行时配置已集中到 `others/config.py`
  - supervisor 启动前会执行 `others/preflight.py`
- 已完成 `infinite_runner.py` / `artifact_pool_flow.py` / `runtime.py` 的主体拆分
  - `infinite_runner.py` 当前仅保留薄入口
  - supervisor、team auth、artifact pool、runtime proxy/mailbox 已拆到 `others/` 子模块
- 已消除当前源码中的 `__package__` / `sys.path` 兼容导入分支
- 已补充工程化入口文件
  - `pyproject.toml`
  - `requirements-dev.txt`
- 已建立当前最小测试分层
  - 单元测试、轻量流程测试、compose smoke 测试

注意：

- 上述运行时代码主体仍然是从旧仓复制过来的状态，后续结构优化应继续按“小步、可测、可回退”的规则推进。
- 恢复基线 tag 只用于回退和比对，后续结构性改动不应复用或覆盖该 tag。

## 3. 总体优化目标

原始优化主线分为五条：

1. 继续收紧运行时安全边界，避免隐式危险默认值。
2. 拆分超大模块，优先降低 `infinite_runner.py` 的复杂度和维护风险。
3. 消除重复逻辑，收敛公共工具函数和状态文件操作逻辑。
4. 把错误分类从“字符串匹配”逐步演进为“结构化错误码 + 结构化上下文”。
5. 建立稳定的测试与回滚机制，保证每轮维护都可以独立验证和独立恢复。

## 4. 结构维护路线

建议按以下顺序推进：

### Phase 0. 新仓初始化基线

- 创建新仓首个导入提交。
- 为新仓建立新的恢复基线 tag。
- 这个基线必须对应“可运行、可测试、可继续开发”的状态。

### Phase 1. 运行时与锁逻辑稳定化

当前状态：已完成主要目标。

已包含的方向：

- 安全默认值收紧
- stale cleanup lock recovery
- lock 元数据标准化
- 运行态配置校验前置化

后续可继续补强：

- lock 超时与观测日志统一
- 更细的 supervisor / worker 运行态观测

### Phase 2. `infinite_runner.py` 模块化拆分

当前状态：已完成主要目标。

优先拆出以下子域：

- cleanup / recovery 逻辑
- team auth state / seat reservation 逻辑
- artifact postprocess / pool drain 逻辑
- worker loop orchestration 逻辑
- env/config resolve 逻辑

拆分原则：

- 先提取纯函数和 helper
- 再下沉有状态逻辑
- 最后缩短 `_worker_loop` 和 `main`

当前结果：

- `infinite_runner.py` 已变为薄入口
- supervisor / team auth / mailbox / cleanup / artifact 路由逻辑已拆出到 `others/`

### Phase 3. `artifact_pool_flow.py` 收口

当前状态：已完成主要目标。

目标：

- 与 `infinite_runner.py` 去重
- 提炼 artifact schema / naming / validation 的公共能力
- 降低 pool claim / finalize / collect 路径的重复状态处理

### Phase 4. `dst_flow.py` 错误与重试体系整理

当前状态：已完成第一轮主目标。

目标：

- 降低 message contains 规则数量
- 引入更稳定的错误码映射边界
- 让 step retry / task retry 判断更可预测

### Phase 5. 工程化补强

当前状态：已完成第一轮主目标。

目标：

- 建立最小测试套件分层
- 明确依赖管理方式
- 建立隔离测试 compose
- 为关键运行路径补文档

## 5. 开发硬约束

以下约束是本仓后续开发必须遵守的规则：

### 5.1 Git 与回滚约束

- 每一轮结构性维护开始前，必须有一个明确可恢复的 Git 提交点。
- 每一轮结构性维护完成后，必须：
  - 先测试
  - 测试通过后再提交
- 不允许把多个大的结构改动混在一个未测试提交里。
- 每一轮提交都必须能作为独立回退点使用。

### 5.2 测试先行约束

- 只有测试通过，才允许继续下一轮开发。
- 新增或重构的公共逻辑，优先补最小单元测试。
- 如果改动影响编排行为、状态文件、锁行为、配置解析、pool 行为，必须补对应测试。
- 如果某一轮无法建立合理测试，则该轮改动不能继续扩散。

### 5.3 线上运行容器隔离约束

- 当前旧仓对应的三个生产容器：
  - `register-service`
  - `register-continue-service`
  - `register-team-service`
  必须保持运行，不允许因本仓开发而被中断。
- 不允许在现有生产容器上直接验证本仓新改动。
- 不允许复用现有生产容器的名字、端口、输出目录进行测试。

### 5.4 测试环境隔离约束

- 本仓测试必须使用新的镜像、新的容器名、新的端口、新的共享输出目录。
- 测试 compose 必须与生产 compose 分离。
- 测试环境允许访问同类外部依赖服务，但不能覆盖生产实例的 bind mount 或状态目录。
- 若需要运行新的 `register-*` 测试容器，必须使用独立前缀，例如：
  - `easyregister-test-main`
  - `easyregister-test-continue`
  - `easyregister-test-team`

### 5.5 变更范围控制约束

- 每轮改动只处理一个清晰主题。
- 优先做低耦合、可验证的小步重构。
- 没有测试护栏前，不做大规模行为改写。
- 如果结构拆分会改变运行时行为，必须先显式记录预期行为差异。

## 6. 每轮开发标准流程

每一轮维护必须按这个顺序执行：

1. 明确本轮目标和边界。
2. 确认当前 Git 状态可回退。
3. 实施本轮改动。
4. 为本轮改动补测试或更新测试。
5. 运行测试。
6. 仅在测试通过后提交代码。
7. 记录本轮结果和下一轮入口。

## 7. 提交与文档约定

- 提交信息应清楚表达本轮改动主题。
- 结构性改动优先使用下列语义前缀：
  - `hardening:`
  - `refactor:`
  - `test:`
  - `chore:`
- 如果本轮改动调整了开发流程、测试约束或运行约束，必须同步更新本文件。

## 8. 测试策略建议

建议将测试分为三层：

### 8.1 纯单元测试

覆盖：

- 配置解析
- 锁语义
- 状态文件读写
- 文件名生成
- 错误分类
- artifact validation

### 8.2 轻量流程测试

覆盖：

- `dst_flow` step enable / retry / alwaysRun 规则
- 小范围 orchestration step dispatch
- pool claim / finalize / restore 行为

### 8.3 隔离集成测试

覆盖：

- 新测试容器启动
- 新输出目录写入
- 基础 supervisor / worker 拉起
- dashboard 或状态文件输出验证

## 9. 可选后续项

原始计划主线当前已经完成。后续如果继续投入，优先级更像增量质量优化，而不是必做欠账：

1. 继续细化超大子模块，例如 `runner_team_auth_state.py`、`runner_artifacts.py`、`runtime_proxy.py`。
2. 继续统一日志/状态观测格式，补更多 supervisor / worker / cleanup 观测字段。
3. 视需要增加更重的隔离集成测试，而不是只停留在 compose config smoke。

## 10. 重要提醒

本仓开发的核心原则不是“尽快重写”，而是：

- 小步推进
- 每轮可测
- 每轮可提交
- 每轮可回退
- 不影响旧仓线上运行实例
