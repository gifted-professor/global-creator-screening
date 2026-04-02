# Workflow Skill Mirror

这里保存的是 `$workflow` 全局 Codex skill 的 repo-tracked 镜像版本。

目的只有两个：

- 让这套 skill 能跟随 integration 分支一起被版本化和评审
- 让换机器或重装环境时，有一份明确的安装来源

当前实际安装态仍然默认放在：

`~/.codex/skills/workflow/`

如果要从仓库同步到本机安装目录，可执行：

```bash
mkdir -p ~/.codex/skills/workflow
rsync -a --delete tools/workflow-skill/ ~/.codex/skills/workflow/
```

说明：

- repo 内这份镜像是可追踪来源
- 本机 `~/.codex/skills/workflow/` 是运行时安装态
- 两者内容应保持一致；如果 skill 有升级，优先更新 repo 镜像，再同步到本机

## Cross-Machine Install

在另一台机器上，推荐按下面顺序安装：

```bash
git fetch origin
git checkout codex/integration-main-shared-mailbox
mkdir -p ~/.codex/skills/workflow
rsync -a --delete tools/workflow-skill/ ~/.codex/skills/workflow/
```

## Validation

同步后，推荐做两步轻量校验：

```bash
python3 ~/.codex/skills/qiuzhi-skill-creator/scripts/quick_validate.py ~/.codex/skills/workflow
python3 ~/.codex/skills/workflow/scripts/test_workflow_skill.py
```

如果两步都通过，说明：

- skill 结构是合法的
- 本机安装态与 repo 镜像可正常工作

## Refresh Rule

Codex session 的 skill 清单通常在会话启动时加载。

所以如果你刚完成安装或更新：

- 新开一个 Codex 会话，或让当前环境重新加载 skills
- 再使用 `$workflow`

后续更新也保持同一条路径：

```bash
git pull --ff-only origin codex/integration-main-shared-mailbox
rsync -a --delete tools/workflow-skill/ ~/.codex/skills/workflow/
python3 ~/.codex/skills/workflow/scripts/test_workflow_skill.py
```
