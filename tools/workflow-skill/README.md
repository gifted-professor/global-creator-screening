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
rsync -a tools/workflow-skill/ ~/.codex/skills/workflow/
```

说明：

- repo 内这份镜像是可追踪来源
- 本机 `~/.codex/skills/workflow/` 是运行时安装态
- 两者内容应保持一致；如果 skill 有升级，优先更新 repo 镜像，再同步到本机
