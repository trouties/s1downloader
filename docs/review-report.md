# Code Review Report

## 总结

`s1downloader` 当前代码结构清晰、测试覆盖较完整，核心下载与 EOF 功能可用。
本轮已完成中等重构，重点提升了包发布一致性、CLI 体验、错误处理边界和工程化基线。

## 发现的问题

1. 包结构与发布入口不一致
- 代码包名原为 `app`，与项目名 `s1downloader` 不一致，不利于长期维护和外部引用。

2. CLI 入口兼容风险
- 使用 `s1downloader.py` 作为 console script 会与 Python 导入机制冲突（会遮蔽包名 `s1downloader`）。
- 处理策略：移除该入口，保留 `s1downloader` + `python -m s1downloader.main` 兼容方式。

3. AOI 文件回退存在非交互阻塞风险
- `--aoi-file` 解析失败时默认进入交互输入，可能卡住非交互流程。
- 处理策略：默认关闭回退提示，仅在 `--allow-aoi-fallback-prompt` 时启用。

4. KML 解析鲁棒性不足
- 对无效 XML/坐标 token 缺少细粒度处理。
- 处理策略：增加 XML 解析错误处理与坐标容错。

5. 凭据写入可改进
- `.netrc` 写入原实现非原子，异常中断可能留下半写文件。
- 处理策略：改为临时文件 + `os.replace` 原子替换。

## 改进建议

- 保持 `s1downloader/` 为主包，`app/` 仅做过渡兼容并在后续版本移除。
- 将 CLI 输出层与业务层继续解耦（例如 future: JSON output mode）。
- 持续扩展异常类型（认证、输入、网络、文件）以提升自动化可观测性。
- 后续可加入集成测试（mock ASF API + 小型样例 manifest）。

## 修复代码片段示例

### 1) 非交互安全 AOI 回退

```python
search.add_argument(
    "--allow-aoi-fallback-prompt",
    action="store_true",
    help="Allow interactive bbox prompt when --aoi-file parsing fails",
)

intersects_with = normalize_aoi_to_wkt(
    wkt_text=args.wkt,
    bbox_text=args.bbox,
    aoi_file=args.aoi_file,
    allow_prompt_fallback=bool(args.allow_aoi_fallback_prompt and sys.stdin.isatty()),
)
```

### 2) `.netrc` 原子写入

```python
temp_path = path.with_name(f".{path.name}.tmp")
temp_path.write_text(updated, encoding="utf-8")
temp_path.chmod(0o600)
os.replace(temp_path, path)
```

## 需要新增/已新增测试

- 已新增：`tests/test_compat_imports.py`（兼容导入验证）。
- 已补充：`track` 参数解析测试（`ASC/DES` 归一化与非法值拒绝）。
- 建议新增（后续）：
  - 非交互环境下 `--aoi-file` 失败应立即报错。
  - CLI 级别 smoke test（`search --help` / `download --help`）。
  - 发布产物安装后命令可用性测试（wheel install smoke）。

## 依赖审查结论

- 运行时依赖与导入一致：`asf-search`, `requests`, `PyYAML`, `shapely`, `pyshp`, `matplotlib`。
- 原项目缺少标准化 `requirements.txt` 与 dev 依赖分层，现已补齐：
  - `requirements.txt`
  - `requirements-dev.txt`
- 已新增工程化依赖：`pytest`, `ruff`, `tox`, `build`, `twine`。
