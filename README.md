# Clash-config

适用于 **全平台 Mihomo（Clash.Meta）内核客户端** 的合并配置：云端预筛免费节点 + 分流规则，定时自动更新。

## 支持客户端

* **主配置文件（`cmfa-merged-config.yaml`）**：支持所有基于 **Mihomo（Clash.Meta）内核** 的客户端（文件名沿用早期命名以保持订阅链接兼容，全平台通用）：
  * **Android**：Clash Meta for Android (CMFA)、FlClash
  * **Windows / macOS / Linux**：Clash Verge Rev、Clash Party（Mihomo Party）、FlClash、Clash Nyanpasu、mihomo 核心等
* **纯节点文件（`nodes` 分支 `proxies.yaml`）**：标准 Clash 节点格式，可供任意支持 Clash 节点或 `proxy-providers` 的客户端单独引用。

## 订阅链接（复制即用）

```text
https://raw.githubusercontent.com/Komeiji6/Clash-config/main/cmfa-merged-config.yaml
```

> **备用镜像**（raw 链接访问不了时使用，更新延迟约 12 小时）：
>
> ```text
> https://cdn.jsdelivr.net/gh/Komeiji6/Clash-config@main/cmfa-merged-config.yaml
> ```

> **仅筛选后的可用节点文件**（`nodes` 分支，供高级用户或第三方配置单独引用）：
>
> ```text
> https://raw.githubusercontent.com/Komeiji6/Clash-config/nodes/proxies.yaml
> ```

## 使用方式

1. 打开支持 Mihomo 内核的客户端（如 **Clash Verge Rev / Clash Party / FlClash / CMFA** 等）→ 进入「配置 / 订阅（Profiles）」→ **从 URL 导入**
2. 粘贴上面的订阅链接，保存并**启用**该配置
3. 桌面端开启系统代理或 TUN 模式（移动端打开代理开关）；在「代理」页 `PROXY` 组中选节点（或直接选 `AUTO` 自动选优）
4. **全自动更新**：
   * **云端预筛**：GitHub Actions 每 8 小时对上游节点进行一轮真实握手测活，剔除超时、无效及延迟高于 `500ms` 的节点，结果发布至 `nodes` 分支（单提交覆写，不保留冗余历史）
   * **本地更新**：客户端会定时拉取 `nodes` 分支节点与分流规则，并在本地做二次 `health-check` 适配当前网络环境，无需手动维护

## 仓库结构

* `main` 分支：存放主配置文件（`cmfa-merged-config.yaml`）、GitHub Actions 工作流（`.github/workflows/`）及说明文档
* `nodes` 分支：由 Actions 自动维护，仅包含筛选后的 `proxies.yaml` 及状态说明 `README.md`

## 数据来源与工具（Attribution）

本仓库通过动态链接及自动化测活流程引用以下第三方项目的资源，**内容版权归原项目及各自权利人所有**：

### 1. 免费节点

* **[VPN-Subcription-Links/ClashX-V2Ray-TopFreeProxy](https://github.com/VPN-Subcription-Links/ClashX-V2Ray-TopFreeProxy)**
  * 许可协议：仅限非商业用途（Noncommercial Use Only）
  * 节点由该项目聚合自多个公开订阅源，每 8 小时刷新
  * 本仓库通过 GitHub Actions 定时拉取其 `combine/clash.config.yaml`，仅进行去重与连通性筛选（保留延迟 `≤500ms` 的节点），并将筛选结果缓存至 `nodes` 分支供 `proxy-providers` 调用；**继续严格遵守并保留原许可中的非商业用途限制**

### 2. 分流规则（RULE-SET）

* **[Loyalsoldier/clash-rules](https://github.com/Loyalsoldier/clash-rules)**（GPL-3.0）
  * 规则数据来源于 [@Loyalsoldier/v2ray-rules-dat](https://github.com/Loyalsoldier/v2ray-rules-dat)、[@v2fly/domain-list-community](https://github.com/v2fly/domain-list-community)、[@felixonmars/dnsmasq-china-list](https://github.com/felixonmars/dnsmasq-china-list)、[@17mon/china_ip_list](https://github.com/17mon/china_ip_list) 等项目
  * 本仓库仅以 `rule-providers` 方式引用其每日构建产物（`release/*.txt`），**未复制规则文件本身**

### 3. 测活工具与本仓库所做的修改

* 使用 **[faceair/clash-speedtest](https://github.com/faceair/clash-speedtest)**（内置 mihomo 内核）在 GitHub Actions 中实现云端并发连通性预筛（`-fast` 模式，`≤500ms` 延迟过滤）
* 将筛选后的 `nodes` 分支节点与 `Loyalsoldier/clash-rules` 规则整合为通用 Mihomo（Clash.Meta）内核兼容配置
* 新增 DNS 配置（DoH + fake-ip）、策略组（PROXY / AUTO / APPLE / GOOGLE / TELEGRAM）及分流规则编排
* 规则文件本身不包含原始副本，仅保留动态引用

## 免责声明

* 本仓库仅供个人学习与非商业使用，**严禁商业用途**
* 免费节点不保证可用性、安全性，请勿用于传输敏感信息
* 云端筛选节点是在 GitHub 服务器环境下进行的可用性测试，不代表各地本地网络环境下均完全可用
* 使用者应遵守所在地法律法规；因使用本配置造成的任何损失，本仓库维护者不承担责任
* 上游资源（订阅、节点、规则）的可用性、准确性、安全性由各自提供方负责

## License

Copyright (c) 2026 Komeiji6.

本仓库的配置文件及文档以 [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/)（署名-非商业性使用 4.0 国际）发布，详见 [LICENSE](./LICENSE)。

引用的第三方内容受其各自许可协议约束（见上文「数据来源与工具」），其版权与许可不因本仓库的许可而改变。本许可不授予第三方内容的额外使用权。
