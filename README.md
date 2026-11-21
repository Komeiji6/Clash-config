# Clash-config

适用于 **全平台 Mihomo（Clash.Meta）内核客户端** 的高品质配置：由本地 **硬件探针** 进行国内真实网络全量测活 + 落地原生住宅 IP（纯净度）打标 + 分流规则编排，全自动定时同步。


## 📱 支持客户端

* **主配置文件（`cmfa-merged-config.yaml`）**：全平台基于 **Mihomo（Clash.Meta）内核** 的客户端通用：
  * **Android**：Clash Meta for Android (CMFA)、FlClash
  * **Windows / macOS / Linux**：Clash Verge Rev、Clash Party（Mihomo Party）、FlClash、Clash Nyanpasu、Mihomo 核心等
* **纯节点文件（`nodes` 分支 `proxies.yaml`）**：标准 Clash 节点格式，各策略组最多保留 20 个最优节点，供第三方配置通过 `proxy-providers` 调用。

## 🔗 订阅链接（复制即用）

### 1. 主配置文件（推荐，含策略组与精细分流）
```text
https://raw.githubusercontent.com/Komeiji6/Clash-config/main/cmfa-merged-config.yaml
```
> **备用加速镜像**（raw 无法访问时使用，国内 CDN 缓存延迟约数小时）：
> ```text
> https://cdn.jsdelivr.net/gh/Komeiji6/Clash-config@main/cmfa-merged-config.yaml
> ```

### 2. 纯精选节点文件（供自建配置通过 `proxy-providers` 引用）
```text
https://raw.githubusercontent.com/Komeiji6/Clash-config/nodes/proxies.yaml
```
> **备用加速镜像**：
> ```text
> https://cdn.jsdelivr.net/gh/Komeiji6/Clash-config@nodes/proxies.yaml
> ```

## 🛠️ 使用方式

1. 打开支持 Mihomo 内核的客户端（如 **CMFA / Clash Verge Rev / Clash Party / FlClash** 等）→ 进入「配置 / 订阅（Profiles）」→ **从 URL 导入**；
2. 粘贴上方的主配置订阅链接，保存并**启用**；
3. 开启系统代理或 TUN 模式；在「代理」页中的 `PROXY` 组中切换：
   * 想体验流媒体或 AI 解锁：直接选择 **`纯净节点`** 策略组；
   * 想延迟最低自动切换：直接选择 **`自动选择`** 策略组；
   * 需要指定国家：选择 **`香港` / `日本` / `新加坡` / `美国` / `其他地区`**；
4. **全自动维护**：本地高通 410 探针持续推流更新，客户端定期自动拉取，完全免去人工搜集、导入和测速的烦恼。



## 📜 数据来源与致谢（Attribution）

本仓库通过自动化动态链接与引用以下优秀的开源项目，**版权归原项目及各自权利人所有**：
1. **[VPN-Subcription-Links/ClashX-V2Ray-TopFreeProxy](https://github.com/VPN-Subcription-Links/ClashX-V2Ray-TopFreeProxy)**：提供公开免费聚合节点订阅源（严格遵照原非商业用途限制）。
2. **[Loyalsoldier/clash-rules](https://github.com/Loyalsoldier/clash-rules)**（GPL-3.0）：提供精细高效的分流规则集（动态引用）。
3. **[MetaCubeX/mihomo](https://github.com/MetaCubeX/mihomo)**：强大的底层代理内核支持。

## ⚖️ 免责声明

* 本项目仅供网络技术学习与个人非商业交流，**严禁用于商业用途**；
* 免费公开节点具有时效性与不可预见性，请勿用于传输任何个人敏感信息或关键生产业务；
* 使用者应严格遵守当地相关法律法规，因使用本项目产生的任何后果由使用者自行承担。

## License

Copyright (c) 2026 Komeiji6.
本项目主配置文件及相关文档以 [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) 许可协议发布。
