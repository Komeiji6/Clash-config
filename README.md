# cmfa-config

适用于 **Clash Meta for Android (CMFA)** 的合并配置：免费节点 + 分流规则，内容每日自动更新。

## 订阅链接（复制即用）

```
https://raw.githubusercontent.com/Komeiji6/CMFA-config-/main/cmfa-merged-config.yaml
```

> **备用镜像**（raw 链接访问不了时使用，更新延迟约 12 小时）：
> ```
> https://cdn.jsdelivr.net/gh/Komeiji6/CMFA-config-@main/cmfa-merged-config.yaml
> ```

## 使用方式

1. 打开 **CMFA**（Clash Meta for Android）→ **配置** → 右下角 **+** → **从 URL 导入**
2. 粘贴上面的订阅链接，保存并**点选启用**该配置
3. 回到首页打开代理开关；在「代理」页 `PROXY` 组中选节点（或选 `AUTO` 自动测速）
4. 节点与规则均为动态拉取（`proxy-providers` / `rule-providers`），**每天自动更新**，无需手动维护

## 数据来源（Attribution）

本仓库的配置文件通过动态链接引用以下第三方项目的资源，**内容版权归原项目及各自权利人所有**：

### 1. 免费节点

- **[VPN-Subcription-Links/ClashX-V2Ray-TopFreeProxy](https://github.com/VPN-Subcription-Links/ClashX-V2Ray-TopFreeProxy)**
  - 许可协议：仅限非商业用途（Noncommercial Use Only）
  - 节点由该项目聚合自多个公开订阅源，每 8 小时刷新
  - 本仓库仅以 `proxy-providers` 方式引用其 `combine/clash.config.yaml`，**未复制其节点数据**；引用时保留其许可中的非商业用途限制

### 2. 分流规则（RULE-SET）

- **[Loyalsoldier/clash-rules](https://github.com/Loyalsoldier/clash-rules)**（GPL-3.0）
  - 规则数据来源于 [@Loyalsoldier/v2ray-rules-dat](https://github.com/Loyalsoldier/v2ray-rules-dat)、[@v2fly/domain-list-community](https://github.com/v2fly/domain-list-community)、[@felixonmars/dnsmasq-china-list](https://github.com/felixonmars/dnsmasq-china-list)、[@17mon/china_ip_list](https://github.com/17mon/china_ip_list) 等项目
  - 本仓库仅以 `rule-providers` 方式引用其每日构建产物（`release/*.txt`），**未复制规则文件本身**

### 3. 本仓库所做的修改

- 将上述两类资源整合为一份 CMFA（mihomo 内核）兼容配置
- 新增 DNS 配置（DoH + fake-ip）、策略组（PROXY / AUTO / APPLE / GOOGLE / TELEGRAM）、分流规则编排
- 配置本身不包含第三方节点或规则的原始数据副本，仅保留动态引用

## 免责声明

- 本仓库仅供个人学习与非商业使用，**严禁商业用途**
- 免费节点不保证可用性、安全性，请勿用于传输敏感信息
- 使用者应遵守所在地法律法规；因使用本配置造成的任何损失，本仓库维护者不承担责任
- 上游资源（订阅、节点、规则）的可用性、准确性、安全性由各自提供方负责

## License

Copyright (c) 2026 Komeiji6.

本仓库的配置文件及文档以 [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/)（署名-非商业性使用 4.0 国际）发布，详见 [LICENSE.md](LICENSE.md)。

引用的第三方内容受其各自许可协议约束（见上文「数据来源」），其版权与许可不因本仓库的许可而改变。本许可不授予第三方内容的额外使用权。