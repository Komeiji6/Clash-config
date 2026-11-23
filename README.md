# Clash-config

开箱即用的高品质 **Mihomo (Clash.Meta)** 全平台配置与实时精选节点订阅。  
全天候自动化多源聚合、双向数据流实测、无套娃精简扁平化策略组，导入即用。

---

## 🔗 快速获取订阅链接

根据你的使用习惯，选择下方任一方式即可：

### 方式一：完整配置订阅（推荐 · 开箱即用）
> 内置精细化国内/海外分流规则与广告拦截，内嵌全量经过端到端真实 HTTPS 校验的存活节点，杜绝任何外部依赖与套娃，导入即可直接畅连。

* **GitHub 直连（推荐，实时更新无缓存）**：
  ```text
  https://raw.githubusercontent.com/Komeiji6/Clash-config/main/cmfa-merged-config.yaml
  ```
* **CDN 加速镜像（国内免代理导入备用）**：
  ```text
  https://cdn.jsdelivr.net/gh/Komeiji6/Clash-config@main/cmfa-merged-config.yaml
  ```
* **国内反代镜像（校园网/直连受阻备用）**：
  ```text
  https://ghproxy.net/https://raw.githubusercontent.com/Komeiji6/Clash-config/main/cmfa-merged-config.yaml
  ```

---

### 方式二：纯节点订阅（高级用户 · 自建配置）
> 仅包含经过精选和双向数据流实测的实时可用节点，适合在你现有的配置中通过 `proxy-providers` 引入。

* **GitHub 直连**：
  ```text
  https://raw.githubusercontent.com/Komeiji6/Clash-config/nodes/proxies.yaml
  ```
* **CDN 加速镜像**：
  ```text
  https://cdn.jsdelivr.net/gh/Komeiji6/Clash-config@nodes/proxies.yaml
  ```
* **国内反代镜像**：
  ```text
  https://ghproxy.net/https://raw.githubusercontent.com/Komeiji6/Clash-config/nodes/proxies.yaml
  ```

---

## 📱 客户端使用教程

本配置适用于所有支持 **Mihomo（原 Clash.Meta）内核** 的全平台客户端：
- **Android**：Clash Meta for Android (CMFA)、FlClash
- **Windows / macOS / Linux**：Clash Verge Rev、Mihomo Party（Clash Party）、FlClash 等

### 步骤 1：导入订阅
1. 打开客户端，进入 **「配置 / 订阅 / Profiles」** 页面；
2. 点击 **「从 URL 导入」**（或「新建订阅」）；
3. 粘贴上方的 **完整配置订阅链接**，输入任意配置名称（例如 `Clash-Live`）；
4. 保存后，在列表中选中并**启用**该配置。

### 步骤 2：启动代理
1. 在客户端主界面开启 **「系统代理」** 或 **「TUN 模式」**；
2. 此时常用国内网站走直连，海外流量已自动分流。

### 步骤 3：选择策略组与节点
进入客户端的 **「代理 / Proxies」** 页面：
* **`PROXY` 主策略组**：平铺展示所有经过全链路实测的合格节点，**已按真实综合延迟由低到高全局排序**（排在最前面的永远是当前网络下最快、最平稳的节点）。所有节点均采用中文地区标准命名（如 `香港 | 优选 | 01`、`新加坡 | 优选 | 01`），直接随手点选顶部节点即可畅连；
* **`漏网之鱼` 策略组**：未匹配到特定规则的兜底流量，默认交由 `PROXY` 代理，也可一键切为 `DIRECT` 直连。

---

## 💡 特性与技术亮点

1. **彻底拒绝套娃与虚假节点**：无冗余多层嵌套、无空规则引发的 `COMPATIBLE` 虚假占位，秒级极速载入；
2. **端到端双向数据传输硬核实测**：不仅测试握手延迟，更通过代理隧道完成完整的 Google 204 与 Cloudflare 双向微数据流拉取，彻底杜绝“显示低延绿灯但实际打不开网页”的假活节点；
3. **动态抗抖动（Anti-Jitter）与稳定性熔断**：双轮严测抖动差值，抖动过大或偶发丢包直接淘汰，保障移动端与校园网丝滑体验；
4. **全天候每 1 小时自动更新**：随身 WiFi 硬件持续巡检，定时剔除失效节点、推流最新高品质节点；
5. **杜绝国内流量回环**：全量过滤并剔除中国大陆落地节点，所有入库节点均为海外及港澳台优质节点。

---

## 📜 数据来源与致谢

本项目通过自动化动态链接与引用以下优秀开源项目，版权归原作者及权利人所有：
1. **[Loyalsoldier/clash-rules](https://github.com/Loyalsoldier/clash-rules)**：精细高效的分流规则集（GPL-3.0）；
2. **[MetaCubeX/mihomo](https://github.com/MetaCubeX/mihomo)**：底层代理内核支持；
3. 公开免费开源节点池（AutoMergePublicNodes、awesome-vpn、NoMoreWalls、proxypool 等）。

## ⚖️ 免责声明

* 本项目仅供网络技术学习与个人非商业测试交流，**严禁用于商业用途**；
* 公开免费节点具有时效性与不可预见性，请勿用于传输任何个人敏感数据或关键业务；
* 使用者应严格遵守当地相关法律法规，因使用本项目产生的任何后果由使用者自行承担。

## License

Copyright (c) 2026 Komeiji6.
本项目主配置文件及相关文档以 [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) 许可协议发布。
