# Clash-config

开箱即用的高品质 **Mihomo (Clash.Meta)** 全平台配置与实时精选节点订阅。  
全天候自动化多源聚合、测速筛选与精细分流，全量排除中国大陆落地节点，导入即用。

---

## 🔗 快速获取订阅链接

根据你的使用习惯，选择下方任一方式即可：

### 方式一：完整配置订阅（推荐 · 新手友好）
> 内置精细化国内/海外分流规则、流媒体与广告拦截规则，无需额外配置，导入即可直接使用。

* **GitHub 直连（推荐，实时更新无缓存）**：
  ```text
  https://raw.githubusercontent.com/Komeiji6/Clash-config/main/cmfa-merged-config.yaml
  ```
* **CDN 加速镜像（GitHub 直连受阻时备用）**：
  ```text
  https://cdn.jsdelivr.net/gh/Komeiji6/Clash-config@main/cmfa-merged-config.yaml
  ```

---

### 方式二：纯节点订阅（高级用户 · 自建配置）
> 仅包含经过精选和分类的实时可用节点，适合在你现有的配置中通过 `proxy-providers` 引入。

* **GitHub 直连**：
  ```text
  https://raw.githubusercontent.com/Komeiji6/Clash-config/nodes/proxies.yaml
  ```
* **CDN 加速镜像**：
  ```text
  https://cdn.jsdelivr.net/gh/Komeiji6/Clash-config@nodes/proxies.yaml
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
进入客户端的 **「代理 / Proxies」** 页面，在 `PROXY` 主策略组中可自由选择：
* **`自动选择`**：自动测速并实时选择当前延迟最低的节点（日常使用最省心）；
* **`香港` / `日本` / `新加坡` / `美国` / `其他地区`**：已全面支持**手动点选具体节点**，也可选择该地区内的自动测速组；
* **`纯净节点`**：优先调度经纯净度鉴定的原生/住宅出口节点；
* **`国内直连` / `漏网之鱼`**：已内置最优分流规则。

---

## 💡 特性与使用建议

1. **杜绝国内流量回环**：全量过滤并剔除中国大陆落地节点，所有入库节点均为海外及港澳台优质节点；
2. **多地区支持自由单选**：各地区策略组均为 Select 模式，既可手动点选具体单条节点，也可选择该地区自动优选；
3. **建议自动更新频率**：免费公共节点具有动态时效性，建议在客户端设置中将该配置的**自动更新间隔（Update Interval）设置为 2 小时或 6 小时**，保持节点实时最新。

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
