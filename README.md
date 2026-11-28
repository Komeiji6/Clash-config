# Clash Live Nodes (`nodes` branch)

全天候自动化实时测速、去重与质量筛选的 Clash 节点订阅（纯节点文件：`proxies.yaml`）。

---

## 🔗 快速获取节点订阅

复制下方链接直接导入支持 Clash / Mihomo 的客户端，或在自建配置中引入：

* **GitHub 直连（推荐，实时更新无缓存）**：
  ```text
  https://raw.githubusercontent.com/Komeiji6/Clash-config/nodes/proxies.yaml
  ```
* **CDN 镜像（GitHub 直连受阻时备用）**：
  ```text
  https://cdn.jsdelivr.net/gh/Komeiji6/Clash-config@nodes/proxies.yaml
  ```
* **国内反代镜像（校园网/直连受阻备用）**：
  ```text
  https://ghfast.top/https://raw.githubusercontent.com/Komeiji6/Clash-config/nodes/proxies.yaml
  ```

> 💡 **需要开箱即用完整分流配置？** 请访问 [main 分支主页](https://github.com/Komeiji6/Clash-config) 获取内置精细规则分流的完整主配置。

---

## 🛠️ 在已有配置中引用 (proxy-providers)

如果你已有自己的 Clash / Mihomo 配置文件，只需在配置中添加以下代码段即可无缝引入：

```yaml
proxy-providers:
  free-nodes:
    type: http
    url: "https://raw.githubusercontent.com/Komeiji6/Clash-config/nodes/proxies.yaml"
    interval: 7200
    path: ./providers/free-nodes.yaml
    health-check:
      enable: true
      interval: 300
      url: http://www.gstatic.com/generate_204
```

---

## 📊 节点状态与分布

| 指标 | 状态 / 数量 |
| :--- | :--- |
| **最后更新时间 (UTC)** | `2026-11-28 15:29:48 UTC` |
| **当前入库节点总数** | **50** (已全量排除中国大陆落地) |
| **非代理 IP / 代理·VPN 标记** | 43 / 7 |
| **原生 IP / 广播 IP** | 23 / 27 |
| **Google 无封禁 / Gemini 可达** | 40 / 44 |

### 落地国家 / 地区分布

| 地区代码 | 地区 | 节点数 |
| :---: | :---: | :---: |
| `US` | 美国 | 14 |
| `DE` | 德国 | 10 |
| `RU` | 俄罗斯 | 4 |
| `FI` | 芬兰 | 4 |
| `IT` | 意大利 | 4 |
| `HK` | 香港 | 3 |
| `NL` | 荷兰 | 2 |
| `GB` | 英国 | 2 |
| `RO` | 罗马尼亚 | 1 |
| `TH` | 泰国 | 1 |
| `EE` | 爱沙尼亚 | 1 |
| `IE` | 爱尔兰 | 1 |
| `ID` | 印尼 | 1 |
| `CZ` | 捷克 | 1 |
| `SA` | 海外其他 | 1 |

---
*注：节点命名规则为 `{地区}|{非代理/代理/VPN/代理VPN}·{原生/广播/未知}|{序号}`，已按综合质量评分降序排列。*
