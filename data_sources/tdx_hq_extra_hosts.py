"""Supplemental TDX HQ candidates vendored from other 7709 clients.

These rows are IP/port lists only. Quote still speaks pytdx; mootdx / xmtdx /
eltdx / tdxpy are not runtime dependencies. Duplicate ip:port against
DEFAULT_HQ_HOSTS and installed pytdx hq_hosts are omitted here.

Sources:
  - mootdx.consts.HQ_HOSTS / GP_HOSTS
  - xmtdx.transport.sync.KNOWN_HOSTS
  - eltdx.hosts.FALLBACK_HOSTS / tdx_server.json
  - injoyai/tdx hosts.go names for overlapping cloud IPs
"""

from typing import Any

SUPPLEMENTAL_HQ_HOSTS: list[dict[str, Any]] = [
    {"ip": "8.129.13.54", "port": 7709, "name": "mootdx 深圳双线主站2"},
    {"ip": "8.129.174.169", "port": 7709, "name": "mootdx 深圳双线主站5"},
    {"ip": "43.139.18.171", "port": 7709, "name": "eltdx 腾讯云广州"},
    {"ip": "43.139.95.83", "port": 7709, "name": "eltdx 腾讯云广州"},
    {"ip": "47.100.236.28", "port": 7709, "name": "mootdx 上海双线主站2"},
    {"ip": "47.107.64.168", "port": 7709, "name": "mootdx 深圳双线主站7"},
    {"ip": "47.107.75.159", "port": 7709, "name": "xmtdx"},
    {"ip": "47.113.94.204", "port": 7709, "name": "mootdx 深圳双线主站4"},
    {"ip": "47.116.21.80", "port": 7709, "name": "mootdx 上海双线主站4"},
    {"ip": "47.116.105.28", "port": 7709, "name": "mootdx 上海双线主站5"},
    {"ip": "49.232.15.141", "port": 7709, "name": "eltdx 腾讯云北京"},
    {"ip": "59.175.238.38", "port": 7709, "name": "xmtdx"},
    {"ip": "62.234.50.143", "port": 7709, "name": "eltdx 腾讯云北京"},
    {"ip": "81.70.151.186", "port": 7709, "name": "eltdx 腾讯云北京"},
    {"ip": "81.71.32.47", "port": 7709, "name": "eltdx 腾讯云广州"},
    {"ip": "82.156.174.84", "port": 7709, "name": "eltdx 腾讯云北京"},
    {"ip": "82.156.214.79", "port": 7709, "name": "eltdx"},
    {"ip": "101.33.225.16", "port": 7709, "name": "eltdx 腾讯云广州"},
    {"ip": "101.35.121.35", "port": 7709, "name": "eltdx 腾讯云上海"},
    {"ip": "101.42.164.241", "port": 7709, "name": "eltdx 腾讯云北京"},
    {"ip": "101.42.240.54", "port": 7709, "name": "eltdx 腾讯云北京"},
    {"ip": "101.43.159.194", "port": 7709, "name": "eltdx 腾讯云北京"},
    {"ip": "101.133.214.242", "port": 7709, "name": "mootdx 上海双线主站3"},
    {"ip": "106.14.190.242", "port": 7709, "name": "mootdx 上海双线主站8"},
    {"ip": "106.14.201.131", "port": 7709, "name": "mootdx 上海双线主站7"},
    {"ip": "110.41.2.72", "port": 7709, "name": "eltdx 华为广州"},
    {"ip": "110.41.147.114", "port": 7709, "name": "mootdx 深圳双线主站1"},
    {"ip": "110.41.154.219", "port": 7709, "name": "mootdx 深圳双线主站6"},
    {"ip": "111.229.247.189", "port": 7709, "name": "eltdx 腾讯上海"},
    {"ip": "111.230.186.52", "port": 7709, "name": "eltdx 腾讯广州"},
    {"ip": "111.231.113.208", "port": 7709, "name": "eltdx 腾讯云上海"},
    {"ip": "116.205.163.254", "port": 7709, "name": "mootdx 广州双线主站5"},
    {"ip": "116.205.171.132", "port": 7709, "name": "mootdx 广州双线主站6"},
    {"ip": "116.205.183.150", "port": 7709, "name": "mootdx 广州双线主站7"},
    {"ip": "118.25.98.114", "port": 7709, "name": "eltdx 腾讯上海"},
    {"ip": "119.97.185.59", "port": 7709, "name": "mootdx 武汉电信主站1"},
    {"ip": "120.24.149.49", "port": 7709, "name": "mootdx 深圳双线主站3"},
    {"ip": "120.46.186.223", "port": 7709, "name": "mootdx 北京双线主站5"},
    {"ip": "120.53.8.251", "port": 7709, "name": "eltdx 腾讯云北京"},
    {"ip": "120.76.152.87", "port": 7709, "name": "mootdx 默认财务数据线路"},
    {"ip": "121.36.54.217", "port": 7709, "name": "mootdx 北京双线主站1"},
    {"ip": "121.36.81.195", "port": 7709, "name": "mootdx 北京双线主站2"},
    {"ip": "121.36.225.169", "port": 7709, "name": "mootdx 上海双线主站9"},
    {"ip": "122.51.120.217", "port": 7709, "name": "eltdx 腾讯上海"},
    {"ip": "122.51.232.182", "port": 7709, "name": "eltdx 腾讯上海"},
    {"ip": "123.60.70.228", "port": 7709, "name": "mootdx 上海双线主站10"},
    {"ip": "123.60.73.44", "port": 7709, "name": "mootdx 上海双线主站11"},
    {"ip": "123.60.84.66", "port": 7709, "name": "mootdx 上海双线主站15"},
    {"ip": "123.60.164.122", "port": 7709, "name": "eltdx"},
    {"ip": "123.249.15.60", "port": 7709, "name": "mootdx 北京双线主站3"},
    {"ip": "124.70.22.210", "port": 7709, "name": "mootdx 北京双线主站6"},
    {"ip": "124.70.75.113", "port": 7709, "name": "mootdx 北京双线主站4"},
    {"ip": "124.70.133.119", "port": 7709, "name": "mootdx 上海双线主站12"},
    {"ip": "124.70.176.52", "port": 7709, "name": "mootdx 上海双线主站1"},
    {"ip": "124.70.199.56", "port": 7709, "name": "mootdx 上海双线主站6"},
    {"ip": "124.71.9.153", "port": 7709, "name": "mootdx 广州双线主站4"},
    {"ip": "124.71.85.110", "port": 7709, "name": "mootdx 广州双线主站1"},
    {"ip": "124.71.187.72", "port": 7709, "name": "mootdx 上海双线主站13"},
    {"ip": "124.71.187.122", "port": 7709, "name": "mootdx 上海双线主站14"},
    {"ip": "124.223.163.242", "port": 7709, "name": "eltdx 腾讯云上海"},
    {"ip": "129.204.230.128", "port": 7709, "name": "eltdx 腾讯云广州"},
    {"ip": "139.9.51.18", "port": 7709, "name": "mootdx 广州双线主站2"},
    {"ip": "139.9.133.247", "port": 7709, "name": "mootdx 北京双线主站7"},
    {"ip": "139.159.239.163", "port": 7709, "name": "mootdx 广州双线主站3"},
    {"ip": "150.158.160.2", "port": 7709, "name": "eltdx 腾讯云上海"},
    {"ip": "152.136.191.169", "port": 7709, "name": "eltdx 腾讯云北京"},
    {"ip": "159.75.29.111", "port": 7709, "name": "eltdx 腾讯云广州"},
    {"ip": "175.178.112.197", "port": 7709, "name": "eltdx 腾讯云广州"},
    {"ip": "175.178.128.227", "port": 7709, "name": "eltdx 腾讯云广州"},
    {"ip": "180.153.18.172", "port": 7709, "name": "xmtdx"},
    {"ip": "47.107.228.47", "port": 7719, "name": "mootdx 深圳双线主站8"},
]
