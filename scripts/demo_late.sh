#!/usr/bin/env bash
# 并发/迟到场景演示：合闸指令迟到、稳定状态查询迟到。
#
# 前提：全新启动的 docker compose（或本地 pdu+api）。建议单独跑，
# 因为脚本会把 srv-1 / srv-2 的 PDU 行为改成延迟模式。
#
#   docker compose up --build
#   bash scripts/demo_late.sh
#
# 可用 API=... PDU=... 覆盖地址。
set -euo pipefail

API=${API:-http://localhost:8000}
PDU=${PDU:-http://localhost:9000}

pretty() { python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2, ensure_ascii=False))"; }
step() { printf '\n\033[1;36m== %s ==\033[0m\n' "$1"; }
pyget() { python3 -c "import sys,json; d=json.load(sys.stdin); print(d$1)"; }

branch() {
  curl -s "$API/api/branch" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(f\"  支路占用 {d['occupied_amps']}A / 余量 {d['remaining_amps']}A；\" + '，'.join(
    f\"{s['server_id']}={s['state']}({s['occupied_amps']}A)\" for s in d['servers']))
"
}

step "场景一：合闸指令迟到 —— 断电释放后，迟到的合闸不得让插座重新通电"
echo "1) 让 srv-1 的 PDU 把合闸指令延迟 3 秒才执行"
curl -s -X POST "$PDU/pdu/admin/mode/srv-1?mode=delayed-close" | pretty
curl -s -X POST "$PDU/pdu/admin/delays/srv-1?close_seconds=3" | pretty

echo "2) srv-1 申请合闸：close 立即返回 201（不干等迟到的 PDU）"
T0=$(date +%s.%N)
curl -s -X POST "$API/api/servers/srv-1/close" | tee /tmp/late_close.json | pretty
T1=$(date +%s.%N)
OID1=$(cat /tmp/late_close.json | pyget "['order_id']")
python3 -c "print(f'   close 耗时 {$T1-$T0:.3f}s（PDU 合闸被延迟 3s，证明接口没有同步等待）')"
branch

echo "3) 合闸指令仍在 PDU 侧排队时，值班员断电 —— open 立即生效"
curl -s -X POST "$API/api/servers/srv-1/open" | pretty

echo "4) 按断电围栏查实释放：PDU 报告的断电代号晚于本单合闸代号 -> 允许释放"
curl -s -X POST "$API/api/orders/$OID1/release" | pretty
branch

echo "5) 等迟到合闸真正到达 PDU；PDU 拒绝过期指令，插座保持断电"
sleep 4
curl -s "$PDU/pdu/status/srv-1" | pretty
echo "   期望 powered=false 且 last_close_fence=null（旧合闸代号从未生效）"
curl -s "$API/api/orders/$OID1" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('   旧单状态:', d['state'], '占用:', d['occupied_amps'], 'A')
print('   历史:', ' -> '.join(h['event'] for h in d['history']))
"
branch

echo "6) 恢复 normal，按原规则开新单仍正常"
curl -s -X POST "$PDU/pdu/admin/mode/srv-1?mode=normal" >/dev/null
curl -s -X POST "$PDU/pdu/admin/delays/srv-1?close_seconds=0" >/dev/null
NEW1=$(curl -s -X POST "$API/api/servers/srv-1/close" | tee /tmp/new1.json | pyget "['order_id']")
cat /tmp/new1.json | pretty
branch
# 清理本场景的新单，方便重复运行脚本
curl -s -X POST "$API/api/servers/srv-1/open" >/dev/null
curl -s -X POST "$API/api/orders/$NEW1/release" >/dev/null

step "场景二：稳定状态查询迟到 —— 释放旧单并开新单后，旧查询结果不得改动任何状态/占额"
echo "1) 让 srv-2 正常合闸并等 PDU 报告稳定"
curl -s -X POST "$PDU/pdu/admin/mode/srv-2?mode=normal" >/dev/null
curl -s -X POST "$PDU/pdu/admin/delays/srv-2?status_seconds=0" >/dev/null
OID2=$(curl -s -X POST "$API/api/servers/srv-2/close" | pyget "['order_id']")
echo "   合闸单: $OID2；等待 PDU_BOOT_SECONDS=5s"
sleep 6
curl -s "$PDU/pdu/status/srv-2" | pretty

echo "2) 让 PDU 的状态查询延迟 1.5 秒返回，并在请求到达时先快照（此刻设备已稳定）"
curl -s -X POST "$PDU/pdu/admin/delays/srv-2?status_seconds=1.5" | pretty

echo "3) 发起 verify-stable（会拿到“已稳定”的旧快照），趁它在途立刻断电并释放"
curl -s -X POST "$API/api/orders/$OID2/verify-stable" > /tmp/late_verify.json &
VERIFY_PID=$!
sleep 0.4
curl -s -X POST "$API/api/servers/srv-2/open" | pretty
curl -s -X POST "$API/api/orders/$OID2/release" | pretty
echo "4) 按原规则立即开新单"
NEW2=$(curl -s -X POST "$API/api/servers/srv-2/close" | tee /tmp/new2.json | pyget "['order_id']")
cat /tmp/new2.json | pretty
branch

echo "5) 等待迟到的稳定查询返回：应被 409 拒绝，不复活旧单、不改新单"
wait $VERIFY_PID || true
echo -n "   迟到结果: "; cat /tmp/late_verify.json | pretty
curl -s "$API/api/orders/$OID2" | python3 -c "
import sys,json; d=json.load(sys.stdin)
print('   旧单:', d['state'], d['occupied_amps'], 'A')"
curl -s "$API/api/orders/$NEW2" | python3 -c "
import sys,json; d=json.load(sys.stdin)
print('   新单:', d['state'], d['occupied_amps'], 'A（应仍为 STARTING / 30A）')"
branch

echo "6) 清理：恢复 srv-2 查询延迟"
curl -s -X POST "$PDU/pdu/admin/delays/srv-2?status_seconds=0" >/dev/null
curl -s -X POST "$API/api/servers/srv-2/open" >/dev/null
curl -s -X POST "$API/api/orders/$NEW2/release" >/dev/null

step "最终支路状态"
curl -s "$API/api/branch" | pretty
echo
echo "演示完成。可对照 GET $API/api/orders 查看每单 history 中的围栏与丢弃事件。"
