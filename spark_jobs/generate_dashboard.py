from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.appName("Dashboard").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs://namenode:9000/data/curated/seller_risk/")
data = [row.asDict() for row in df.collect()]
data.sort(key=lambda x: x['risk_score'], reverse=True)

fraud_df = spark.read.parquet("hdfs://namenode:9000/data/curated/fraud_patterns/")
fraud_data = [row.asDict() for row in fraud_df.collect()]

high = len([d for d in data if d['risk_label']=='HIGH'])
medium = len([d for d in data if d['risk_label']=='MEDIUM'])
low = len([d for d in data if d['risk_label']=='LOW'])
avg_score = sum(d['risk_score'] for d in data) / len(data)

regions = {}
for d in data:
    r = d['region']
    if r not in regions:
        regions[r] = {'sellers':[], 'total':0}
    regions[r]['sellers'].append(d['seller_name'])
    regions[r]['total'] += d['risk_score']

region_avg = {r: v['total']/len(v['sellers']) for r,v in regions.items()}

def risk_color(label):
    return {'HIGH':'#ef4444','MEDIUM':'#f59e0b','LOW':'#22c55e'}.get(label,'#94a3b8')

def risk_bg(label):
    return {'HIGH':'#fee2e2','MEDIUM':'#fef3c7','LOW':'#dcfce7'}.get(label,'#f1f5f9')

def risk_text(label):
    return {'HIGH':'#b91c1c','MEDIUM':'#92400e','LOW':'#15803d'}.get(label,'#475569')

region_cards = ""
for r, v in sorted(regions.items(), key=lambda x: -region_avg[x[0]]):
    avg = region_avg[r]
    label = 'HIGH' if avg >= 60 else 'MEDIUM' if avg >= 30 else 'LOW'
    region_cards += f"""
    <div style="background:white;border:1px solid #e2e8f0;border-left:4px solid {risk_color(label)};border-radius:10px;padding:16px">
        <div style="font-weight:600;font-size:14px">{r}</div>
        <div style="font-size:28px;font-weight:700;color:{risk_color(label)};margin:8px 0">{avg:.1f}</div>
        <div style="font-size:12px;color:#64748b">{', '.join(v['sellers'])} · {len(v['sellers'])} seller(s)</div>
        <span style="display:inline-block;margin-top:8px;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600;background:{risk_bg(label)};color:{risk_text(label)}">{label} RISK</span>
    </div>"""

table_rows = ""
for i, d in enumerate(data):
    rr = d['return_rate'] * 100
    rr_color = '#ef4444' if rr > 40 else '#f59e0b' if rr > 20 else '#22c55e'
    ver_color = '#22c55e' if d['verification_status']=='verified' else '#ef4444'
    table_rows += f"""
    <tr>
        <td style="padding:12px 14px">{i+1}</td>
        <td style="padding:12px 14px"><b>{d['seller_name']}</b><br><span style="color:#94a3b8;font-size:11px">{d['seller_id']}</span></td>
        <td style="padding:12px 14px;color:#64748b">{d['region']}</td>
        <td style="padding:12px 14px;color:{ver_color};font-size:12px">{d['verification_status']}</td>
        <td style="padding:12px 14px">{d['total_orders']}</td>
        <td style="padding:12px 14px;color:{rr_color}">{rr:.1f}%</td>
        <td style="padding:12px 14px;color:{'#ef4444' if d['high_severity_complaints']>=5 else '#64748b'}">{d['high_severity_complaints']}</td>
        <td style="padding:12px 14px;font-weight:600;color:{risk_color(d['risk_label'])}">{d['risk_score']:.1f}</td>
        <td style="padding:12px 14px"><span style="padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600;background:{risk_bg(d['risk_label'])};color:{risk_text(d['risk_label'])}">{d['risk_label']}</span></td>
    </tr>"""

fraud_rows = ""
for f in fraud_data:
    fraud_rows += f"""
    <tr>
        <td style="padding:12px 14px;font-weight:600;color:#ef4444">{f['seller_id']}</td>
        <td style="padding:12px 14px">{f['pattern_type']}</td>
        <td style="padding:12px 14px;color:#64748b">{f['explanation']}</td>
        <td style="padding:12px 14px;font-weight:600;color:#ef4444">{f['indicator_value']:.1f}</td>
        <td style="padding:12px 14px;font-size:11px;color:#94a3b8">{str(f['flagged_at'])[:19]}</td>
    </tr>"""

bar_labels = json.dumps([d['seller_name'] for d in data])
bar_data = json.dumps([round(d['risk_score'],1) for d in data])
bar_colors = json.dumps([risk_color(d['risk_label']) for d in data])
donut_labels = json.dumps(list(regions.keys()))
donut_data = json.dumps([round(v['total'],1) for v in regions.values()])
from datetime import datetime
generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

html = f"""<!DOCTYPE html>
<html>
<head>
<title>Trust & Safety - Geographic Risk Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:system-ui,sans-serif; background:#f8fafc; color:#1e293b; padding:30px; }}
h1 {{ font-size:24px; font-weight:600; margin-bottom:4px; }}
.sub {{ color:#64748b; font-size:13px; margin-bottom:24px; }}
.metrics {{ display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:24px; }}
.metric {{ background:white; border:1px solid #e2e8f0; border-radius:10px; padding:16px; }}
.metric .label {{ font-size:12px; color:#64748b; margin-bottom:6px; }}
.metric .value {{ font-size:28px; font-weight:600; }}
.charts {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:24px; }}
.card {{ background:white; border:1px solid #e2e8f0; border-radius:10px; padding:20px; }}
.card h3 {{ font-size:14px; font-weight:600; margin-bottom:16px; }}
.regions {{ display:grid; grid-template-columns:repeat(2,1fr); gap:12px; margin-bottom:24px; }}
table {{ width:100%; border-collapse:collapse; background:white; border-radius:10px; overflow:hidden; border:1px solid #e2e8f0; margin-bottom:24px; }}
th {{ text-align:left; padding:10px 14px; font-size:11px; color:#64748b; font-weight:500; border-bottom:1px solid #e2e8f0; background:#f8fafc; }}
td {{ border-bottom:1px solid #f1f5f9; font-size:13px; }}
tr:last-child td {{ border-bottom:none; }}
</style>
</head>
<body>
<h1>Trust & Safety — Geographic Risk Dashboard</h1>
<p class="sub">Live data · Generated: {generated_at} · {len(data)} sellers · {sum(d['total_orders'] for d in data)} total orders</p>

<div class="metrics">
  <div class="metric"><div class="label">Total sellers</div><div class="value" style="color:#3b82f6">{len(data)}</div></div>
  <div class="metric"><div class="label">High risk</div><div class="value" style="color:#ef4444">{high}</div></div>
  <div class="metric"><div class="label">Medium risk</div><div class="value" style="color:#f59e0b">{medium}</div></div>
  <div class="metric"><div class="label">Avg risk score</div><div class="value" style="color:#8b5cf6">{avg_score:.1f}</div></div>
</div>

<div class="charts">
  <div class="card"><h3>Risk score by seller</h3><div style="position:relative;height:220px"><canvas id="c1" role="img" aria-label="Risk scores by seller"></canvas></div></div>
  <div class="card"><h3>Risk by region</h3><div style="position:relative;height:220px"><canvas id="c2" role="img" aria-label="Risk by region"></canvas></div></div>
</div>

<div class="regions">{region_cards}</div>

<div class="card" style="margin-bottom:24px">
  <h3>Seller Investigation Priority List</h3>
  <table>
    <thead><tr><th>#</th><th>Seller</th><th>Region</th><th>Status</th><th>Orders</th><th>Return Rate</th><th>High Complaints</th><th>Risk Score</th><th>Label</th></tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
</div>

<div class="card">
  <h3>Fraud Patterns Detected</h3>
  <table>
    <thead><tr><th>Seller</th><th>Pattern</th><th>Explanation</th><th>Score</th><th>Flagged At</th></tr></thead>
    <tbody>{fraud_rows}</tbody>
  </table>
</div>

<script>
new Chart(document.getElementById('c1'),{{type:'bar',data:{{labels:{bar_labels},datasets:[{{data:{bar_data},backgroundColor:{bar_colors},borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{x:{{ticks:{{font:{{size:10}},maxRotation:35,autoSkip:false}},grid:{{display:false}}}},y:{{grid:{{color:'#f1f5f9'}}}}}}}}}});
new Chart(document.getElementById('c2'),{{type:'doughnut',data:{{labels:{donut_labels},datasets:[{{data:{donut_data},backgroundColor:['#fca5a5','#fcd34d','#86efac','#93c5fd','#c4b5fd'],borderWidth:1}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom',labels:{{font:{{size:11}},padding:10}}}}}}}}}});
</script>
</body>
</html>"""

with open("/tmp/dashboard.html", "w") as f:
    f.write(html)

print("✅ Dashboard generated!")
spark.stop()
