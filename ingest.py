"""
Pure Python stdlib HTTP server — SAP O2C Graph Query System
Endpoints:
  GET  /api/graph   → graph JSON
  GET  /api/stats   → row counts
  POST /api/query   → NL query → SQL → answer
  GET  /            → frontend
"""
import sqlite3, json, os, sys, re, urllib.request
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE, 'data', 'otc.db')
FRONTEND_DIR = os.path.join(BASE, 'frontend')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from graph_builder import build_graph_json, SCHEMA

GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
GROQ_URL = 'https://api.groq.com/openai/v1/chat/completions'

SYSTEM_PROMPT = f"""You are a data analyst assistant for an SAP Order-to-Cash (O2C) business dataset.
You ONLY answer questions about this dataset. If asked anything unrelated — general knowledge, coding help, creative writing, world events, opinions, recipes, weather, sports — respond EXACTLY with this string and nothing else:
GUARDRAIL: This system is designed to answer questions related to the provided dataset only.

{SCHEMA}

When the user asks a data question, respond ONLY with a valid JSON object (no markdown, no code fences):
{{
  "sql": "SELECT ...",
  "explanation": "brief one-sentence description of what the query finds",
  "highlighted_nodes": ["node_id_1"]
}}

Rules for SQL:
- Use only SQLite-compatible syntax
- Always use LIMIT (max 100 rows unless a count query)
- highlighted_nodes should contain entity IDs mentioned in or returned by the query (e.g. SO00001, INV00001, C0001)
- If no specific IDs, set highlighted_nodes to []
"""

GUARDRAIL_KEYWORDS = [
    'capital of','president of','prime minister','who invented','write a poem','write a story',
    'recipe for','how to cook','weather','stock price','bitcoin','cryptocurrency','nba','nfl',
    'tell me a joke','meaning of life','python tutorial','javascript','what is love',
    'world war','history of','explain gravity','black hole','einstein','darwin',
]
BUSINESS_TERMS = [
    'order','delivery','invoice','payment','customer','product','material','billing',
    'sales','shipment','plant','vendor','revenue','status','amount','flow','trace',
    'find','list','show','how many','which','what','top','overdue','unpaid','complete',
    'broken','incomplete','paid','open','cancelled','partial','carrier','region',
]

def is_off_topic(q: str) -> bool:
    q = q.lower()
    if any(kw in q for kw in GUARDRAIL_KEYWORDS):
        return True
    return not any(t in q for t in BUSINESS_TERMS)

def call_groq(question: str, api_key: str) -> dict:
    payload = json.dumps({
        'model': 'llama3-70b-8192',
        'messages': [
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content': question}
        ],
        'temperature': 0.05, 'max_tokens': 800
    }).encode()
    req = urllib.request.Request(GROQ_URL, data=payload,
        headers={'Content-Type':'application/json','Authorization':f'Bearer {api_key}'}, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        content = data['choices'][0]['message']['content'].strip()
        if content.startswith('GUARDRAIL:'):
            return {'guardrail': True}
        content = re.sub(r'^```json\s*', '', content); content = re.sub(r'```\s*$', '', content)
        return json.loads(content)
    except json.JSONDecodeError as e:
        return {'error': f'LLM returned non-JSON: {str(e)}'}
    except Exception as e:
        return {'error': str(e)}

def fallback_sql(question: str) -> dict:
    q = question.lower()
    if 'billing' in q and ('product' in q or 'material' in q):
        return {'sql': """SELECT m.description, COUNT(DISTINCT i.invoice_id) as billing_count
FROM materials m JOIN sales_order_items soi ON m.material_id=soi.material_id
JOIN invoices i ON soi.sales_order_id=i.sales_order_id
GROUP BY m.material_id,m.description ORDER BY billing_count DESC LIMIT 10""",
            'explanation':'Products ranked by number of billing documents.','highlighted_nodes':[]}

    if 'trace' in q or 'full flow' in q:
        m = re.search(r'(SO\d+|INV\d+|DEL\d+)',question.upper())
        eid = m.group(1) if m else 'SO00001'
        col = 'so.sales_order_id' if eid.startswith('SO') else 'i.invoice_id' if eid.startswith('INV') else 'd.delivery_id'
        return {'sql':f"""SELECT so.sales_order_id,so.status as so_status,so.order_date,
d.delivery_id,d.status as del_status,d.actual_delivery_date,
i.invoice_id,i.status as inv_status,i.amount,
p.payment_id,p.amount as paid_amount,p.payment_date
FROM sales_orders so
LEFT JOIN deliveries d ON so.sales_order_id=d.sales_order_id
LEFT JOIN invoices i ON so.sales_order_id=i.sales_order_id
LEFT JOIN payments p ON i.invoice_id=p.invoice_id
WHERE {col}='{eid}'""",
            'explanation':f'Full O2C flow trace for {eid}.','highlighted_nodes':[eid]}

    if any(w in q for w in ['broken','incomplete','not billed','without delivery','delivered but']):
        return {'sql':"""SELECT so.sales_order_id, so.status,
CASE WHEN d.delivery_id IS NULL THEN 'No Delivery'
     WHEN i.invoice_id IS NULL THEN 'Delivered but not Billed'
     WHEN p.payment_id IS NULL THEN 'Billed but not Paid'
     ELSE 'Complete' END as flow_status, so.total_value
FROM sales_orders so
LEFT JOIN deliveries d ON so.sales_order_id=d.sales_order_id AND d.status='DELIVERED'
LEFT JOIN invoices i ON so.sales_order_id=i.sales_order_id
LEFT JOIN payments p ON i.invoice_id=p.invoice_id
WHERE so.status!='CANCELLED'
AND (d.delivery_id IS NULL OR i.invoice_id IS NULL OR p.payment_id IS NULL)
ORDER BY flow_status LIMIT 30""",
            'explanation':'Sales orders with broken or incomplete O2C flows.','highlighted_nodes':[]}

    if 'overdue' in q or 'unpaid' in q or 'outstanding' in q:
        return {'sql':"""SELECT i.invoice_id,c.name as customer,i.amount,i.due_date,i.status
FROM invoices i JOIN customers c ON i.customer_id=c.customer_id
WHERE i.status IN ('OVERDUE','OPEN') ORDER BY i.due_date ASC LIMIT 20""",
            'explanation':'Open and overdue invoices.','highlighted_nodes':[]}

    if 'customer' in q and any(w in q for w in ['revenue','top','highest','most']):
        return {'sql':"""SELECT c.name,c.region,COUNT(DISTINCT so.sales_order_id) as orders,
ROUND(SUM(so.total_value),2) as total_revenue
FROM customers c LEFT JOIN sales_orders so ON c.customer_id=so.customer_id
GROUP BY c.customer_id,c.name,c.region ORDER BY total_revenue DESC""",
            'explanation':'Customers ranked by total revenue.','highlighted_nodes':[]}

    if 'summary' in q or 'pipeline' in q or 'overall' in q:
        return {'sql':"""SELECT
(SELECT COUNT(*) FROM sales_orders) as total_orders,
(SELECT COUNT(*) FROM sales_orders WHERE status='COMPLETE') as complete_orders,
(SELECT COUNT(*) FROM deliveries WHERE status='DELIVERED') as delivered,
(SELECT COUNT(*) FROM invoices) as invoices_issued,
(SELECT COUNT(*) FROM invoices WHERE status='PAID') as paid_invoices,
(SELECT ROUND(SUM(amount),2) FROM payments) as total_collected""",
            'explanation':'Overall O2C pipeline summary.','highlighted_nodes':[]}

    return {'sql':"""SELECT so.sales_order_id,c.name as customer,so.status,
so.order_date,so.total_value FROM sales_orders so
JOIN customers c ON so.customer_id=c.customer_id ORDER BY so.order_date DESC LIMIT 15""",
        'explanation':'Recent sales orders.','highlighted_nodes':[]}

def execute_sql(sql: str):
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    c = conn.cursor(); c.execute(sql)
    cols = [d[0] for d in c.description] if c.description else []
    rows = [dict(r) for r in c.fetchall()]
    conn.close(); return cols, rows

def get_stats():
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    stats = {}
    for t in ['customers','sales_orders','deliveries','invoices','payments','materials']:
        c.execute(f'SELECT COUNT(*) FROM {t}'); stats[t] = c.fetchone()[0]
    conn.close(); return stats

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): print(f"  {self.address_string()} {fmt%args}")
    def send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status); self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(body))); self.send_header('Access-Control-Allow-Origin','*')
        self.end_headers(); self.wfile.write(body)
    def do_OPTIONS(self):
        self.send_response(204); self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Access-Control-Allow-Methods','GET,POST,OPTIONS')
        self.send_header('Access-Control-Allow-Headers','Content-Type,X-Groq-Key'); self.end_headers()
    def do_GET(self):
        path = urlparse(self.path).path
        if path == '/api/graph': return self.send_json(build_graph_json())
        if path == '/api/stats': return self.send_json(get_stats())
        if path == '/api/schema': return self.send_json({'schema':SCHEMA})
        if path in ('/','/index.html'):
            html_path = os.path.join(FRONTEND_DIR,'index.html')
            with open(html_path,'rb') as f: body=f.read()
            self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8')
            self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body)
        else: self.send_json({'error':'Not found'},404)
    def do_POST(self):
        if urlparse(self.path).path != '/api/query':
            return self.send_json({'error':'Not found'},404)
        length = int(self.headers.get('Content-Length',0))
        body = json.loads(self.rfile.read(length))
        question = body.get('question','').strip()
        groq_key = body.get('groq_key','') or self.headers.get('X-Groq-Key','') or GROQ_API_KEY

        if not question: return self.send_json({'error':'Empty question'},400)

        if is_off_topic(question):
            return self.send_json({'answer':'This system is designed to answer questions related to the provided dataset only.',
                'sql':None,'rows':[],'columns':[],'highlighted_nodes':[]})

        # Try LLM first, fall back to rule-based
        llm = {}
        if groq_key:
            llm = call_groq(question, groq_key)
            if llm.get('guardrail'):
                return self.send_json({'answer':'This system is designed to answer questions related to the provided dataset only.',
                    'sql':None,'rows':[],'columns':[],'highlighted_nodes':[]})

        if not llm.get('sql'):
            llm = fallback_sql(question)

        sql = llm.get('sql',''); explanation = llm.get('explanation',''); highlighted = llm.get('highlighted_nodes',[])

        try:
            cols, rows = execute_sql(sql)
            answer = f"{explanation}\n\n✅ Found {len(rows)} result(s)."
            return self.send_json({'answer':answer,'sql':sql,'columns':cols,'rows':rows[:100],'highlighted_nodes':highlighted})
        except Exception as e:
            return self.send_json({'answer':f'SQL error: {e}','sql':sql,'rows':[],'columns':[],'highlighted_nodes':[]})

if __name__ == '__main__':
    port = int(os.environ.get('PORT',8000))
    print(f"🚀  http://localhost:{port}")
    print(f"   GROQ_API_KEY: {'✓ set' if GROQ_API_KEY else 'not set (fallback mode)'}")
    HTTPServer(('0.0.0.0',port),Handler).serve_forever()
