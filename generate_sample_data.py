"""Build NetworkX graph from SQLite data, export as JSON for frontend."""
import sqlite3, json, os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'otc.db')

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def build_graph_json():
    conn = get_conn()
    c = conn.cursor()
    nodes = {}
    edges = []

    def add_node(nid, label, node_type, props=None):
        nodes[nid] = {'id': nid, 'label': label, 'type': node_type, 'props': props or {}}

    def add_edge(src, tgt, rel):
        edges.append({'source': src, 'target': tgt, 'relation': rel})

    # Customers
    for r in c.execute("SELECT * FROM customers"):
        nid = r['customer_id']
        add_node(nid, r['name'], 'customer', {
            'region': r['region'], 'credit_limit': r['credit_limit']
        })

    # Plants
    for r in c.execute("SELECT * FROM plants"):
        add_node(r['plant_id'], r['plant_name'], 'plant', {'location': r['location']})

    # Materials (top 20 used)
    for r in c.execute("""
        SELECT m.*, COUNT(soi.item_id) as usage_count
        FROM materials m
        LEFT JOIN sales_order_items soi ON m.material_id = soi.material_id
        GROUP BY m.material_id ORDER BY usage_count DESC LIMIT 20
    """):
        add_node(r['material_id'], r['description'], 'material', {
            'group': r['material_group'], 'unit_price': r['unit_price'],
            'usage_count': r['usage_count']
        })

    # Sales Orders
    for r in c.execute("SELECT * FROM sales_orders LIMIT 80"):
        nid = r['sales_order_id']
        add_node(nid, nid, 'sales_order', {
            'status': r['status'], 'order_date': r['order_date'],
            'total_value': r['total_value'], 'customer_id': r['customer_id']
        })
        if r['customer_id'] in nodes:
            add_edge(r['customer_id'], nid, 'PLACED')

    # Sales Order Items → Materials
    for r in c.execute("""
        SELECT soi.*, so.sales_order_id
        FROM sales_order_items soi
        JOIN sales_orders so ON soi.sales_order_id = so.sales_order_id
        LIMIT 200
    """):
        if r['sales_order_id'] in nodes and r['material_id'] in nodes:
            add_edge(r['sales_order_id'], r['material_id'], 'INCLUDES')

    # Deliveries
    for r in c.execute("SELECT * FROM deliveries LIMIT 80"):
        nid = r['delivery_id']
        add_node(nid, nid, 'delivery', {
            'status': r['status'], 'carrier': r['carrier'],
            'ship_date': r['ship_date'], 'actual_delivery_date': r['actual_delivery_date']
        })
        if r['sales_order_id'] in nodes:
            add_edge(r['sales_order_id'], nid, 'TRIGGERS')
        if r['plant_id'] in nodes:
            add_edge(nid, r['plant_id'], 'SHIPS_FROM')

    # Invoices
    for r in c.execute("SELECT * FROM invoices LIMIT 80"):
        nid = r['invoice_id']
        add_node(nid, nid, 'invoice', {
            'status': r['status'], 'amount': r['amount'],
            'invoice_date': r['invoice_date'], 'due_date': r['due_date']
        })
        if r['sales_order_id'] in nodes:
            add_edge(r['sales_order_id'], nid, 'BILLED_AS')
        if r['customer_id'] in nodes:
            add_edge(r['customer_id'], nid, 'OWES')

    # Payments
    for r in c.execute("SELECT * FROM payments LIMIT 60"):
        nid = r['payment_id']
        add_node(nid, nid, 'payment', {
            'amount': r['amount'], 'method': r['method'],
            'payment_date': r['payment_date']
        })
        if r['invoice_id'] in nodes:
            add_edge(r['invoice_id'], nid, 'SETTLED_BY')

    conn.close()
    return {'nodes': list(nodes.values()), 'edges': edges}


# ── Schema export for LLM prompting ──────────────────────────────────────────
SCHEMA = """
Tables in the SAP Order-to-Cash SQLite database:

customers(customer_id, name, region, credit_limit, created_date)
addresses(address_id, customer_id, street, city, state, zip, address_type)
plants(plant_id, plant_name, location)
materials(material_id, description, material_group, unit_price, weight_kg)
sales_orders(sales_order_id, customer_id, order_date, requested_delivery_date, sales_org, distribution_channel, status, total_value)
  -- status values: COMPLETE, PARTIAL, OPEN, CANCELLED
sales_order_items(item_id, sales_order_id, item_number, material_id, quantity, unit_price, net_value, plant_id)
deliveries(delivery_id, sales_order_id, ship_date, actual_delivery_date, plant_id, carrier, status)
  -- status values: DELIVERED, IN_TRANSIT, PENDING
delivery_items(delivery_item_id, delivery_id, sales_order_item_id, material_id, quantity_delivered)
invoices(invoice_id, sales_order_id, customer_id, invoice_date, due_date, amount, currency, status)
  -- status values: PAID, OPEN, OVERDUE, PARTIAL
payments(payment_id, invoice_id, customer_id, payment_date, amount, method, reference)
  -- method values: BANK_TRANSFER, CHECK, CREDIT_CARD, ACH

Key relationships:
- customers.customer_id → sales_orders.customer_id
- sales_orders.sales_order_id → sales_order_items.sales_order_id
- sales_orders.sales_order_id → deliveries.sales_order_id
- sales_orders.sales_order_id → invoices.sales_order_id
- sales_order_items.material_id → materials.material_id
- deliveries.plant_id → plants.plant_id
- invoices.invoice_id → payments.invoice_id
""".strip()
