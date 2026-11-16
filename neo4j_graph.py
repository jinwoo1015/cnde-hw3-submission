"""
Assignment 3 — Part 3 & 4b: Neo4j Aura property graph for restaurant recommendations
Domain: Kitchen-Aware Restaurant Ranking (Chicago food delivery)

Graph (built from the PII-masked Silver layer, data/silver/*.csv)
  Nodes          :Customer :Order :Restaurant :Dish :Cuisine :Zone :Driver
  Relationships  (Customer)-[:PLACED]->(Order)            (Order)-[:FROM]->(Restaurant)
                 (Order)-[:CONTAINS {quantity, unit_price}]->(Dish)
                 (Order)-[:DELIVERED_BY]->(Driver)          (Restaurant)-[:SERVES]->(Dish)
                 (Restaurant)-[:IN_ZONE]->(Zone)            (Customer)-[:LIVES_IN]->(Zone)
                 (Restaurant)-[:OF_CUISINE]->(Cuisine)      (Cuisine)-[:SUBCATEGORY_OF]->(Cuisine)
                 (Customer)-[:RATED {rating, reviews}]->(Restaurant)

Commands (run on your Mac from the HW3 folder)
  python3 neo4j_graph.py load        create constraints and load the graph (MERGE, so re-runs are safe)
  python3 neo4j_graph.py counts      node / relationship counts
  python3 neo4j_graph.py part3       the 3 Part 3 Cypher queries (incl. multi-hop traversal)
  python3 neo4j_graph.py showcase    the 3 Part 4b recommendation queries
  python3 neo4j_graph.py cypher      (re)write graph.cypher with every statement and query
Connection: export NEO4J_URI, NEO4J_USERNAME (and NEO4J_DATABASE if the Aura credentials file lists one)
            from the credentials file Aura gives you; the password is prompted and never stored.
Requirements: pip install neo4j pandas
"""
import getpass
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATABASE = os.environ.get("NEO4J_DATABASE") or None      # None = the user's home database (newer Aura: the instance id)
SILVER = os.path.join(HERE, "data", "silver")
ZONES = {"Z01": "Hyde Park", "Z02": "Loop", "Z03": "South Loop", "Z04": "West Loop",
         "Z05": "Lincoln Park", "Z06": "Wicker Park", "Z07": "Logan Square", "Z08": "Pilsen"}

CONSTRAINTS = [
    "CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (n:Customer) REQUIRE n.customer_id IS UNIQUE",
    "CREATE CONSTRAINT order_id IF NOT EXISTS FOR (n:Order) REQUIRE n.order_id IS UNIQUE",
    "CREATE CONSTRAINT restaurant_id IF NOT EXISTS FOR (n:Restaurant) REQUIRE n.restaurant_id IS UNIQUE",
    "CREATE CONSTRAINT dish_id IF NOT EXISTS FOR (n:Dish) REQUIRE n.item_id IS UNIQUE",
    "CREATE CONSTRAINT cuisine_name IF NOT EXISTS FOR (n:Cuisine) REQUIRE n.name IS UNIQUE",
    "CREATE CONSTRAINT zone_id IF NOT EXISTS FOR (n:Zone) REQUIRE n.zone_id IS UNIQUE",
    "CREATE CONSTRAINT driver_id IF NOT EXISTS FOR (n:Driver) REQUIRE n.driver_id IS UNIQUE",
]

LOAD = {
    "zones": """UNWIND $rows AS r
MERGE (z:Zone {zone_id: r.zone_id}) SET z.name = r.name""",
    "cuisines": """UNWIND $rows AS r
MERGE (c:Cuisine {name: r.name}) SET c.level = r.level
WITH c, r WHERE r.parent IS NOT NULL
MERGE (p:Cuisine {name: r.parent})
MERGE (c)-[:SUBCATEGORY_OF]->(p)""",
    "restaurants": """UNWIND $rows AS r
MERGE (x:Restaurant {restaurant_id: r.restaurant_id})
SET x.name = r.restaurant_name, x.kitchen_capacity = r.kitchen_capacity, x.cuisine = r.cuisine
WITH x, r
MATCH (z:Zone {zone_id: r.zone_id}) MERGE (x)-[:IN_ZONE]->(z)
WITH x, r
MATCH (c:Cuisine {name: r.cuisine}) MERGE (x)-[:OF_CUISINE]->(c)""",
    "customers": """UNWIND $rows AS r
MERGE (c:Customer {customer_id: r.customer_id})
SET c.name_masked = r.customer_name_masked, c.email_domain = r.email_domain, c.signup_date = date(r.signup_date)
WITH c, r WHERE r.zone_id IS NOT NULL
MATCH (z:Zone {zone_id: r.zone_id}) MERGE (c)-[:LIVES_IN]->(z)""",
    "drivers": """UNWIND $rows AS r
MERGE (d:Driver {driver_id: r.driver_id})
SET d.name_masked = r.driver_name_masked, d.vehicle_type = r.vehicle_type, d.rating = r.driver_rating""",
    "dishes": """UNWIND $rows AS r
MERGE (d:Dish {item_id: r.item_id}) SET d.name = r.item_name, d.restaurant_id = r.restaurant_id
WITH d, r
MATCH (x:Restaurant {restaurant_id: r.restaurant_id}) MERGE (x)-[:SERVES]->(d)""",
    "orders": """UNWIND $rows AS r
MERGE (o:Order {order_id: r.order_id})
SET o.ordered_at = localdatetime(r.order_ts), o.status = r.order_status, o.food_revenue = r.food_revenue,
    o.item_count = r.item_count, o.delivery_min = r.delivery_min, o.is_late = r.is_late,
    o.weather = r.weather_condition, o.congestion = r.congestion_level, o.kitchen_queue = r.kitchen_queue_at_order
WITH o, r
MATCH (c:Customer {customer_id: r.customer_id}) MERGE (c)-[:PLACED]->(o)
WITH o, r
MATCH (x:Restaurant {restaurant_id: r.restaurant_id}) MERGE (o)-[:FROM]->(x)
WITH o, r WHERE r.driver_id IS NOT NULL
MATCH (d:Driver {driver_id: r.driver_id}) MERGE (o)-[:DELIVERED_BY]->(d)""",
    "order_items": """UNWIND $rows AS r
MATCH (o:Order {order_id: r.order_id}), (d:Dish {item_id: r.item_id})
MERGE (o)-[c:CONTAINS]->(d) SET c.quantity = r.quantity, c.unit_price = r.unit_price""",
    "ratings": """UNWIND $rows AS r
MATCH (c:Customer {customer_id: r.customer_id}), (x:Restaurant {restaurant_id: r.restaurant_id})
MERGE (c)-[t:RATED]->(x) SET t.rating = r.rating, t.reviews = r.reviews""",
}

COUNTS = """CALL () { MATCH (n) RETURN labels(n)[0] AS name, count(*) AS n, 'node' AS kind
         UNION ALL MATCH ()-[r]->() RETURN type(r) AS name, count(*) AS n, 'relationship' AS kind }
RETURN kind, name, n ORDER BY kind, n DESC"""

PART3 = [
    ("P3-Q1  Multi-hop: restaurants recommended to a customer by customers who share 2+ restaurants with them "
     "(Customer->Order->Restaurant<-Order<-Customer->Order->Restaurant)",
     """MATCH (me:Customer {customer_id: $customer_id})-[:PLACED]->(:Order)-[:FROM]->(r:Restaurant)
      <-[:FROM]-(:Order)<-[:PLACED]-(peer:Customer)
WHERE peer <> me
WITH me, peer, count(DISTINCT r) AS shared_restaurants
WHERE shared_restaurants >= 2
MATCH (peer)-[:PLACED]->(:Order)-[:FROM]->(rec:Restaurant)-[:IN_ZONE]->(z:Zone)
WHERE NOT EXISTS { (me)-[:PLACED]->(:Order)-[:FROM]->(rec) }
RETURN rec.name AS recommended_restaurant, z.name AS zone,
       count(DISTINCT peer) AS similar_customers, sum(shared_restaurants) AS score
ORDER BY score DESC, similar_customers DESC LIMIT 5"""),
    ("P3-Q2  Shared customers: which restaurants share the most customers with Thai Garden?",
     """MATCH (src:Restaurant {restaurant_id: 'R011'})<-[:FROM]-(:Order)<-[:PLACED]-(c:Customer)
      -[:PLACED]->(:Order)-[:FROM]->(other:Restaurant)
WHERE other <> src
RETURN other.name AS restaurant, other.cuisine AS cuisine, count(DISTINCT c) AS shared_customers
ORDER BY shared_customers DESC LIMIT 5"""),
    ("P3-Q3  Hierarchy traversal: delivered orders and revenue for every cuisine under 'Asian' (variable-length path)",
     """MATCH (top:Cuisine {name: 'Asian'})<-[:SUBCATEGORY_OF*0..]-(c:Cuisine)<-[:OF_CUISINE]-(r:Restaurant)
      <-[:FROM]-(o:Order {status: 'delivered'})
RETURN c.name AS cuisine, count(DISTINCT r) AS restaurants, count(o) AS delivered_orders,
       round(sum(o.food_revenue), 2) AS food_revenue
ORDER BY food_revenue DESC"""),
]

SHOWCASE = [
    ("S1  Personalized recommendation: restaurants rated 4+ by customers who rated the same restaurants 4+ "
     "as this customer, not yet tried by them",
     """MATCH (me:Customer {customer_id: $rater_id})-[mine:RATED]->(:Restaurant)<-[theirs:RATED]-(peer:Customer)
WHERE mine.rating >= 4 AND theirs.rating >= 4 AND peer <> me
MATCH (peer)-[liked:RATED]->(rec:Restaurant)-[:IN_ZONE]->(z:Zone)
WHERE liked.rating >= 4
  AND NOT EXISTS { (me)-[:RATED]->(rec) }
  AND NOT EXISTS { (me)-[:PLACED]->(:Order)-[:FROM]->(rec) }
RETURN rec.name AS recommended_restaurant, rec.cuisine AS cuisine, z.name AS zone,
       count(DISTINCT peer) AS liked_by_similar_customers, round(avg(liked.rating), 2) AS avg_rating
ORDER BY liked_by_similar_customers DESC, avg_rating DESC LIMIT 5"""),
    ("S2  Cross-restaurant dish recommendation: customers who ordered this dish also ordered which dishes elsewhere?",
     """MATCH (d:Dish {item_id: $dish_id})<-[:CONTAINS]-(:Order)<-[:PLACED]-(c:Customer)
      -[:PLACED]->(:Order)-[:CONTAINS]->(other:Dish)<-[:SERVES]-(r:Restaurant)
WHERE other.restaurant_id <> d.restaurant_id
RETURN d.name AS because_you_ordered, other.name AS also_ordered, r.name AS at_restaurant,
       count(DISTINCT c) AS customers
ORDER BY customers DESC LIMIT 5"""),
    ("S3  Capacity-aware substitute: when Thai Garden is overloaded, which restaurants in the same cuisine family "
     "share its customers and deliver on time more often?",
     """MATCH (src:Restaurant {restaurant_id: 'R011'})-[:OF_CUISINE]->(:Cuisine)-[:SUBCATEGORY_OF*0..]->(family:Cuisine)
WHERE NOT EXISTS { (family)-[:SUBCATEGORY_OF]->() }
MATCH (family)<-[:SUBCATEGORY_OF*0..]-(:Cuisine)<-[:OF_CUISINE]-(alt:Restaurant)-[:IN_ZONE]->(z:Zone)
WHERE alt <> src
OPTIONAL MATCH (src)<-[:FROM]-(:Order)<-[:PLACED]-(c:Customer)-[:PLACED]->(:Order)-[:FROM]->(alt)
WITH family, alt, z, count(DISTINCT c) AS shared_customers
MATCH (alt)<-[:FROM]-(o:Order {status: 'delivered'})
RETURN family.name AS family, alt.name AS substitute, alt.cuisine AS cuisine, z.name AS zone,
       alt.kitchen_capacity AS kitchen_capacity, shared_customers,
       round(100.0 * avg(o.is_late), 1) AS late_rate_pct
ORDER BY shared_customers DESC, late_rate_pct ASC LIMIT 5"""),
]

PICK_PARAMS = {
    "customer_id": """MATCH (c:Customer)-[:PLACED]->(o:Order)
                     WITH c.customer_id AS v, count(o) AS n RETURN v ORDER BY n DESC, v LIMIT 1""",
    "rater_id": """MATCH (c:Customer)-[r:RATED]->() WHERE r.rating >= 4
                  WITH c.customer_id AS v, count(r) AS n RETURN v ORDER BY n DESC, v LIMIT 1""",
    "dish_id": """MATCH (:Restaurant {restaurant_id: 'R011'})-[:SERVES]->(d:Dish)<-[c:CONTAINS]-()
                 WITH d.item_id AS v, sum(c.quantity) AS n RETURN v ORDER BY n DESC, v LIMIT 1""",
}


# ----------------------------------------------------------------------------- data from Silver
def rows(df):
    return [{k: (None if pd.isna(v) else (v.item() if hasattr(v, "item") else v)) for k, v in r.items()} for r in df.to_dict("records")]


def silver_frames():
    rd = lambda n: pd.read_csv(os.path.join(SILVER, f"{n}.csv"))
    rest, cust, drv, orders, items = rd("silver_restaurants"), rd("silver_customers"), rd("silver_drivers"), rd("silver_orders"), rd("silver_order_items")
    pairs = {}
    for path in rest.cuisine_path:
        parts = [p.strip() for p in path.split(">")]
        for i, p in enumerate(parts):
            pairs[p] = {"name": p, "parent": parts[i - 1] if i else None, "level": i}
    cuisines = sorted(pairs.values(), key=lambda r: r["level"])
    orders["order_ts"] = orders.order_ts.str.replace(" ", "T")
    dish = items.merge(orders[["order_id", "restaurant_id"]], on="order_id").drop_duplicates("item_id")[["item_id", "item_name", "restaurant_id"]]
    rated = (orders.dropna(subset=["order_rating"]).groupby(["customer_id", "restaurant_id"])
             .order_rating.agg(rating="mean", reviews="size").reset_index())
    rated["rating"] = rated.rating.round(2)
    return {
        "zones": [{"zone_id": k, "name": v} for k, v in ZONES.items()],
        "cuisines": cuisines,
        "restaurants": rows(rest),
        "customers": rows(cust),
        "drivers": rows(drv),
        "dishes": rows(dish),
        "orders": rows(orders),
        "order_items": rows(items),
        "ratings": rows(rated),
    }


# ----------------------------------------------------------------------------- Neo4j
def aura_credentials():
    """Read KEY=VALUE lines from the Aura credentials file (Neo4j-*.txt) in this folder, if present.
    The file stays on this machine; it must NOT be submitted (it contains the password)."""
    import glob
    path = os.environ.get("NEO4J_CREDENTIALS_FILE") or next(iter(sorted(glob.glob(os.path.join(HERE, "Neo4j-*.txt")))), None)
    creds = {}
    if path and os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                creds[k.strip()] = v.strip()
        print(f"using credentials from {os.path.basename(path)} (keep this file out of your submission)")
    return creds


def driver():
    global DATABASE
    from neo4j import GraphDatabase
    creds = aura_credentials()
    uri = os.environ.get("NEO4J_URI") or creds.get("NEO4J_URI") or input("NEO4J_URI (neo4j+s://...): ").strip()
    user = os.environ.get("NEO4J_USERNAME") or creds.get("NEO4J_USERNAME") or "neo4j"
    pwd = os.environ.get("NEO4J_PASSWORD") or creds.get("NEO4J_PASSWORD") or getpass.getpass("Neo4j password: ")
    DATABASE = DATABASE or creds.get("NEO4J_DATABASE") or None
    d = GraphDatabase.driver(uri, auth=(user, pwd))
    d.verify_connectivity()
    return d


def show(title, records, keys, out):
    rows_ = [[("NULL" if r[k] is None else str(r[k])) for k in keys] for r in records]
    w = [max([len(k)] + [len(x[i]) for x in rows_]) for i, k in enumerate(keys)]
    line = lambda v: " | ".join(s.ljust(w[i]) for i, s in enumerate(v))
    block = f"\n=== {title}  ({len(rows_)} rows)\n" + "\n".join([line(keys), "-+-".join("-" * x for x in w)] + [line(x) for x in rows_])
    print(block); out.append(block)


def run_queries(d, queries, out):
    params = {}
    for name, q in PICK_PARAMS.items():
        recs, _, _ = d.execute_query(q, database_=DATABASE)
        params[name] = recs[0]["v"] if recs else None
    out.append("parameters: " + ", ".join(f"${k} = {v}" for k, v in params.items()))
    print(out[-1])
    for title, q in queries:
        recs, summary, keys = d.execute_query(q, params, database_=DATABASE)
        show(title, recs, keys, out)
        out.append(f"(server time: {summary.result_available_after} ms to first row)")


def write_cypher():
    parts = ["// Assignment 3 — Neo4j Aura graph: constraints, load statements and all Cypher queries",
             "// Load statements take a $rows parameter (a list of maps) and are run in batches by neo4j_graph.py.", ""]
    parts += [c + ";" for c in CONSTRAINTS] + [""]
    for name, q in LOAD.items():
        parts += [f"// load: {name}", q + ";", ""]
    parts += ["// counts", COUNTS + ";", ""]
    parts += ["// parameter pickers (the script runs these first)"] + [f"// ${k}: {q.strip()}" for k, q in PICK_PARAMS.items()] + [""]
    for title, q in PART3 + SHOWCASE:
        parts += [f"// {title}", q + ";", ""]
    with open(os.path.join(HERE, "graph.cypher"), "w", encoding="utf-8") as f:
        f.write("\n".join(parts))
    print("wrote graph.cypher")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "cypher":
        write_cypher(); return
    if cmd not in ("load", "counts", "part3", "showcase"):
        sys.exit(__doc__)
    out = []
    with driver() as d:
        if cmd == "load":
            for c in CONSTRAINTS:
                d.execute_query(c, database_=DATABASE)
            for name, data in silver_frames().items():
                for i in range(0, len(data), 1000):
                    d.execute_query(LOAD[name], {"rows": data[i:i + 1000]}, database_=DATABASE)
                print(f"loaded {name:<12} {len(data):>6,} rows")
            cmd = "counts"
        if cmd == "counts":
            recs, _, keys = d.execute_query(COUNTS, database_=DATABASE)
            show("graph counts", recs, keys, out)
        elif cmd == "part3":
            run_queries(d, PART3, out)
        elif cmd == "showcase":
            run_queries(d, SHOWCASE, out)
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    with open(os.path.join(HERE, "results", f"NEO4J_{cmd.upper()}.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    print(f"\nsaved results/NEO4J_{cmd.upper()}.txt")


if __name__ == "__main__":
    main()
