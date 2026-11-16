// Assignment 3 — Neo4j Aura graph: constraints, load statements and all Cypher queries
// Load statements take a $rows parameter (a list of maps) and are run in batches by neo4j_graph.py.

CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (n:Customer) REQUIRE n.customer_id IS UNIQUE;
CREATE CONSTRAINT order_id IF NOT EXISTS FOR (n:Order) REQUIRE n.order_id IS UNIQUE;
CREATE CONSTRAINT restaurant_id IF NOT EXISTS FOR (n:Restaurant) REQUIRE n.restaurant_id IS UNIQUE;
CREATE CONSTRAINT dish_id IF NOT EXISTS FOR (n:Dish) REQUIRE n.item_id IS UNIQUE;
CREATE CONSTRAINT cuisine_name IF NOT EXISTS FOR (n:Cuisine) REQUIRE n.name IS UNIQUE;
CREATE CONSTRAINT zone_id IF NOT EXISTS FOR (n:Zone) REQUIRE n.zone_id IS UNIQUE;
CREATE CONSTRAINT driver_id IF NOT EXISTS FOR (n:Driver) REQUIRE n.driver_id IS UNIQUE;

// load: zones
UNWIND $rows AS r
MERGE (z:Zone {zone_id: r.zone_id}) SET z.name = r.name;

// load: cuisines
UNWIND $rows AS r
MERGE (c:Cuisine {name: r.name}) SET c.level = r.level
WITH c, r WHERE r.parent IS NOT NULL
MERGE (p:Cuisine {name: r.parent})
MERGE (c)-[:SUBCATEGORY_OF]->(p);

// load: restaurants
UNWIND $rows AS r
MERGE (x:Restaurant {restaurant_id: r.restaurant_id})
SET x.name = r.restaurant_name, x.kitchen_capacity = r.kitchen_capacity, x.cuisine = r.cuisine
WITH x, r
MATCH (z:Zone {zone_id: r.zone_id}) MERGE (x)-[:IN_ZONE]->(z)
WITH x, r
MATCH (c:Cuisine {name: r.cuisine}) MERGE (x)-[:OF_CUISINE]->(c);

// load: customers
UNWIND $rows AS r
MERGE (c:Customer {customer_id: r.customer_id})
SET c.name_masked = r.customer_name_masked, c.email_domain = r.email_domain, c.signup_date = date(r.signup_date)
WITH c, r WHERE r.zone_id IS NOT NULL
MATCH (z:Zone {zone_id: r.zone_id}) MERGE (c)-[:LIVES_IN]->(z);

// load: drivers
UNWIND $rows AS r
MERGE (d:Driver {driver_id: r.driver_id})
SET d.name_masked = r.driver_name_masked, d.vehicle_type = r.vehicle_type, d.rating = r.driver_rating;

// load: dishes
UNWIND $rows AS r
MERGE (d:Dish {item_id: r.item_id}) SET d.name = r.item_name, d.restaurant_id = r.restaurant_id
WITH d, r
MATCH (x:Restaurant {restaurant_id: r.restaurant_id}) MERGE (x)-[:SERVES]->(d);

// load: orders
UNWIND $rows AS r
MERGE (o:Order {order_id: r.order_id})
SET o.ordered_at = localdatetime(r.order_ts), o.status = r.order_status, o.food_revenue = r.food_revenue,
    o.item_count = r.item_count, o.delivery_min = r.delivery_min, o.is_late = r.is_late,
    o.weather = r.weather_condition, o.congestion = r.congestion_level, o.kitchen_queue = r.kitchen_queue_at_order
WITH o, r
MATCH (c:Customer {customer_id: r.customer_id}) MERGE (c)-[:PLACED]->(o)
WITH o, r
MATCH (x:Restaurant {restaurant_id: r.restaurant_id}) MERGE (o)-[:FROM]->(x)
WITH o, r WHERE r.driver_id IS NOT NULL
MATCH (d:Driver {driver_id: r.driver_id}) MERGE (o)-[:DELIVERED_BY]->(d);

// load: order_items
UNWIND $rows AS r
MATCH (o:Order {order_id: r.order_id}), (d:Dish {item_id: r.item_id})
MERGE (o)-[c:CONTAINS]->(d) SET c.quantity = r.quantity, c.unit_price = r.unit_price;

// load: ratings
UNWIND $rows AS r
MATCH (c:Customer {customer_id: r.customer_id}), (x:Restaurant {restaurant_id: r.restaurant_id})
MERGE (c)-[t:RATED]->(x) SET t.rating = r.rating, t.reviews = r.reviews;

// counts
CALL () { MATCH (n) RETURN labels(n)[0] AS name, count(*) AS n, 'node' AS kind
         UNION ALL MATCH ()-[r]->() RETURN type(r) AS name, count(*) AS n, 'relationship' AS kind }
RETURN kind, name, n ORDER BY kind, n DESC;

// parameter pickers (the script runs these first)
// $customer_id: MATCH (c:Customer)-[:PLACED]->(o:Order)
                     WITH c.customer_id AS v, count(o) AS n RETURN v ORDER BY n DESC, v LIMIT 1
// $rater_id: MATCH (c:Customer)-[r:RATED]->() WHERE r.rating >= 4
                  WITH c.customer_id AS v, count(r) AS n RETURN v ORDER BY n DESC, v LIMIT 1
// $dish_id: MATCH (:Restaurant {restaurant_id: 'R011'})-[:SERVES]->(d:Dish)<-[c:CONTAINS]-()
                 WITH d.item_id AS v, sum(c.quantity) AS n RETURN v ORDER BY n DESC, v LIMIT 1

// P3-Q1  Multi-hop: restaurants recommended to a customer by customers who share 2+ restaurants with them (Customer->Order->Restaurant<-Order<-Customer->Order->Restaurant)
MATCH (me:Customer {customer_id: $customer_id})-[:PLACED]->(:Order)-[:FROM]->(r:Restaurant)
      <-[:FROM]-(:Order)<-[:PLACED]-(peer:Customer)
WHERE peer <> me
WITH me, peer, count(DISTINCT r) AS shared_restaurants
WHERE shared_restaurants >= 2
MATCH (peer)-[:PLACED]->(:Order)-[:FROM]->(rec:Restaurant)-[:IN_ZONE]->(z:Zone)
WHERE NOT EXISTS { (me)-[:PLACED]->(:Order)-[:FROM]->(rec) }
RETURN rec.name AS recommended_restaurant, z.name AS zone,
       count(DISTINCT peer) AS similar_customers, sum(shared_restaurants) AS score
ORDER BY score DESC, similar_customers DESC LIMIT 5;

// P3-Q2  Shared customers: which restaurants share the most customers with Thai Garden?
MATCH (src:Restaurant {restaurant_id: 'R011'})<-[:FROM]-(:Order)<-[:PLACED]-(c:Customer)
      -[:PLACED]->(:Order)-[:FROM]->(other:Restaurant)
WHERE other <> src
RETURN other.name AS restaurant, other.cuisine AS cuisine, count(DISTINCT c) AS shared_customers
ORDER BY shared_customers DESC LIMIT 5;

// P3-Q3  Hierarchy traversal: delivered orders and revenue for every cuisine under 'Asian' (variable-length path)
MATCH (top:Cuisine {name: 'Asian'})<-[:SUBCATEGORY_OF*0..]-(c:Cuisine)<-[:OF_CUISINE]-(r:Restaurant)
      <-[:FROM]-(o:Order {status: 'delivered'})
RETURN c.name AS cuisine, count(DISTINCT r) AS restaurants, count(o) AS delivered_orders,
       round(sum(o.food_revenue), 2) AS food_revenue
ORDER BY food_revenue DESC;

// S1  Personalized recommendation: restaurants rated 4+ by customers who rated the same restaurants 4+ as this customer, not yet tried by them
MATCH (me:Customer {customer_id: $rater_id})-[mine:RATED]->(:Restaurant)<-[theirs:RATED]-(peer:Customer)
WHERE mine.rating >= 4 AND theirs.rating >= 4 AND peer <> me
MATCH (peer)-[liked:RATED]->(rec:Restaurant)-[:IN_ZONE]->(z:Zone)
WHERE liked.rating >= 4
  AND NOT EXISTS { (me)-[:RATED]->(rec) }
  AND NOT EXISTS { (me)-[:PLACED]->(:Order)-[:FROM]->(rec) }
RETURN rec.name AS recommended_restaurant, rec.cuisine AS cuisine, z.name AS zone,
       count(DISTINCT peer) AS liked_by_similar_customers, round(avg(liked.rating), 2) AS avg_rating
ORDER BY liked_by_similar_customers DESC, avg_rating DESC LIMIT 5;

// S2  Cross-restaurant dish recommendation: customers who ordered this dish also ordered which dishes elsewhere?
MATCH (d:Dish {item_id: $dish_id})<-[:CONTAINS]-(:Order)<-[:PLACED]-(c:Customer)
      -[:PLACED]->(:Order)-[:CONTAINS]->(other:Dish)<-[:SERVES]-(r:Restaurant)
WHERE other.restaurant_id <> d.restaurant_id
RETURN d.name AS because_you_ordered, other.name AS also_ordered, r.name AS at_restaurant,
       count(DISTINCT c) AS customers
ORDER BY customers DESC LIMIT 5;

// S3  Capacity-aware substitute: when Thai Garden is overloaded, which restaurants in the same cuisine family share its customers and deliver on time more often?
MATCH (src:Restaurant {restaurant_id: 'R011'})-[:OF_CUISINE]->(:Cuisine)-[:SUBCATEGORY_OF*0..]->(family:Cuisine)
WHERE NOT EXISTS { (family)-[:SUBCATEGORY_OF]->() }
MATCH (family)<-[:SUBCATEGORY_OF*0..]-(:Cuisine)<-[:OF_CUISINE]-(alt:Restaurant)-[:IN_ZONE]->(z:Zone)
WHERE alt <> src
OPTIONAL MATCH (src)<-[:FROM]-(:Order)<-[:PLACED]-(c:Customer)-[:PLACED]->(:Order)-[:FROM]->(alt)
WITH family, alt, z, count(DISTINCT c) AS shared_customers
MATCH (alt)<-[:FROM]-(o:Order {status: 'delivered'})
RETURN family.name AS family, alt.name AS substitute, alt.cuisine AS cuisine, z.name AS zone,
       alt.kitchen_capacity AS kitchen_capacity, shared_customers,
       round(100.0 * avg(o.is_late), 1) AS late_rate_pct
ORDER BY shared_customers DESC, late_rate_pct ASC LIMIT 5;
