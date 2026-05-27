"""
Execute SQL statements against Databricks SQL warehouse via REST API.
Generates synthetic retail grocery data.

Usage:
    python execute_sql.py --profile DEFAULT --warehouse-id <id> --catalog <catalog> --schema <schema>
"""

import argparse
import json
import random
import subprocess
import sys
import time
from datetime import datetime, timedelta

random.seed(42)

FULL_SCHEMA = ""


def get_token(profile: str) -> str:
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", profile, "--output", "json"],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        print(f"ERROR: Failed to get auth token for profile '{profile}'.", file=sys.stderr)
        if result.stderr:
            print(f"  {result.stderr.strip()}", file=sys.stderr)
        print(f"\nFix: Run 'databricks auth login --profile {profile}' to authenticate.", file=sys.stderr)
        sys.exit(1)
    try:
        return json.loads(result.stdout)["access_token"]
    except (json.JSONDecodeError, KeyError):
        print(f"ERROR: Unexpected response from 'databricks auth token':", file=sys.stderr)
        print(f"  {result.stdout[:200]}", file=sys.stderr)
        print(f"\nFix: Run 'databricks auth login --profile {profile}' to re-authenticate.", file=sys.stderr)
        sys.exit(1)


def get_host(profile: str) -> str:
    result = subprocess.run(
        ["databricks", "auth", "env", "--profile", profile, "--output", "json"],
        capture_output=True, text=True,
    )
    if result.returncode == 0 and result.stdout.strip():
        try:
            env_data = json.loads(result.stdout)
            return env_data.get("env", {}).get("DATABRICKS_HOST", "").rstrip("/")
        except (json.JSONDecodeError, KeyError):
            pass

    # Fallback: try profiles list
    result = subprocess.run(
        ["databricks", "auth", "profiles", "--output", "json"],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        print(f"ERROR: Could not determine workspace host for profile '{profile}'.", file=sys.stderr)
        print(f"Fix: Run 'databricks auth login --profile {profile}'.", file=sys.stderr)
        sys.exit(1)
    try:
        data = json.loads(result.stdout)
        profiles = data.get("profiles", data) if isinstance(data, dict) else data
        for p in profiles:
            if p.get("name") == profile:
                return p.get("host", "").rstrip("/")
    except (json.JSONDecodeError, KeyError):
        pass
    print(f"ERROR: Profile '{profile}' not found. Available profiles:", file=sys.stderr)
    print(f"  Run 'databricks auth profiles' to see available profiles.", file=sys.stderr)
    sys.exit(1)


def run_sql(statement: str, token: str, host: str, warehouse_id: str) -> dict:
    """Execute SQL via REST API with polling for long queries."""
    import urllib.request
    import urllib.error

    payload = json.dumps({
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "50s",
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{host}/api/2.0/sql/statements",
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        print(f"  HTTP {e.code}: {body[:500]}", file=sys.stderr)
        return {"status": {"state": "FAILED"}}
    except Exception as e:
        print(f"  Error: {e}", file=sys.stderr)
        return {"status": {"state": "FAILED"}}

    # Poll if still pending
    state = data.get("status", {}).get("state", "")
    stmt_id = data.get("statement_id", "")
    poll_count = 0
    while state == "PENDING" and stmt_id and poll_count < 30:
        time.sleep(2)
        poll_count += 1
        poll_req = urllib.request.Request(
            f"{host}/api/2.0/sql/statements/{stmt_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(poll_req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                state = data.get("status", {}).get("state", "")
        except Exception:
            break

    return data


def exec_sql(statement: str, token: str, host: str, wid: str, label: str = "") -> str:
    data = run_sql(statement, token, host, wid)
    state = data.get("status", {}).get("state", "UNKNOWN")
    err = data.get("status", {}).get("error", {}).get("message", "")
    if label:
        status_msg = f"{state}" + (f" - {err}" if err else "")
        print(f"  {label}: {status_msg}")
    return state


def esc(s):
    return s.replace("'", "''")


# ── Domain data (abbreviated for clarity) ───────────────────────────
FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Kenneth", "Carol", "Kevin", "Amanda", "Brian", "Dorothy", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
    "Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
    "Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory", "Debra",
    "Frank", "Rachel", "Alexander", "Carolyn", "Patrick", "Janet", "Jack", "Catherine",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts",
]
STREETS = [
    "Main St", "Oak Ave", "Elm St", "Park Blvd", "Cedar Ln", "Maple Dr", "Pine St",
    "Washington Ave", "Lake Rd", "Hill St", "Forest Dr", "River Rd", "Church St",
    "Spring St", "Meadow Ln", "Sunset Blvd", "Valley Rd", "Garden Way", "Market St",
    "Highland Ave",
]
CITIES_STATES = [
    ("Portland", "OR"), ("Seattle", "WA"), ("San Francisco", "CA"), ("Los Angeles", "CA"),
    ("Denver", "CO"), ("Austin", "TX"), ("Chicago", "IL"), ("Boston", "MA"),
    ("New York", "NY"), ("Atlanta", "GA"), ("Miami", "FL"), ("Phoenix", "AZ"),
    ("Minneapolis", "MN"), ("Nashville", "TN"), ("Salt Lake City", "UT"),
]
MEMBERSHIP_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
DIETARY_PREFS = ["vegetarian", "vegan", "gluten-free", "keto", "paleo", "dairy-free", "nut-free", "none"]
FAVORITE_CATEGORIES = ["Produce", "Dairy", "Bakery", "Meat & Seafood", "Frozen", "Snacks", "Beverages", "Deli", "Organic"]
PAYMENT_METHODS = ["credit_card", "debit_card", "cash", "mobile_pay", "gift_card"]

PRODUCTS_BY_CATEGORY = {
    "Produce": [
        ("Organic Bananas", "Dole", 0.79, "bunch"), ("Red Apples", "Honeycrisp", 2.49, "lb"),
        ("Baby Spinach", "Earthbound", 3.99, "bag"), ("Avocados", "Hass", 1.49, "each"),
        ("Roma Tomatoes", "Local Farm", 1.99, "lb"), ("Blueberries", "Driscolls", 4.99, "pint"),
        ("Sweet Potatoes", "Local Farm", 1.29, "lb"), ("Broccoli Crowns", "Green Giant", 2.49, "lb"),
        ("Strawberries", "Driscolls", 5.99, "pack"), ("Lemons", "Sunkist", 0.69, "each"),
        ("Cucumbers", "English", 1.79, "each"), ("Bell Peppers", "Local Farm", 1.49, "each"),
        ("Carrots", "Bolthouse", 1.99, "bag"), ("Russet Potatoes", "Idaho", 4.99, "5lb bag"),
        ("Mixed Greens", "Taylor Farms", 4.49, "bag"), ("Grapes Red Seedless", "Chile", 3.49, "lb"),
    ],
    "Dairy": [
        ("Whole Milk", "Horizon Organic", 5.49, "gallon"), ("2% Milk", "Darigold", 4.29, "gallon"),
        ("Greek Yogurt Plain", "Chobani", 5.99, "32oz"), ("Cheddar Cheese Block", "Tillamook", 5.99, "8oz"),
        ("Butter Unsalted", "Kerrygold", 4.99, "8oz"), ("Heavy Cream", "Darigold", 3.99, "pint"),
        ("Sour Cream", "Daisy", 2.49, "16oz"), ("Cream Cheese", "Philadelphia", 3.29, "8oz"),
        ("Shredded Mozzarella", "Galbani", 4.49, "8oz"), ("Almond Milk", "Silk", 3.99, "half gallon"),
        ("Oat Milk", "Oatly", 4.49, "half gallon"), ("Cottage Cheese", "Daisy", 3.99, "16oz"),
        ("Parmesan Wedge", "BelGioioso", 6.99, "5oz"), ("Egg Dozen Large", "Pete and Gerrys", 5.49, "dozen"),
    ],
    "Bakery": [
        ("Sourdough Bread", "Bakery Fresh", 4.99, "loaf"), ("Whole Wheat Bread", "Daves Killer", 5.49, "loaf"),
        ("Croissants", "Bakery Fresh", 3.99, "4 pack"), ("Bagels Everything", "Bakery Fresh", 4.49, "6 pack"),
        ("Baguette", "Bakery Fresh", 2.99, "each"), ("Cinnamon Rolls", "Bakery Fresh", 5.99, "4 pack"),
        ("Tortillas Flour", "Mission", 3.49, "10 pack"), ("Hamburger Buns", "Bakery Fresh", 3.99, "8 pack"),
    ],
    "Meat & Seafood": [
        ("Chicken Breast Boneless", "Foster Farms", 6.99, "lb"), ("Ground Beef 80/20", "Angus", 5.99, "lb"),
        ("Atlantic Salmon Fillet", "Fresh Catch", 12.99, "lb"), ("Pork Chops", "Smithfield", 4.99, "lb"),
        ("Bacon Thick Cut", "Applegate", 7.99, "12oz"), ("Shrimp Large Peeled", "Wild Caught", 11.99, "lb"),
        ("Ribeye Steak", "USDA Choice", 14.99, "lb"), ("Ground Turkey", "Jennie-O", 5.49, "lb"),
    ],
    "Frozen": [
        ("Frozen Pizza Margherita", "Amys", 8.99, "each"), ("Ice Cream Vanilla", "Tillamook", 5.99, "1.5qt"),
        ("Frozen Vegetables Mixed", "Birds Eye", 2.99, "bag"), ("Frozen Berries Mixed", "Wymans", 5.49, "bag"),
        ("Frozen Waffles", "Eggo", 3.49, "10 pack"), ("Frozen Burritos", "Amys", 3.49, "each"),
        ("Fish Sticks", "Gortons", 4.99, "box"), ("Frozen Edamame", "Seapoint Farms", 3.49, "bag"),
    ],
    "Snacks": [
        ("Potato Chips Sea Salt", "Kettle Brand", 4.49, "bag"), ("Trail Mix", "Kirkland", 8.99, "bag"),
        ("Granola Bars", "Nature Valley", 3.99, "6 pack"), ("Pretzels Twists", "Snyders", 3.49, "bag"),
        ("Dark Chocolate Bar", "Lindt", 3.99, "bar"), ("Popcorn Butter", "SkinnyPop", 4.49, "bag"),
        ("Crackers Wheat", "Triscuit", 3.99, "box"), ("Hummus Classic", "Sabra", 4.49, "10oz"),
        ("Mixed Nuts Roasted", "Planters", 7.99, "can"), ("Tortilla Chips", "Late July", 3.99, "bag"),
    ],
    "Beverages": [
        ("Orange Juice", "Tropicana", 4.99, "52oz"), ("Sparkling Water Lime", "LaCroix", 5.49, "12 pack"),
        ("Coffee Ground Medium", "Stumptown", 12.99, "12oz bag"), ("Green Tea Bags", "Tazo", 4.49, "20 bags"),
        ("Kombucha Ginger", "GTs", 3.99, "16oz"), ("Apple Juice", "Martinellis", 3.49, "1.5L"),
        ("Cold Brew Coffee", "Stumptown", 4.99, "12oz"), ("Coconut Water", "Vita Coco", 2.99, "16oz"),
    ],
    "Pantry": [
        ("Olive Oil Extra Virgin", "California Olive Ranch", 9.99, "bottle"), ("Pasta Spaghetti", "Barilla", 1.79, "box"),
        ("Marinara Sauce", "Raos", 7.99, "jar"), ("Rice Jasmine", "Mahatma", 4.99, "2lb bag"),
        ("Black Beans", "Goya", 1.29, "can"), ("Chicken Broth", "Swanson", 2.49, "32oz"),
        ("Peanut Butter", "Jif", 3.99, "jar"), ("Honey", "Local Harvest", 8.99, "16oz"),
        ("Canned Tuna", "Wild Planet", 3.49, "can"), ("Coconut Milk", "Thai Kitchen", 2.49, "can"),
        ("Maple Syrup", "Grade A", 9.99, "12oz"), ("Flour All Purpose", "King Arthur", 4.49, "5lb bag"),
    ],
    "Deli": [
        ("Roast Turkey Breast", "Boars Head", 10.99, "lb"), ("Ham Black Forest", "Boars Head", 9.99, "lb"),
        ("Swiss Cheese Sliced", "Boars Head", 8.99, "lb"), ("Rotisserie Chicken", "Store Made", 8.99, "each"),
        ("Potato Salad", "Deli Fresh", 4.99, "lb"), ("Coleslaw", "Deli Fresh", 3.99, "lb"),
    ],
    "Household": [
        ("Paper Towels", "Bounty", 12.99, "6 roll"), ("Dish Soap", "Dawn", 3.99, "bottle"),
        ("Trash Bags", "Glad", 9.99, "45 count"), ("Laundry Detergent", "Tide", 11.99, "bottle"),
        ("Aluminum Foil", "Reynolds", 4.49, "roll"), ("Sponges", "Scotch-Brite", 3.49, "3 pack"),
    ],
}

STORE_NAMES = [
    "FreshMart Downtown", "FreshMart Westside", "FreshMart Pearl District",
    "FreshMart Hawthorne", "FreshMart Lake Oswego", "FreshMart Beaverton",
    "FreshMart Sellwood", "FreshMart Alberta", "FreshMart Division",
    "FreshMart Hillsdale",
]


def random_phone():
    return f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}"


def random_email(first, last):
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]
    sep = random.choice([".", "_", ""])
    num = random.choice(["", str(random.randint(1, 99))])
    return f"{first.lower()}{sep}{last.lower()}{num}@{random.choice(domains)}"


def batch_insert(table, columns, rows, token, host, wid, batch_size=50):
    col_str = ", ".join(columns)
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        values_str = ", ".join(batch)
        stmt = f"INSERT INTO {table} ({col_str}) VALUES {values_str}"
        state = exec_sql(stmt, token, host, wid, f"Batch {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1}")
        if state == "SUCCEEDED":
            total += len(batch)
        elif state == "FAILED":
            print(f"    STOPPING inserts for {table} due to failure", file=sys.stderr)
            return total
    return total


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic retail grocery data in Databricks Unity Catalog."
    )
    parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile name")
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse ID")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name (e.g. my_catalog)")
    parser.add_argument("--schema", required=True, help="Schema name (e.g. retail_agent)")
    args = parser.parse_args()

    global FULL_SCHEMA
    FULL_SCHEMA = f"{args.catalog}.{args.schema}"

    print("Getting auth token...")
    token = get_token(args.profile)
    host = get_host(args.profile)
    wid = args.warehouse_id
    print(f"Host: {host}")
    print(f"Target schema: {FULL_SCHEMA}")

    # ── 1. Customers ────────────────────────────────────────────
    print("\n=== 1. Creating customers table (200 rows) ===")
    exec_sql(f"""
        CREATE OR REPLACE TABLE {FULL_SCHEMA}.customers (
            customer_id STRING, first_name STRING, last_name STRING, email STRING,
            phone STRING, address STRING, city STRING, state STRING, zip_code STRING,
            membership_tier STRING, join_date STRING, preferences STRING
        )
    """, token, host, wid, "Create table")

    rows = []
    for i in range(1, 201):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        city, state = random.choice(CITIES_STATES)
        prefs = json.dumps({
            "dietary": random.sample(DIETARY_PREFS, k=random.randint(0, 2)),
            "favorite_categories": random.sample(FAVORITE_CATEGORIES, k=random.randint(1, 3)),
            "organic_preference": random.choice([True, False]),
        }).replace("'", "''")
        addr = f"{random.randint(100,9999)} {random.choice(STREETS)}"
        zipcode = f"{random.randint(10000, 99999)}"
        tier = random.choices(MEMBERSHIP_TIERS, weights=[40, 30, 20, 10])[0]
        join_date = (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1800))).strftime("%Y-%m-%d")
        email = random_email(first, last)
        rows.append(
            f"('CUST-{i:04d}', '{esc(first)}', '{esc(last)}', '{esc(email)}', "
            f"'{random_phone()}', '{esc(addr)}', '{esc(city)}', '{state}', '{zipcode}', "
            f"'{tier}', '{join_date}', '{prefs}')"
        )

    count = batch_insert(f"{FULL_SCHEMA}.customers",
        ["customer_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "zip_code", "membership_tier", "join_date", "preferences"],
        rows, token, host, wid)
    print(f"  Total: {count} customers inserted")

    # ── 2. Products ─────────────────────────────────────────────
    print("\n=== 2. Creating products table (~500 rows) ===")
    exec_sql(f"""
        CREATE OR REPLACE TABLE {FULL_SCHEMA}.products (
            product_id STRING, name STRING, category STRING, brand STRING,
            price DOUBLE, stock_quantity INT, aisle INT, unit STRING
        )
    """, token, host, wid, "Create table")

    products = []
    aisles = {}
    aisle_num = 1
    pid = 1
    for cat in PRODUCTS_BY_CATEGORY:
        if cat not in aisles:
            aisles[cat] = aisle_num
            aisle_num += 1
        for name, brand, price, unit in PRODUCTS_BY_CATEGORY[cat]:
            products.append({"product_id": f"PROD-{pid:04d}", "name": name, "category": cat, "brand": brand, "price": price, "stock_quantity": random.randint(0, 500), "aisle": aisles[cat], "unit": unit})
            pid += 1

    while len(products) < 500:
        cat = random.choice(list(PRODUCTS_BY_CATEGORY.keys()))
        base = random.choice(PRODUCTS_BY_CATEGORY[cat])
        variation = random.choice(["Organic ", "Family Size ", "Value Pack ", "Premium ", "Lite "])
        products.append({"product_id": f"PROD-{pid:04d}", "name": f"{variation}{base[0]}", "category": cat, "brand": base[1], "price": round(base[2] * random.uniform(0.8, 1.5), 2), "stock_quantity": random.randint(0, 500), "aisle": aisles[cat], "unit": base[3]})
        pid += 1

    rows = []
    for p in products:
        rows.append(f"('{p['product_id']}', '{esc(p['name'])}', '{esc(p['category'])}', '{esc(p['brand'])}', {p['price']}, {p['stock_quantity']}, {p['aisle']}, '{esc(p['unit'])}')")

    count = batch_insert(f"{FULL_SCHEMA}.products",
        ["product_id", "name", "category", "brand", "price", "stock_quantity", "aisle", "unit"],
        rows, token, host, wid, batch_size=100)
    print(f"  Total: {count} products inserted")

    # ── 3. Stores ───────────────────────────────────────────────
    print("\n=== 3. Creating stores table (10 rows) ===")
    exec_sql(f"""
        CREATE OR REPLACE TABLE {FULL_SCHEMA}.stores (
            store_id STRING, name STRING, address STRING, city STRING, state STRING,
            zip_code STRING, hours STRING, phone STRING
        )
    """, token, host, wid, "Create table")

    rows = []
    store_ids = []
    for i, sname in enumerate(STORE_NAMES, 1):
        city, state = CITIES_STATES[i % len(CITIES_STATES)]
        store_ids.append(f"STORE-{i:02d}")
        rows.append(f"('STORE-{i:02d}', '{esc(sname)}', '{random.randint(100,9999)} {random.choice(STREETS)}', '{esc(city)}', '{state}', '{random.randint(10000,99999)}', '7:00 AM - 10:00 PM', '{random_phone()}')")

    count = batch_insert(f"{FULL_SCHEMA}.stores",
        ["store_id", "name", "address", "city", "state", "zip_code", "hours", "phone"],
        rows, token, host, wid)
    print(f"  Total: {count} stores inserted")

    # ── 4. Transactions + Items ─────────────────────────────────
    print("\n=== 4. Creating transactions table (2000 rows) ===")
    exec_sql(f"""
        CREATE OR REPLACE TABLE {FULL_SCHEMA}.transactions (
            transaction_id STRING, customer_id STRING, store_id STRING,
            transaction_date STRING, total_amount DOUBLE, payment_method STRING, status STRING
        )
    """, token, host, wid, "Create table")

    print("=== Creating transaction_items table (~8000+ rows) ===")
    exec_sql(f"""
        CREATE OR REPLACE TABLE {FULL_SCHEMA}.transaction_items (
            item_id STRING, transaction_id STRING, product_id STRING,
            quantity INT, unit_price DOUBLE, discount DOUBLE
        )
    """, token, host, wid, "Create table")

    txn_rows = []
    item_rows = []
    item_id = 1
    customer_ids = [f"CUST-{i:04d}" for i in range(1, 201)]

    for txn_id in range(1, 2001):
        cust = random.choice(customer_ids)
        store = random.choice(store_ids)
        txn_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 440), hours=random.randint(7, 21), minutes=random.randint(0, 59))
        num_items = random.randint(2, 8)
        txn_products = random.sample(products, k=min(num_items, len(products)))

        total = 0.0
        for prod in txn_products:
            qty = random.randint(1, 5)
            discount = round(random.choice([0, 0, 0, 0.5, 1.0, 1.5, 2.0]), 2)
            line_total = round(qty * prod["price"] - discount, 2)
            total += line_total
            item_rows.append(f"('ITEM-{item_id:06d}', 'TXN-{txn_id:05d}', '{prod['product_id']}', {qty}, {prod['price']}, {discount})")
            item_id += 1

        status = random.choices(["completed", "refunded", "pending"], weights=[90, 7, 3])[0]
        txn_rows.append(f"('TXN-{txn_id:05d}', '{cust}', '{store}', '{txn_date.strftime('%Y-%m-%d %H:%M:%S')}', {round(total, 2)}, '{random.choice(PAYMENT_METHODS)}', '{status}')")

    print(f"\n  Inserting {len(txn_rows)} transactions...")
    count = batch_insert(f"{FULL_SCHEMA}.transactions",
        ["transaction_id", "customer_id", "store_id", "transaction_date", "total_amount", "payment_method", "status"],
        txn_rows, token, host, wid, batch_size=100)
    print(f"  Total: {count} transactions inserted")

    print(f"\n  Inserting {len(item_rows)} transaction items...")
    count = batch_insert(f"{FULL_SCHEMA}.transaction_items",
        ["item_id", "transaction_id", "product_id", "quantity", "unit_price", "discount"],
        item_rows, token, host, wid, batch_size=200)
    print(f"  Total: {count} transaction items inserted")

    # ── 5. Payment History ──────────────────────────────────────
    print("\n=== 5. Creating payment_history table (400 rows) ===")
    exec_sql(f"""
        CREATE OR REPLACE TABLE {FULL_SCHEMA}.payment_history (
            payment_id STRING, customer_id STRING, payment_method STRING,
            card_last4 STRING, billing_address STRING, created_date STRING
        )
    """, token, host, wid, "Create table")

    rows = []
    for pay_id in range(1, 401):
        cust = random.choice(customer_ids)
        method = random.choice(PAYMENT_METHODS)
        card_last4 = str(random.randint(1000, 9999)) if method in ("credit_card", "debit_card") else ""
        city, state = random.choice(CITIES_STATES)
        billing = f"{random.randint(100,9999)} {random.choice(STREETS)}, {city}, {state}"
        created = (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 440))).strftime("%Y-%m-%d")
        card_val = f"'{card_last4}'" if card_last4 else "NULL"
        rows.append(f"('PAY-{pay_id:04d}', '{cust}', '{method}', {card_val}, '{esc(billing)}', '{created}')")

    count = batch_insert(f"{FULL_SCHEMA}.payment_history",
        ["payment_id", "customer_id", "payment_method", "card_last4", "billing_address", "created_date"],
        rows, token, host, wid)
    print(f"  Total: {count} payment records inserted")

    print("\n" + "=" * 50)
    print("All tables created successfully!")
    print(f"Schema: {FULL_SCHEMA}")
    print("=" * 50)


if __name__ == "__main__":
    main()
