# FlashEats Notebook Challenges

## Class 5

### Challenge 1: size the late-delivery problem

Defined a valid delivered order as one with a delivered status and both promised ETA and actual delivery timestamps. Defined late as actual delivery after promised ETA (`delay_min > 0`). Do not count cancelled orders or silently classify missing completion times as on time.

| Measure | Reference result |
|---|---:|
| Unique orders | 1,600 |
| Delivered orders after status normalization | 1,532 |
| Delivered orders missing actual delivery time | 37 |
| Valid delivered denominator | 1,495 |
| Late by any positive delay | 843 / 1,495 = 56.4% |
| Median delay among late orders | 8.03 minutes |
| Late by more than 10 minutes | 349 / 1,495 = 23.3% |

The database has 1,603 rows for 1,600 order IDs. The three repeated IDs have different `traffic_bucket` values, so exact-row deduplication will not remove them, while `drop_duplicates("order_id")` chooses a traffic value arbitrarily. Use the order-level `order_outcomes.csv` for the headline outcome check, and flag the conflicting source rows before using traffic segments. The five `Delivered` status spellings should be normalized for analysis, but preserve the original values for audit.

The denominator is a choice, not a technicality. The reported 56% is consistent with `delay_min > 0` among delivered records with known delivery times; it does not establish that this is the agreed KPI definition.

```python
# Use the normalized, one-row-per-order outcome file for the headline rate.
valid = outcomes[
    outcomes["delivered_flag"].eq(1) & outcomes["late_flag"].notna()
].copy()
valid["late_over_10"] = valid["delay_min"] > 10
late = valid["delay_min"] > 0

print("unique source orders:", orders["order_id"].nunique())
print("source rows:", len(orders))
print("valid delivered:", len(valid))
print("late:", int(late.sum()), f"({late.mean():.1%})")
print("median minutes late:", valid.loc[late, "delay_min"].median())
print("more than 10 minutes late:", int(valid["late_over_10"].sum()),
      f"({valid['late_over_10'].mean():.1%})")

# Inspect duplicate business keys and conflicting dimensions before segmenting.
duplicate_orders = orders_with_rowid[
    orders_with_rowid["order_id"].duplicated(keep=False)
].sort_values("order_id")
print(duplicate_orders[["source_row", "order_id", "traffic_bucket"]])
```

### Challenge 2: test the traffic explanation

After trimming and case-normalizing traffic labels, late rates by traffic bucket are approximately:

| Traffic | Valid orders | Late rate | Median delay |
|---|---:|---:|---:|
| low | 360 | 49.4% | -0.09 min |
| medium | 588 | 51.4% | 0.26 min |
| high | 424 | 66.5% | 3.99 min |
| severe | 123 | 65.9% | 4.26 min |

The uppercase `HIGH` value is a representation variant; combine it with `high` only after documenting the normalization. Heavy rain also has a higher late rate (73.6%, 72 valid orders) than clear weather (53.3%, 1,192 orders). These are associations, not causal estimates. Traffic, weather, distance, restaurant preparation time, and order timing may overlap; category coverage and the three conflicting traffic rows also limit interpretation. A useful next check is to compare preparation and travel times, and then request better instrumentation before claiming a cause.

```python
# Three duplicated IDs have conflicting traffic labels. Keeping the first row
# reproduces the reference summary, but resolve those conflicts before publishing.
one_row_per_order = orders.drop_duplicates("order_id", keep="first").copy()
one_row_per_order["traffic_norm"] = one_row_per_order["traffic_bucket"].str.strip().str.lower()
one_row_per_order["weather_norm"] = one_row_per_order["weather_bucket"].str.strip().str.lower()
segments = one_row_per_order.merge(
    outcomes[["order_id", "delivered_flag", "late_flag", "delay_min"]],
    on="order_id",
    validate="one_to_one",
)
segments = segments[
    segments["delivered_flag"].eq(1) & segments["late_flag"].notna()
]
for dimension in ["traffic_norm", "weather_norm"]:
    summary = segments.groupby(dimension).agg(
        orders=("order_id", "nunique"),
        late_rate=("late_flag", "mean"),
        median_delay=("delay_min", "median"),
    )
    print(dimension)
    print(summary)
```

### Challenge 3: customer-support evidence

There are 202 ticket rows, 201 distinct ticket IDs, and 198 distinct orders. The common categories are `late_delivery` (38), `eta_changed` (37), `restaurant_delay` (37), `ready_but_waiting` (33), `status_mismatch` (29), and `driver_not_moving` (25). The remaining three rows include casing/spacing variants (`Late Delivery`, `late_delivery `) and `ETA issue`; do not automatically map the latter to another category without confirming its meaning.

Support records add the customer-reported experience and complaint reason, which system timestamps alone do not capture. They are not a complete census of unhappy customers, and multiple tickets can refer to one order.

```python
print("ticket rows:", len(tickets))
print("distinct ticket IDs:", tickets["ticket_id"].nunique())
print("duplicate ticket IDs:", tickets["ticket_id"].duplicated().sum())
print("distinct linked orders:", tickets["order_id"].nunique())
print(tickets["category"].value_counts(dropna=False))
print("raw category spellings:", sorted(tickets["category"].dropna().unique()))
```

### Challenge 4: retrieve dispatch pages completely

The mock API contains 1,600 records. At page size 50, success means 32 pages, all records accounted for, and no repeated order IDs. The mock deliberately returns a first-attempt HTTP 500 on page 3 and HTTP 429 on page 5; retry these, preserve each successful response body as a raw page, and fail clearly if retries are exhausted or the reported total does not match the collected count.

```python
API_URL = "http://127.0.0.1:8000/dispatch/orders"
RAW_DIR = BASE / "student_output" / "raw_dispatch"
RAW_DIR.mkdir(parents=True, exist_ok=True)
```

One valid implementation pattern:

```python
import json
import time

def fetch_all_dispatch_orders():
    records = []
    expected_total = None
    page = 1

    while True:
        for attempt in range(3):
            response = requests.get(
                API_URL,
                params={"page": page, "page_size": 50},
                timeout=10,
            )
            if response.status_code in (429, 500) and attempt < 2:
                try:
                    wait = float(response.json().get("retry_after_seconds", 1))
                except (ValueError, json.JSONDecodeError):
                    wait = 1
                time.sleep(wait)
                continue
            response.raise_for_status()
            (RAW_DIR / f"page_{page:04}.json").write_bytes(response.content)
            payload = response.json()
            break
        else:
            raise RuntimeError(f"Page {page} failed after retries")

        if payload.get("page") != page:
            raise RuntimeError(f"Unexpected page returned for request {page}")
        if expected_total is None:
            expected_total = payload["total_records"]
        elif payload["total_records"] != expected_total:
            raise RuntimeError("API total_records changed during ingestion")

        records.extend(payload["data"])
        if not payload["has_more"]:
            break
        page += 1

    if len(records) != expected_total:
        raise RuntimeError(f"Expected {expected_total} records; got {len(records)}")
    if len({record["order_id"] for record in records}) != len(records):
        raise RuntimeError("Duplicate order IDs in retrieved pages")
    return records
```

Expected behavior is 32 successful pages and 34 HTTP attempts because pages 3 and 5 each need one retry. Keep the raw files even when a later page fails so the partial run is auditable; do not describe it as complete.

### Challenge 5: driver events and arrival evidence

The driver JSON contains 120 drivers, 1,600 assignment events, 5,371 GPS pings, and 1,532 pickup and delivery events. Assignment, pickup, and delivery are directly observed. There is no explicit driver-arrived-at-restaurant event. Arrival can only be inferred from GPS, and a proximity rule would need a geofence, GPS accuracy/timing assumptions, and validation against an authoritative arrival signal.

```python
event_counts = Counter(
    event["type"]
    for driver in driver_events
    for event in driver["events"]
)
print("drivers:", len(driver_events))
print(event_counts)
print("explicit restaurant-arrival events:",
      event_counts.get("arrived_at_restaurant", 0))
```

**Recommendation:** do not build the delay predictor yet. Agree on the late definition, resolve the duplicate/order-grain and instrumentation issues, and collect trustworthy workflow events such as restaurant-ready and driver-arrival timestamps first.

## Class 6

### Challenge 1: validation contract

- **Grain:** one row should represent one business order. The SQLite table violates this with three repeated IDs; inspect and resolve the conflicting traffic labels rather than treating exact-row deduplication as sufficient.
- **Completion:** 37 delivered orders have no actual-delivery timestamp. Exclude them from the timestamp-based rate and report them separately.
- **ETA and chronology:** four orders have promised ETA before creation; five have pickup after actual delivery. These are warnings or failures depending on the intended decision.
- **Definition:** `> 0` minutes produces 56.4%; `> 10` minutes produces 23.3%. The definitions answer different business questions.
- **Ownership:** the metric notes list VP Operations, Support, Finance, and Data definitions, but no canonical KPI owner.

```python
analysis = orders.drop_duplicates("order_id", keep="first").copy()
for column in ["created_at", "promised_eta", "pickup_at", "actual_delivery_at"]:
    analysis[column] = pd.to_datetime(analysis[column], format="mixed", errors="coerce")
analysis["status_norm"] = analysis["final_status"].str.strip().str.lower()

delivered = analysis[analysis["status_norm"].eq("delivered")]
print("rows / unique IDs:", len(orders), orders["order_id"].nunique())
print("duplicate IDs:", orders["order_id"].duplicated().sum())
print("delivered rows:", len(delivered))
print("delivered missing actual:", delivered["actual_delivery_at"].isna().sum())
print("ETA before creation:", (analysis["promised_eta"] < analysis["created_at"]).sum())
print("delivery before pickup:", (analysis["actual_delivery_at"] < analysis["pickup_at"]).sum())
```

### Challenge 2: compare definitions

| Definition | Rate | Interpretation |
|---|---:|---|
| Any positive delay, among valid delivered orders | 56.4% | Sensitive to any lateness, including seconds. |
| More than 10 minutes late, same denominator | 23.3% | Counts materially late orders under Support's threshold. |
| Historical delivered/non-null population | 56.4% in this data | Reproduces the historical-style denominator; does not prove the definition is approved. |

Leadership should not publish an unqualified “56%” until the KPI owner approves the threshold, denominator, cancellation/refund policy, missing-time treatment, and reporting period. A provisional dashboard can show both rates with definitions and the missing-data count.

```python
valid_delivered = outcomes[
    outcomes["delivered_flag"].eq(1) & outcomes["delay_min"].notna()
]
definitions = {
    "any positive delay": valid_delivered["delay_min"] > 0,
    "more than 10 minutes": valid_delivered["delay_min"] > 10,
}
for name, late_mask in definitions.items():
    print(name, int(late_mask.sum()), "/", len(valid_delivered),
          f"= {late_mask.mean():.1%}")
```

### Challenge 3: categories

Observed representation issues include `delivered` / `Delivered`, `high` / `HIGH`, whitespace and casing variants in ticket categories, and restaurant status variants such as `ready` / `READY` / `Ready` and `handed_off` / `handoff`. Trimming whitespace and normalizing case for known representation variants is generally safe if raw values are retained. Mapping `handoff` to `handed_off`, or mapping `ETA issue` to `eta_changed`, changes semantics and needs an owner to confirm.

```python
for column in ["final_status", "traffic_bucket"]:
    print(column, orders[column].value_counts(dropna=False).to_dict())
print("ticket categories:", tickets["category"].value_counts(dropna=False).to_dict())
print("restaurant statuses:", restaurant_status["status"].value_counts(dropna=False).to_dict())
```

### Challenge 4: cross-source integrity

- All 1,597 non-null order restaurant IDs map to the restaurants table; three order rows have a null restaurant ID.
- All 1,597 non-null order driver IDs map to the drivers table; three order rows have a null driver ID.
- All 199 non-null ticket order IDs map to orders; three ticket rows have no order ID. There is also one repeated ticket ID.
- All 502 restaurant-status order IDs map to orders; those rows cover 500 distinct orders, so status is not one row per order.

Report coverage over non-null keys and null-key counts separately. Whether missing mappings are acceptable depends on the decision and should be owned by the consumer.

```python
def mapping_evidence(source, key, target):
    non_null = source[key].dropna()
    mapped = non_null.isin(target).sum()
    return {
        "non_null": len(non_null),
        "mapped": int(mapped),
        "coverage": mapped / len(non_null) if len(non_null) else float("nan"),
        "null_keys": int(source[key].isna().sum()),
    }

orders_one = orders.drop_duplicates("order_id")
print("restaurant:", mapping_evidence(
    orders_one, "restaurant_id", restaurants["restaurant_id"]
))
print("driver:", mapping_evidence(
    orders_one, "driver_id", drivers["driver_id"]
))
print("ticket order:", mapping_evidence(
    tickets, "order_id", orders_one["order_id"]
))
print("status order:", mapping_evidence(
    restaurant_status, "order_id", orders_one["order_id"]
))
print("status rows / distinct orders:",
      len(restaurant_status), restaurant_status["order_id"].nunique())
```

### Challenge 5: freshness

The 502 restaurant-status records cover 500 orders. Their `last_updated_at` is a median of 20 minutes after order creation (range: 354 minutes before to 35 minutes after); 166 records are later than pickup, and none is later than delivery. This may support retrospective or weekly analysis, but it is not enough to certify live ETA use. “Fresh enough” has no pass/fail answer until an SLA is defined; the negative timestamps and sparse order coverage need investigation.

```python
status_check = restaurant_status.merge(
    orders_one[["order_id", "created_at", "pickup_at", "actual_delivery_at"]],
    on="order_id",
    how="left",
    validate="many_to_one",
)
for column in ["last_updated_at", "created_at", "pickup_at", "actual_delivery_at"]:
    status_check[column] = pd.to_datetime(status_check[column], format="mixed", errors="coerce")
lag_min = (status_check["last_updated_at"] - status_check["created_at"]).dt.total_seconds() / 60
print("status update lag (minutes):", lag_min.describe())
print("updated after pickup:", (status_check["last_updated_at"] > status_check["pickup_at"]).sum())
print("updated after delivery:", (status_check["last_updated_at"] > status_check["actual_delivery_at"]).sum())
```

### Challenge 6: validation gate

| Check | Suggested status | Evidence / action |
|---|---|---|
| Business grain | WARN | Resolve three repeated order IDs and conflicting traffic labels. |
| Timestamp chronology | WARN | Investigate four ETA-before-create and five delivery-before-pickup records. |
| KPI definition | UNKNOWN | Get a documented definition and named owner. |
| Category semantics | WARN | Normalize representation only; confirm ambiguous status/category mappings. |
| Cross-source mapping | WARN | Report null IDs and the repeated ticket/status records; set decision-specific thresholds. |
| Freshness | UNKNOWN | Define an SLA and validate it for the intended use. |
| Publish “56%” | UNKNOWN / do not publish unqualified | Publish only after definition ownership and data caveats are resolved. |

These statuses are not a substitute for an agreed organizational severity policy. The defensible conclusion today is that 56.4% is reproducible under one definition, not that it is the canonical Late Delivery Rate.

```python
# Evidence can be computed; severity and publication status still need owners.
validation_report = {
    "business_grain": "WARN" if orders["order_id"].duplicated().any() else "PASS",
    "timestamp_chronology": "WARN",  # review the invalid chronology rows above
    "kpi_definition": "UNKNOWN",      # no canonical owner is documented
    "category_semantics": "WARN",    # ambiguous mappings remain unresolved
    "cross_source_mapping": "WARN", # null keys and repeated status/ticket rows exist
    "freshness": "UNKNOWN",         # no use-case SLA is specified
    "publish_56_percent": "UNKNOWN",
}
print(validation_report)
```

## Class 7

### Challenge 1: lifecycle timeline

Use `data/order_events.csv` as the event spine (`order_id`, `event_time`, `event_type`, actor, source), then add relevant app actions, tickets, and interventions if those sources are not already represented. Sort the combined events by parsed timestamp. Select example order IDs from `order_outcomes.csv` for a late delivered order, an on-time delivered order, and an order in `order_interventions.csv`. Preserve source and actor on every row. Do not fabricate an arrival-at-restaurant event: the available data does not observe it.

```python
def build_order_timeline(order_id):
    event_rows = order_events[order_events["order_id"].eq(order_id)].rename(
        columns={"event_time": "event_at", "event_type": "event", "actor_type": "actor"}
    )[["order_id", "event_at", "event", "actor", "source_system"]]
    action_rows = customer_actions[customer_actions["order_id"].eq(order_id)].rename(
        columns={"action_at": "event_at", "action_type": "event", "customer_id": "actor"}
    )[["order_id", "event_at", "event", "actor"]].assign(source_system="customer_app_actions")
    ticket_rows = tickets[tickets["order_id"].eq(order_id)].rename(
        columns={"created_at": "event_at", "category": "event"}
    )[["order_id", "event_at", "event"]].assign(actor="support", source_system="support_tickets")
    intervention_rows = interventions[interventions["order_id"].eq(order_id)].rename(
        columns={"intervention_at": "event_at", "intervention_type": "event", "initiated_by": "actor"}
    )[["order_id", "event_at", "event", "actor"]].assign(source_system="order_interventions")
    timeline = pd.concat(
        [event_rows, action_rows, ticket_rows, intervention_rows], ignore_index=True
    )
    timeline["event_at"] = pd.to_datetime(timeline["event_at"], format="mixed", errors="coerce")
    return timeline.sort_values("event_at")

late_order_id = outcomes.loc[outcomes["late_flag"].eq(1), "order_id"].iloc[0]
on_time_order_id = outcomes.loc[outcomes["late_flag"].eq(0), "order_id"].iloc[0]
intervention_order_id = interventions["order_id"].iloc[0]
print("late order", late_order_id)
print(build_order_timeline(late_order_id))
print("on-time order", on_time_order_id)
print(build_order_timeline(on_time_order_id))
print("intervention order", intervention_order_id)
print(build_order_timeline(intervention_order_id))
```

### Challenge 2: canonical project model

| Table | Key | Grain and relationships |
|---|---|---|
| `orders` | `order_id` | One business order; links customer, restaurant, and driver. |
| `customer_app_actions` | `action_id` | One customer action; many actions can belong to an order and customer. |
| `support_tickets` | `ticket_id` | One ticket; many tickets can refer to an order. |
| `order_interventions` | `intervention_id` | One operational intervention; many may belong to an order. |
| `order_outcomes` | `order_id` | One normalized outcome per order, including late flag and delay. |

Keep one-to-many facts separate and aggregate them to order grain before joining to outcomes. This supports the project questions without copying each source system's layout or multiplying order rows in joins.

```python
tables = {
    "orders": ("order_id", orders["order_id"].nunique()),
    "customer_app_actions": ("action_id", customer_actions["action_id"].nunique()),
    "support_tickets": ("ticket_id", tickets["ticket_id"].nunique()),
    "order_interventions": ("intervention_id", interventions["intervention_id"].nunique()),
    "order_outcomes": ("order_id", outcomes["order_id"].nunique()),
}
for name, (candidate_key, distinct_keys) in tables.items():
    print(name, "candidate key:", candidate_key, "distinct keys:", distinct_keys)
```

### Challenge 3: interaction → intervention → outcome

Build flags from `customer_app_actions` (`SUPPORT_OPENED`, `CANCEL_ATTEMPTED`), aggregate intervention count and types from `order_interventions`, then join those aggregates to the one-row-per-order outcome table. Tickets are a distinct support signal; do not conflate a ticket with an app action.

Reference results:

- 138 orders have a `SUPPORT_OPENED` app action; all 138 are marked late in the packaged outcome data. Ten orders have a `CANCEL_ATTEMPTED` action.
- 202 tickets represent 198 distinct orders and 201 distinct ticket IDs. Among valid outcomes, 163 of 197 ticketed orders are late (82.7%), versus 52.4% among orders without a ticket.
- 430 orders have an intervention. `DRIVER_REASSIGNMENT` is most common (155), followed by `RESTAURANT_CONTACT` (116), `PRIORITY_DISPATCH` (95), and `CUSTOMER_CREDIT` (64).
- Late rates by intervention type range from 51.2% for priority dispatch to 66.7% for customer credit. They describe selected cases, not intervention effectiveness.

“Frustrated journey” must be defined. A ticket, support-open action, and cancel attempt are stronger signals than an ETA view alone. Filter the chosen interaction flag to orders with no intervention and inspect those order IDs; do not label every app action as frustration.

```python
action_flags = customer_actions.pivot_table(
    index="order_id", columns="action_type", values="action_id",
    aggfunc="count", fill_value=0,
)
action_flags["support_opened"] = action_flags.get("SUPPORT_OPENED", 0) > 0
action_flags["cancel_attempted"] = action_flags.get("CANCEL_ATTEMPTED", 0) > 0
intervention_summary = interventions.groupby("order_id").agg(
    intervention_count=("intervention_id", "nunique"),
    intervention_types=("intervention_type", lambda values: ", ".join(sorted(set(values)))),
)
ticket_summary = tickets.groupby("order_id").agg(ticket_count=("ticket_id", "count"))

order_model = outcomes.merge(action_flags, on="order_id", how="left", validate="one_to_one")
order_model = order_model.merge(intervention_summary, on="order_id", how="left", validate="one_to_one")
order_model = order_model.merge(ticket_summary, on="order_id", how="left", validate="one_to_one")
order_model[["support_opened", "cancel_attempted"]] = order_model[
    ["support_opened", "cancel_attempted"]
].fillna(False)
order_model[["intervention_count", "ticket_count"]] = order_model[
    ["intervention_count", "ticket_count"]
].fillna(0)
order_model["intervention_types"] = order_model["intervention_types"].fillna("")

valid_model = order_model[order_model["late_flag"].notna()]
print("support-opened orders:", int(order_model["support_opened"].sum()))
print("late rate by ticket presence:")
print(valid_model.assign(has_ticket=valid_model["ticket_count"].gt(0))
      .groupby("has_ticket")["late_flag"].mean())
print("intervention counts:", interventions["intervention_type"].value_counts())
print("frustrated signals without intervention:")
print(order_model.loc[
    (order_model["support_opened"] | order_model["cancel_attempted"])
    & order_model["intervention_count"].eq(0), "order_id"
].tolist())
```

### Challenge 4: useful metrics

| Metric | Formula and grain | KPI connection |
|---|---|---|
| Late delivery rate | Late valid delivered orders / all valid delivered orders; order grain | Direct project outcome; always publish the definition and missing count. |
| Support-contact rate | Distinct orders with a ticket / eligible orders; order grain | Customer experience signal; not a direct delay cause. |
| Intervention rate | Distinct orders with an intervention / eligible orders; order grain | Operational activity measure, not proof of successful action. |
| Median preparation time | Median(`pickup_at - created_at`) for valid orders; order grain | Workflow diagnostic; compare segments and monitor alongside the outcome. |
| Median pickup-to-delivery time | Median(`actual_delivery_at - pickup_at`) for valid delivered orders; order grain | Delivery-stage diagnostic and possible operational lever. |

```python
valid_outcomes = outcomes[outcomes["late_flag"].notna()]
metric_results = {
    "late_delivery_rate": valid_outcomes["late_flag"].mean(),
    "support_ticket_order_rate": tickets["order_id"].nunique() / outcomes["order_id"].nunique(),
    "intervention_order_rate": interventions["order_id"].nunique() / outcomes["order_id"].nunique(),
    "cancel_attempt_order_rate": customer_actions.loc[
        customer_actions["action_type"].eq("CANCEL_ATTEMPTED"), "order_id"
    ].nunique() / outcomes["order_id"].nunique(),
}
print(metric_results)
```

For valid outcomes, median preparation time is 31.88 minutes for late orders versus 19.85 for on-time orders; median pickup-to-delivery time is 48.92 versus 45.26 minutes. This points to preparation as a useful investigation area, not a causal conclusion.

```python
stage_data = orders.drop_duplicates("order_id").merge(
    outcomes[["order_id", "late_flag"]], on="order_id", validate="one_to_one"
)
for column in ["created_at", "pickup_at", "actual_delivery_at"]:
    stage_data[column] = pd.to_datetime(stage_data[column], format="mixed", errors="coerce")
stage_data["prep_min"] = (
    stage_data["pickup_at"] - stage_data["created_at"]
).dt.total_seconds() / 60
stage_data["travel_min"] = (
    stage_data["actual_delivery_at"] - stage_data["pickup_at"]
).dt.total_seconds() / 60
print(stage_data.groupby("late_flag")[["prep_min", "travel_min"]].median())
```

### Challenge 5: interpret the comparisons

- Ticketed orders are more often late than unticketed orders (82.7% vs 52.4% among valid outcomes). This is consistent with customers contacting support about problematic deliveries; it does not show that support contact causes lateness.
- Late rates are almost identical for orders with and without an intervention (56.44% vs 56.37%). Interventions may be triggered by harder cases, so this is not an estimate of their effect.
- Priority dispatch has the lowest observed late rate among intervention types (51.2%), while customer credit has the highest (66.7%). These actions have different purposes and selection rules; do not rank them as causal treatments.
- Restaurant late-order counts vary, but compare rates and sample sizes before targeting a restaurant. Counts alone mostly reflect order volume.
- Journeys with a support signal, an intervention, and a late outcome identify recovery failures worth reviewing. They do not show whether the intervention improved the counterfactual outcome.

```python
valid_model = order_model[order_model["late_flag"].notna()].copy()
valid_model["has_intervention"] = valid_model["intervention_count"].gt(0)
print("late rate with/without intervention:")
print(valid_model.groupby("has_intervention")["late_flag"].mean())

restaurant_performance = (
    orders.drop_duplicates("order_id")[["order_id", "restaurant_id"]]
    .merge(outcomes[["order_id", "late_flag"]], on="order_id", validate="one_to_one")
    .groupby("restaurant_id")
    .agg(orders=("order_id", "nunique"), late_orders=("late_flag", "sum"),
         late_rate=("late_flag", "mean"))
    .sort_values("late_orders", ascending=False)
)
print(restaurant_performance.head(10))

by_intervention = interventions.merge(
    outcomes[["order_id", "late_flag"]], on="order_id", validate="many_to_one"
).groupby("intervention_type").agg(
    intervention_rows=("intervention_id", "count"),
    orders=("order_id", "nunique"),
    late_rate=("late_flag", "mean"),
)
print(by_intervention)
```

### Challenge 6: connect the model to the KPI

Use late delivery rate as the outcome KPI; preparation time, pickup-to-delivery time, support-contact rate, and intervention rate as diagnostic/driver metrics; and event/action/ticket tables as evidence. Operations can directly control dispatch and restaurant follow-up processes, while delivery lateness is an outcome influenced by multiple factors. The most important missing event for attribution is a reliable driver-arrival-at-restaurant timestamp (and, for stage diagnosis, a trustworthy restaurant-ready timestamp). Instrument these with event time, actor, source, and quality/latency metadata.

The model's key limitation is selection and observability: interventions are not randomized, support records are incomplete, and important lifecycle transitions are absent. It can describe workflow associations and prioritize investigation, but cannot yet prove which intervention reduces late delivery.

```python
project_view = {
    "project_kpi": "late delivery rate",
    "outcome_metric": "late valid delivered orders / all valid delivered orders",
    "workflow_diagnostics": ["median preparation minutes", "median pickup-to-delivery minutes"],
    "customer_signals": ["support-contact rate", "cancel-attempt rate"],
    "operational_activity": ["intervention rate", "intervention type"],
    "supporting_sources": [
        "orders", "order_outcomes", "order_events", "customer_app_actions",
        "support_tickets", "order_interventions",
    ],
    "important_missing_events": ["driver arrived at restaurant", "restaurant marked ready"],
}
print(project_view)
```