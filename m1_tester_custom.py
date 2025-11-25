
#!/usr/bin/env python3
import random
import time

from lstore.db import Database
from lstore.query import Query


def print_table_info(table):
    print("Table:")
    print(f"  name={table.name}")
    print(f"  num_columns={table.num_columns}")
    print(f"  key_index={table.key}")
    print(f"  key_to_rid size={len(table.key_to_rid)}")
    print(f"  page_directory size={len(table.page_directory)}")


def pretty_record(label, rec):
    print(f"{label}: rid={rec.rid} key={rec.key} cols={rec.columns}")


def select_exact(query, key):
    return query.select(key, query.table.key, [1] * query.table.num_columns)[0]


def main():
    random.seed(3562901)

    db = Database()
    table = db.create_table("Grades", 5, 0)  # key at col 0
    query = Query(table)

    print_table_info(table)
    print()

    # 1) Insert
    num = 100
    keys = set()
    rows = {}
    t0 = time.time()
    for _ in range(num):
        k = 92106429 + random.randint(0, num * 2)
        while k in keys:
            k = 92106429 + random.randint(0, num * 2)
        keys.add(k)
        row = [k, random.randint(0, 20), random.randint(0, 20), random.randint(0, 20), random.randint(0, 20)]
        ok = query.insert(*row)
        assert ok is True
        rows[k] = row
    t_insert = time.time() - t0
    print(f"Inserted {len(rows)} rows in {t_insert:.4f}s")
    print_table_info(table)
    print()

    # Show a couple of samples after insert
    sample_keys = list(sorted(keys))[:3]
    print("Sample records after insert:")
    for k in sample_keys:
        rec = select_exact(query, k)
        pretty_record(f"  key={k}", rec)
        assert rec.columns == rows[k]
    print()

    # 2) Select verification (spot-check)
    t0 = time.time()
    for k in sample_keys:
        rec = select_exact(query, k)
        assert rec.columns == rows[k]
    t_select = time.time() - t0
    print(f"Spot select for {len(sample_keys)} rows in {t_select:.4f}s")
    print()

    # 3) Update a subset: update columns 2..4; None means no change
    print("Updating subset of rows (cols 2..4)...")
    t0 = time.time()
    updates = {}
    for i, k in enumerate(sorted(keys)[:10]):  # small subset for visibility
        upd = [None, None, None, None, None]
        # change columns 2..4
        for c in range(2, table.num_columns):
            upd[c] = random.randint(0, 20)
        ok = query.update(k, *upd)
        assert ok is True
        # reflect in mirror dict
        new_row = rows[k].copy()
        for c in range(2, table.num_columns):
            new_row[c] = upd[c]
        rows[k] = new_row
        updates[k] = upd
    t_update = time.time() - t0
    print(f"Updated {len(updates)} rows in {t_update:.4f}s")
    print()

    # Show before/after for a single updated row
    if updates:
        k0 = next(iter(updates.keys()))
        rec_after = select_exact(query, k0)
        print(f"After update for key={k0}:")
        pretty_record("  latest", rec_after)
        print(f"  expected: {rows[k0]}")
        assert rec_after.columns == rows[k0]
        print()

    # 4) Deterministic sum checks
    print("Running deterministic sum checks...")
    ksorted = sorted(keys)
    if len(ksorted) >= 6:
        a, b = ksorted[1], ksorted[5]
        for col in range(table.num_columns):
            want = sum(rows[k][col] for k in ksorted if a <= k <= b)
            got = query.sum(a, b, col)
            assert got == want, f"sum mismatch: col={col} got={got} want={want}"
        print("  Sum checks OK.")
    else:
        print("  Not enough keys to run sum checks.")
    print()

    # 5) Inspect one base/tail record metadata (optional visibility)
    print("Inspecting one base record's metadata after updates:")
    k_any = next(iter(keys))
    base_rid = table.key_to_rid[k_any]
    base = table.page_directory[base_rid]
    # base layout: [indirection, rid, timestamp, schema, *cols...]
    indirection = base[0]
    rid = base[1]
    ts = base[2]
    schema = base[3]
    print(f"  base_rid={rid} indirection={indirection} timestamp={ts} schema={schema}")
    if indirection != 0:
        tail = table.page_directory[indirection]
        print(f"  tail_rid={tail[1]} tail_ts={tail[2]} tail_schema={tail[3]}")
    print()

    print("All sanity checks passed.")


if __name__ == "__main__":
    main()