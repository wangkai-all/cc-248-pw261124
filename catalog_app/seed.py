"""Deterministic seed content for the simulated source.

Object graph of the clean seed:

    catalog/main
        product/p100 -> dataset/sales
        product/p101 -> dataset/sales
        product/p102 -> dataset/inventory
    dataset/sales      -> field/region, field/amount, rule/region-check
    dataset/inventory  -> field/sku, field/qty,    rule/qty-limit
    (dataset/cyclic is seeded but unreferenced; the control panel can wire a
    reference to it to demonstrate circular-reference detection)
"""

from __future__ import annotations

from .source import SourceState


def seed_state(state: SourceState) -> SourceState:
    def put(otype, oid, body):
        status, _, _ = state.put(otype, oid, body)
        assert status == 201, (otype, oid, status)

    put("field", "region", {"name": "region", "dtype": "string",
                            "label": "销售区域"})
    put("field", "amount", {"name": "amount", "dtype": "decimal",
                            "label": "销售额"})
    put("field", "sku", {"name": "sku", "dtype": "string", "label": "SKU"})
    put("field", "qty", {"name": "qty", "dtype": "integer", "label": "库存数量"})
    put("field", "orphan", {"name": "note", "dtype": "string",
                            "label": "未被引用的字段"})

    put("rule", "region-check", {"name": "region-check", "kind": "enum",
                                 "params": {"values": ["east", "west",
                                                       "north", "south"]}})
    put("rule", "qty-limit", {"name": "qty-limit", "kind": "range",
                              "params": {"min": 0, "max": 100000}})

    put("dataset", "sales", {
        "name": "销售数据集",
        "fields": ["field/region", "field/amount"],
        "rules": ["rule/region-check"],
    })
    put("dataset", "inventory", {
        "name": "库存数据集",
        "fields": ["field/sku", "field/qty"],
        "rules": ["rule/qty-limit"],
    })
    # cyclic dataset points back at itself; only becomes reachable when a
    # product or another dataset references it.
    put("dataset", "cyclic", {
        "name": "循环引用数据集",
        "fields": ["field/sku"],
        "rules": [],
        "depends_on": ["dataset/cyclic"],
    })

    put("product", "p100", {"name": "销售看板", "dataset": "dataset/sales"})
    put("product", "p101", {"name": "区域报表", "dataset": "dataset/sales"})
    put("product", "p102", {"name": "库存看板", "dataset": "dataset/inventory"})
    put("product", "p-ghost", {"name": "指向已删除数据集的产品",
                               "dataset": "dataset/missing"})

    put("catalog", "main", {
        "name": "产品目录",
        "products": ["product/p100", "product/p101", "product/p102"],
    })
    put("catalog", "with-ghost", {
        "name": "含断链目录",
        "products": ["product/p100", "product/p-ghost"],
    })
    return state


def fresh_state() -> SourceState:
    return seed_state(SourceState())
