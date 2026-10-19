"""Spike 08, phase 2: the candidate routes the phase harness times (throwaway prototypes).

  APlusRoute     A+, JSON: Spike08Marker's Jackson pre-pass, a read with the inferred schema of the marked
                 documents (the fit pass), the inferred schema compared with the definitions path by path,
                 from_json per value where a complex path disagrees, and SQL generated from the definitions
                 that checks every value and builds the stored layout and the findings. Egress: to_json with
                 every decimal marked and unmarked after (the existing T100j mechanism). Like A0's egress,
                 it leaves a null structure null and passes an all-null structure or a null-holding array
                 through as it is (decision 71), relying on the stored layout to carry neither.
  APlusRoute     A+, XML: the from_xml route, with the gaps closed that a fair comparison needs (a prefixed
                 FHIR namespace is read, a missing one is refused, text or a second root after the document
                 is refused through Spike08XmlDocCheck), and the same generated checks. Egress: to_xml, the
                 decimal's text as stored. A-variant has no XML route of its own and is served by this one.
  AVariantRoute  A-variant, JSON only: parse_json, Spike08JsonDocCheck for what parse_json accepts (a
                 top-level array, text after the object), schema_of_variant_agg as the fit pass, and SQL
                 generated from the definitions over the variant. Egress: HEAD's (pc decode), as ticket 08
                 says: A-variant has no route that keeps a decimal's text.
  BRoute         B: Spike08B (Java), our own parser and renderer driven by the definitions, JSON and XML.

Policy (SPIKE_POLICY): fail (the default of decision 133: the first invalid value fails the job, naming the
resource, the path and the value) or drop (invalid values are left out and recorded in _findings, which
SPIKE_FINDINGS=1 adds to the stored dataset). Timing uses fail, with every check still evaluated.
Every route here builds the stored layout itself rather than through ResourceTransformer.toLayout, whose
decimal converter would put the value through a double. The schema it builds is the one toLayout builds
(gate_candidates.py checks this against HEAD at size s).
"""
import json, os, re, time
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (ArrayType, BooleanType, StringType, StructField, StructType)

import spike_checks as C
from fromxml_route import FHIR_NS, DIV_PLACEHOLDER, NARRATIVE, Node, XmlRoute, _field, _inner

MARK_NUM, MARK_STR, PRE_F, PRE_D = "\u001e#", "\u001e$", "\u001ef", "\u001ed"
XML = "application/fhir+xml"


# An element that has checks but no stored column: present only as {} or [], it is left out of the schema
# (FR-011), and the checks still find it.
NO_COLUMN = object()


class Out:
    """The generated parts of one value: the stored column, whether it is valid, and its findings."""

    def __init__(self, stored, ok=None, fs=None):
        self.stored, self.ok, self.fs = stored, ok, fs


class Gen:
    """Walks the definitions beside a source's schema, through an adapter, and generates one of:
    'store' (the stored value and validity, for fail), 'findings' (the findings alone, for fail's message),
    or 'drop' (the stored value with invalid values left out, and the findings)."""

    def __init__(self, adapter, mode):
        self.a, self.mode = adapter, mode
        # The dotted key of each path column, without indices, for the types the fit pass resolved.
        self.keys = {}

    def child(self, path, name):
        p = C.child_path(path, name)
        k = self.key(path)
        self.keys[id(p)] = (p, name if not k else k + "." + name)
        return p

    def index(self, path, i):
        p = C.index_path(path, i)
        self.keys[id(p)] = (p, self.key(path))
        return p

    def key(self, path):
        return self.keys.get(id(path), (None, ""))[1]

    def node(self, node, stype, acc, path, ctx, root=False):
        cols, oks, fss = [], [], []
        for e in node.entries:
            if e.is_resource_type:
                if root:
                    cols.append(F.when(F.col("value").isNotNull(), F.lit(ctx["type"])).alias("resourceType"))
                continue
            if not e.is_element:
                continue
            r = self.a.element(self, node, e, stype, acc, self.child(path, e.name), ctx)
            if r is None:
                continue
            if r.stored is not NO_COLUMN:
                cols.append((r.stored if r.stored is not None else F.lit(None)).alias(e.name))
            oks.append(r.ok)
            fss.append(r.fs)
        for ok, fs in self.a.extras(self, node, stype, acc, path, ctx, root):
            oks.append(ok)
            fss.append(fs)
        return cols, C.all_of(oks), C.concat_findings(fss)

    def struct(self, node, stype, v, path, ctx):
        cols, ok, fs = self.node(node, stype, lambda n: v[n], path, ctx)
        if not cols:
            return None
        guard = v.isNotNull()
        return Out(F.when(guard, F.struct(*cols)),
                   None if ok is None else (~guard | ok),
                   None if fs is None else F.when(guard, fs).otherwise(C.no_findings()))

    def each(self, arr, element, path):
        """Applies a per-element plan to an array; element(x, path) returns an Out."""
        if self.mode == "findings":
            return Out(None, None, F.when(arr.isNotNull(), F.flatten(F.transform(
                arr, lambda x, i: _or_empty(element(x, self.index(path, i)).fs)))).otherwise(C.no_findings()))
        if self.mode == "store":
            def pair(x):
                o = element(x, path)
                return F.struct(o.stored.alias("s"), (o.ok if o.ok is not None else F.lit(True)).alias("o"))
            t = F.transform(arr, pair)
            return Out(F.transform(t, lambda y: y["s"]), arr.isNull() | F.forall(t, lambda y: y["o"]))
        def triple(x, i):
            o = element(x, self.index(path, i))
            return F.struct(o.stored.alias("s"), _or_empty(o.fs).alias("f"))
        t = F.transform(arr, triple)
        return Out(F.transform(t, lambda y: y["s"]), None,
                   F.when(arr.isNotNull(), F.flatten(F.transform(t, lambda y: y["f"]))).otherwise(C.no_findings()))

    def leaf(self, prim, has, text, token_ok, path, typed=None):
        """One primitive value: has (present and not null), its text, and whether its token type is right."""
        valid = token_ok if prim == "boolean" and typed is not None else (token_ok & C.valid_text(prim, text))
        value = typed if typed is not None else C.stored_value(prim, text)
        bad = has & ~valid
        if self.mode == "store":
            return Out(F.when(has, value), ~bad)
        kind = F.when(~token_ok, F.lit("type")).when(F.length(text) == 0, F.lit("empty")).otherwise(F.lit("invalid"))
        fs = F.when(bad, F.array(F.struct(path.alias("path"), kind.alias("kind"), text.cast("string").alias("value")))) \
            .otherwise(C.no_findings())
        return Out(F.when(has & valid, value), None, fs)

    def problem(self, has, kind, text, path, stored_type):
        """A value the layout cannot hold (a shape or type the definitions contradict): stored as null."""
        if self.mode == "store":
            return Out(F.lit(None).cast(stored_type), ~has)
        return Out(F.lit(None).cast(stored_type), None,
                   F.when(has, F.array(C.finding(path, kind, text))).otherwise(C.no_findings()))


def _or_empty(fs):
    return fs if fs is not None else C.no_findings()


def _stored(e, child_type=None):
    t = C.stored_type(e.prim) if e.prim else child_type
    return ArrayType(t, True) if e.repeating else t


# ---------------------------------------------------------------------------------------- A+ JSON adapter

class MarkedJson:
    """Reads the inferred structs of Spike08Marker's output. A number is a string marked \\u001e#; a string
    that could be mistaken for another token's text is marked \\u001e$; so every value of a column that
    widened to string can be told apart: unmarked text starting with { or [ is a structure, unmarked true or
    false is a boolean, anything else is a string."""

    def element(self, gen, node, e, stype, acc, path, ctx):
        f = _field(stype, e.name)
        meta = _field(stype, "_" + e.name) if e.prim else None
        if f is None and meta is None:
            return None
        if f is None:
            # Only the primitive's id and extensions: the element exists, its value is null (as toLayout).
            return Out(F.lit(None).cast(_stored(e)), None, None)
        col, t = acc(e.name), f.dataType
        if e.prim:
            meta_col = acc("_" + e.name) if meta is not None else None
            return self.primitive(gen, e, col, t, path, meta_col)
        child = node.child(e)
        if isinstance(t, StructType):
            return gen.struct(child, t, col, path, ctx) if not e.repeating else \
                gen.problem(col.isNotNull(), "shape", F.to_json(col), path, _stored(e, StringType()))
        if isinstance(t, ArrayType) and isinstance(t.elementType, StructType):
            if not e.repeating:
                return gen.problem(col.isNotNull(), "shape", F.to_json(col), path, StringType())
            return gen.each(col, lambda x, p: _require(gen.struct(child, t.elementType, x, p, ctx), x), path)
        if isinstance(t, StringType):
            return self.widened(gen, node, e, child, col, path, ctx)
        if isinstance(t, ArrayType) and isinstance(t.elementType, StringType) and e.repeating:
            rt = ctx["resolved"].get(gen.key(path) + "[]")
            def element(x, p):
                ok_shape = x.startswith("{")
                if rt is None:
                    return gen.problem(x.isNotNull(), "type", x, p, StringType())
                parsed = F.from_json(F.when(ok_shape, x), rt)
                inner = gen.struct(child, rt, parsed, p, ctx)
                return _merge(gen, inner, gen.problem(x.isNotNull() & ~ok_shape, "type", x, p, StringType()))
            return gen.each(col, element, path)
        return gen.problem(col.isNotNull(), "type", col.cast("string"), path, StringType())

    def widened(self, gen, node, e, child, col, path, ctx):
        """A complex path that widened to text: each row's value is resolved with from_json where it has the
        shape the definitions declare, and is a finding where it does not."""
        rt = ctx["resolved"].get(gen.key(path))
        expected = col.startswith("[" if e.repeating else "{")
        mismatch = gen.problem(col.isNotNull() & ~expected, "shape", col, path, StringType())
        if rt is None:
            return mismatch
        parsed = F.from_json(F.when(expected, col), rt)
        if e.repeating:
            inner = gen.each(parsed, lambda x, p: _require(gen.struct(child, rt.elementType, x, p, ctx), x), path)
        else:
            inner = gen.struct(child, rt, parsed, path, ctx)
        return _merge(gen, inner, mismatch)

    def primitive(self, gen, e, col, t, path, meta_col):
        if isinstance(t, ArrayType):
            if not e.repeating:
                return gen.problem(col.isNotNull(), "shape", F.to_json(col), path, _stored(e))
            et = t.elementType
            def element(x, p):
                null_ok = meta_col.isNotNull() if meta_col is not None else F.lit(False)
                o = self.scalar(gen, e.prim, x, et, p)
                # A null pads a repeating primitive only where its metadata array is there to align with.
                pad = gen.problem(x.isNull() & ~null_ok, "empty", F.lit("null"), p, C.stored_type(e.prim))
                return _merge(gen, o, pad)
            return gen.each(col, element, path)
        if e.repeating:
            return gen.problem(col.isNotNull(), "shape", col.cast("string"), path, _stored(e))
        return self.scalar(gen, e.prim, col, t, path)

    @staticmethod
    def scalar(gen, prim, x, t, path):
        if isinstance(t, BooleanType):
            if prim == "boolean":
                return gen.leaf(prim, x.isNotNull(), x.cast("string"), F.lit(True), path, typed=x)
            return gen.leaf(prim, x.isNotNull(), x.cast("string"), F.lit(False), path)
        if not isinstance(t, StringType):
            return gen.problem(x.isNotNull(), "type", F.to_json(F.array(x)), path, C.stored_type(prim))
        number, mstr = x.startswith(MARK_NUM), x.startswith(MARK_STR)
        text = F.when(number | mstr, x.substr(F.lit(3), F.length(x))).otherwise(x)
        if prim in C.NUMERIC:
            token = number
        elif prim == "boolean":
            token = ~number & ~mstr & x.isin("true", "false")
        else:
            token = ~number & (mstr | ~(x.startswith("{") | x.startswith("[") | x.isin("true", "false")))
        return gen.leaf(prim, x.isNotNull(), text, token, path)

    def extras(self, gen, node, stype, acc, path, ctx, root):
        out = []
        for f in stype.fields:
            name = f.name
            if root and name in ("resourceType", PRE_F, PRE_D):
                continue
            e = node.entry(name)
            if e is not None and e.is_element:
                continue
            if name.startswith("_") and node.entry(name[1:]) is not None and node.entry(name[1:]).prim:
                continue
            col = acc(name)
            if root and name == "contained":
                if gen.mode != "store":
                    out.append((None, F.when(col.isNotNull(), F.array(C.finding(C.child_path(path, name), "contained", F.lit("not stored")))).otherwise(C.no_findings())))
                continue
            value = col if isinstance(f.dataType, StringType) else F.to_json(F.array(col))
            o = gen.problem(col.isNotNull(), "undescribed", value, C.child_path(path, name), StringType())
            out.append((o.ok, o.fs))
        if root and _field(stype, PRE_F) is not None and gen.mode != "store":
            # The pre-pass's own findings (under drop only: under fail it throws).
            pre = F.transform(F.col(PRE_F), lambda s: F.struct(
                F.split(s, "\\|").getItem(0).alias("path"), F.split(s, "\\|").getItem(1).alias("kind"),
                F.split(s, "\\|").getItem(2).alias("value")))
            out.append((None, F.coalesce(pre, C.no_findings())))
        return out


def _require(o, x):
    if o is None:
        return Out(F.lit(None), None, None)
    if o.stored is NO_COLUMN:
        return Out(F.lit(None), o.ok, o.fs)
    return o


def _merge(gen, a, b):
    """Combines two plans of the same value: a's stored value, both validities, both findings."""
    if a is None:
        return b
    ok = C.all_of([a.ok, b.ok])
    fs = C.concat_findings([a.fs, b.fs]) if gen.mode != "store" else None
    return Out(a.stored, ok, fs)


# ---------------------------------------------------------------------------------------- XML adapter

class XmlText:
    """Reads the structs from_xml parses with every value as text: a primitive is a struct with _value (and
    _id and extension), Element.id and Extension.url are attributes, a repeating element that occurs once
    is a single struct, which is conformant XML, and a singular element given twice is an array."""

    def element(self, gen, node, e, stype, acc, path, ctx):
        if node.is_attribute(e):
            f = _field(stype, "_" + e.name)
            if f is None:
                return None
            x = acc("_" + e.name)
            return gen.leaf(e.prim, x.isNotNull(), x, F.lit(True), path)
        f = _field(stype, e.name)
        if f is None:
            return None
        col, t = acc(e.name), f.dataType
        if e.prim == "xhtml":
            return Out(ctx["narrative"], None, None) if ctx.get("narrative") is not None else None
        inner = _inner(t)
        if isinstance(t, ArrayType) and not e.repeating:
            # The column is an array only because some row repeats the element; per row, one occurrence is
            # the value, and more than one is a finding for that row alone.
            single = F.when(F.size(col) == 1, F.try_element_at(col, F.lit(1)))
            many = gen.problem(F.size(col) > 1, "shape", F.lit("a singular element given more than once"),
                               path, _stored(e) if e.prim else StringType())
            if not isinstance(inner, StructType):
                return many
            if e.prim:
                o = self.primitive(gen, e.prim, single, inner, path)
            else:
                o = _require(gen.struct(node.child(e), inner, single, path, ctx), single)
            return _merge(gen, o, many)
        if e.prim:
            if not isinstance(inner, StructType):
                return gen.problem(col.isNotNull(), "type", col.cast("string"), path, _stored(e))
            urns = ctx.get("urns") if node.fhir_type == "Reference" and e.name == "reference" else None
            plan = lambda x, p: self.primitive(gen, e.prim, x, inner, p, urns)
        else:
            if not isinstance(inner, StructType):
                return gen.problem(col.isNotNull(), "type", col.cast("string"), path, StringType())
            child = node.child(e)
            plan = lambda x, p: _require(gen.struct(child, inner, x, p, ctx), x)
        if isinstance(t, ArrayType):
            return gen.each(col, plan, path)
        if e.repeating:
            o = plan(col, gen.index(path, F.lit(0)))
            return Out(F.when(col.isNotNull(), F.array(o.stored)), o.ok, o.fs)
        return plan(col, path)

    @staticmethod
    def primitive(gen, prim, x, t, path, urns=None):
        has_value = _field(t, "_value") is not None
        has_meta = _field(t, "extension") is not None
        text = x["_value"] if has_value else F.lit(None).cast("string")
        if urns is not None:
            # FR-062: a reference to another entry by its urn: fullUrl becomes that entry's relative reference.
            text = F.when(text.startswith("urn:"), F.coalesce(F.try_element_at(urns, text), text)).otherwise(text)
        meta = x["extension"].isNotNull() if has_meta else F.lit(False)
        o = gen.leaf(prim, text.isNotNull(), text, F.lit(True), path)
        empty = gen.problem(x.isNotNull() & text.isNull() & ~meta, "empty", F.lit("no value"), path, C.stored_type(prim))
        extra = []
        for f in t.fields:
            if f.name == "_text":
                extra.append(gen.problem(x["_text"].isNotNull(), "type", x["_text"], path, StringType()))
            elif f.name not in ("_value", "_id", "extension"):
                extra.append(gen.problem(x[f.name].isNotNull(), "undescribed", F.lit(f.name), C.child_path(path, f.name), StringType()))
        out = _merge(gen, o, empty)
        for p in extra:
            out = _merge(gen, out, p)
        return out

    def extras(self, gen, node, stype, acc, path, ctx, root):
        out = []
        for f in stype.fields:
            name = f.name
            if name == "_xmlns":
                continue
            if name.startswith("_"):
                e = node.entry(name[1:])
                if e is not None and node.is_attribute(e):
                    continue
            else:
                e = node.entry(name)
                if e is not None and e.is_element and not node.is_attribute(e):
                    continue
            col = acc(name)
            if root and name == "contained":
                if gen.mode != "store":
                    out.append((None, F.when(col.isNotNull(), F.array(C.finding(C.child_path(path, name), "contained", F.lit("not stored")))).otherwise(C.no_findings())))
                continue
            o = gen.problem(col.isNotNull(), "undescribed", F.lit(name), C.child_path(path, name.lstrip("_")), StringType())
            out.append((o.ok, o.fs))
        return out


# ---------------------------------------------------------------------------------------- variant adapter

class Variant:
    """Reads parse_json's variant. An object is a map<string, variant>, so its keys can be checked against
    the definitions; a scalar's token type comes from schema_of_variant; null is told from absent."""

    def element(self, gen, node, e, ptree, acc, path, ctx):
        p = ptree.get(e.name) if ptree else None
        meta = ptree.get("_" + e.name) if (ptree and e.prim) else None
        if p is None and meta is None:
            return None
        if p is None:
            return Out(F.lit(None).cast(_stored(e)), None, None)
        x = acc(e.name)
        has = x.isNotNull() & ~F.is_variant_null(x)
        null = gen.problem(x.isNotNull() & F.is_variant_null(x), "empty", F.lit("null"), path,
                           _stored(e) if e.prim else StringType())
        if e.repeating:
            arr = F.try_variant_get(x, "$", "array<variant>")
            shape = _merge(gen, gen.problem(has & arr.isNull(), "shape", F.to_json(x), path, StringType()),
                           gen.problem(arr.isNotNull() & (F.size(arr) == 0), "empty", F.lit("[]"), path, StringType()))
            if e.prim:
                meta_col = acc("_" + e.name) if meta is not None else None
                def plan(y, q):
                    o = self.scalar(gen, e.prim, y, q)
                    pad_ok = meta_col.isNotNull() if meta_col is not None else F.lit(False)
                    pad = gen.problem(F.is_variant_null(y) & ~pad_ok, "empty", F.lit("null"), q, C.stored_type(e.prim))
                    return _merge(gen, o, pad)
            else:
                child, cp = node.child(e), _element_tree(p)
                if cp is None:
                    return _merge(gen, Out(NO_COLUMN), _merge(gen, shape, null))
                def plan(y, q):
                    return _require(self.object(gen, child, cp, y, q, ctx), y)
            o = gen.each(arr, plan, path)
            return _merge(gen, _merge(gen, o, shape), null)
        if e.prim:
            o = self.scalar(gen, e.prim, x, path)
            return _merge(gen, o, null)
        child = node.child(e)
        if not isinstance(p, dict):
            return _merge(gen, _merge(gen, Out(NO_COLUMN), gen.problem(has, "type", F.to_json(x), path, StringType())), null)
        o = self.object(gen, child, p, x, path, ctx)
        return _merge(gen, o, null)

    def object(self, gen, node, ptree, x, path, ctx):
        m = F.try_variant_get(x, "$", "map<string,variant>")
        has = x.isNotNull() & ~F.is_variant_null(x)
        cols, ok, fs = gen.node(node, ptree, lambda n: F.try_element_at(m, F.lit(n)), path, dict(ctx, _map=m))
        bad = gen.problem(has & m.isNull(), "type", F.to_json(x), path, StringType())
        empty = gen.problem(has & m.isNotNull() & (F.size(m) == 0), "empty", F.lit("{}"), path, StringType())
        if not cols:
            checks = Out(NO_COLUMN, ok, fs)
            return _merge(gen, _merge(gen, checks, bad), empty)
        o = Out(F.when(m.isNotNull() & (F.size(m) > 0), F.struct(*cols)),
                None if ok is None else (m.isNull() | ok),
                None if fs is None else F.when(m.isNotNull(), fs).otherwise(C.no_findings()))
        return _merge(gen, _merge(gen, o, bad), empty)

    @staticmethod
    def scalar(gen, prim, y, path):
        kind = F.schema_of_variant(y)
        text = F.try_variant_get(y, "$", "string")
        if prim in C.INTEGRAL:
            token = kind.isin("BIGINT", "INT", "SMALLINT", "TINYINT")
        elif prim == "decimal":
            token = kind.startswith("DECIMAL") | kind.isin("BIGINT", "INT", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT")
        elif prim == "boolean":
            token = kind == "BOOLEAN"
            return gen.leaf(prim, y.isNotNull() & ~F.is_variant_null(y), F.coalesce(text, F.to_json(y)), token,
                            path, typed=F.try_variant_get(y, "$", "boolean"))
        else:
            token = kind == "STRING"
        return gen.leaf(prim, y.isNotNull() & ~F.is_variant_null(y), F.coalesce(text, F.to_json(y)), token, path)

    def extras(self, gen, node, ptree, acc, path, ctx, root):
        m = ctx.get("_map")
        if m is None:
            return []
        allowed = {e.name for e in node.entries if e.is_element} | {"_" + e.name for e in node.entries if e.is_element and e.prim}
        if root:
            allowed |= {"resourceType"}
        allowed_arr = F.array(*[F.lit(a) for a in sorted(allowed)])
        extra = F.filter(F.map_keys(m), lambda k: ~F.array_contains(allowed_arr, k) & (k != F.lit("contained")))
        out = []
        if gen.mode == "store":
            out.append((m.isNull() | (F.size(extra) == 0), None))
        else:
            out.append((None, F.when(m.isNotNull(), F.transform(extra, lambda k: F.struct(
                F.concat(path, F.lit("."), k).alias("path"), F.lit("undescribed").alias("kind"),
                F.to_json(F.try_element_at(m, k)).alias("value")))).otherwise(C.no_findings())))
            if root:
                out.append((None, F.when(F.try_element_at(m, F.lit("contained")).isNotNull(), F.array(
                    C.finding(C.child_path(path, "contained"), "contained", F.lit("not stored")))).otherwise(C.no_findings())))
        return out


def _element_tree(p):
    """The presence of an array's elements: the object tree under ARRAY<OBJECT<...>>."""
    if isinstance(p, tuple) and p[0] == "ARRAY":
        return p[1] if isinstance(p[1], dict) else None
    return None


def parse_variant_type(s):
    """Parses schema_of_variant's type text into dicts (objects), ('ARRAY', element) and strings (scalars)."""
    pos = 0

    def ws():
        nonlocal pos
        while pos < len(s) and s[pos] == " ":
            pos += 1

    def name():
        nonlocal pos
        ws()
        if s[pos] == "`":
            end = s.index("`", pos + 1)
            while end + 1 < len(s) and s[end + 1] == "`":
                end = s.index("`", end + 2)
            n = s[pos + 1:end].replace("``", "`")
            pos = end + 1
            return n
        start = pos
        while s[pos] not in ":":
            pos += 1
        return s[start:pos].strip()

    def typ():
        nonlocal pos
        ws()
        if s.startswith("OBJECT<", pos):
            pos += 7
            out = {}
            ws()
            if s[pos] == ">":
                pos += 1
                return out
            while True:
                n = name()
                ws()
                pos += 1  # ':'
                out[n] = typ()
                ws()
                if s[pos] == ",":
                    pos += 1
                    continue
                pos += 1  # '>'
                return out
        if s.startswith("ARRAY<", pos):
            pos += 6
            el = typ()
            ws()
            pos += 1
            return ("ARRAY", el)
        start = pos
        depth = 0
        while pos < len(s) and (depth > 0 or s[pos] not in ",>"):
            if s[pos] == "(":
                depth += 1
            elif s[pos] == ")":
                depth -= 1
            pos += 1
        return s[start:pos].strip()

    return typ()


# ---------------------------------------------------------------------------------------- the routes

class _Route:
    def __init__(self, pc):
        self.pc, self.spark = pc, pc.spark
        self.jvm = self.spark._jvm
        self.xml = XmlRoute(pc)  # The definitions, over py4j, cached for the process.
        self.policy = os.environ.get("SPIKE_POLICY", "fail")
        self.findings_column = os.environ.get("SPIKE_FINDINGS") == "1"
        self.plan_ms = 0.0
        self.undescribed = []

    def strings(self, df):
        return getattr(df.select(df.columns[0])._jdf, "as")(self.jvm.org.apache.spark.sql.Encoders.STRING())

    def finish(self, df, node, stype, acc, path, ctx, adapter, keep=None):
        """Builds the stored dataset from a source: the stored columns, and under fail a filter that raises
        on the first invalid row, naming the resource, the path and the value; under drop the findings."""
        if self.policy == "fail":
            cols, ok, _ = Gen(adapter, "store").node(node, stype, acc, path, ctx, root=True)
            _, _, fs = Gen(adapter, "findings").node(node, stype, acc, path, ctx, root=True)
            if ok is not None:
                message = F.concat(F.lit("Invalid content in " + ctx["type"] + "/"),
                                   F.coalesce(ctx.get("id", F.lit("?")).cast("string"), F.lit("?")),
                                   F.lit(": "), F.to_json(F.slice(_or_empty(fs), 1, 1)))
                df = df.filter(F.when(ok, F.lit(True)).otherwise(F.raise_error(message).cast("boolean")))
            return df.select(*cols)
        cols, _, fs = Gen(adapter, "drop").node(node, stype, acc, path, ctx, root=True)
        if self.findings_column:
            cols = cols + [_or_empty(fs).alias("_findings")]
        return df.select(*cols)


class APlusRoute(_Route):
    def fit(self, source, raw, t):
        if source == "json":
            return self.fit_json(raw, t)
        return self.fit_xml(raw, t, bundles=source != "xml-lines")

    # --- JSON ---
    def fit_json(self, raw, t):
        col = raw.columns[0]
        rtype = F.get_json_object(F.col(col), "$.resourceType")
        docs = raw.filter(rtype.isNull() | (rtype == t)).select(F.col(col).alias("value"))
        marked = self.jvm.au.csiro.pathling.io.Spike08Marker.mark(self.strings(docs), self.policy)
        reader = self.spark._jsparkSession.read().option("mode", "FAILFAST")
        inferred = StructType.fromJson(json.loads(reader.json(marked).schema().json()))   # The fit pass.
        start = time.perf_counter()
        node = self.xml.node(t)
        typed = DataFrame(reader.schema(self.jvm.org.apache.spark.sql.types.DataType.fromJson(inferred.json())).json(marked), self.spark)
        if _field(inferred, PRE_D) is not None:
            typed = typed.filter(F.col(PRE_D).isNull())
        if _field(inferred, "value") is None:
            typed = typed.withColumn("value", F.lit(""))
        ctx = {"type": t, "resolved": {}, "id": F.col("id") if _field(inferred, "id") is not None else F.lit("?")}
        self.plan_ms = (time.perf_counter() - start) * 1000
        self._resolve(typed, node, inferred, ctx)   # Part of the fit: may run one job per widened path.
        start = time.perf_counter()
        out = self.finish(typed, node, inferred, lambda n: F.col(f"`{n}`"), F.lit(t), ctx, MarkedJson())
        self.plan_ms += (time.perf_counter() - start) * 1000
        return out

    def _resolve(self, typed, node, inferred, ctx, prefix=None, parent=None):
        """For each complex path that widened to text, infers the type of its values that have the shape
        the definitions declare. Paths below an array are not resolved (a prototype limit)."""
        for f in inferred.fields:
            e = node.entry(f.name)
            if e is None or not e.is_element or e.prim:
                continue
            key = (prefix + "." if prefix else "") + f.name
            column = (parent[f.name] if parent is not None else F.col(f"`{f.name}`"))
            t = f.dataType
            if isinstance(t, StringType):
                expected = "[" if e.repeating else "{"
                texts = typed.select(column.alias("j")).where(F.col("j").startswith(expected))
                if texts.limit(1).count() == 0:
                    continue
                st = StructType.fromJson(json.loads(self.spark._jsparkSession.read().json(self.strings(texts)).schema().json()))
                ctx["resolved"][key] = ArrayType(st, True) if e.repeating else st
            elif isinstance(t, ArrayType) and isinstance(t.elementType, StringType) and e.repeating:
                texts = typed.select(F.explode(column).alias("j")).where(F.col("j").startswith("{"))
                if texts.limit(1).count() > 0:
                    st = StructType.fromJson(json.loads(self.spark._jsparkSession.read().json(self.strings(texts)).schema().json()))
                    ctx["resolved"][key + "[]"] = st
            elif isinstance(t, StructType):
                self._resolve(typed, node.child(e), t, ctx, key, column)

    # --- XML ---
    def fit_xml(self, raw, t, bundles):
        docs = raw.select(F.col(raw.columns[0]).alias("value"))
        check = "spike08_xml_doc"
        self.spark._jsparkSession.udf().register(check, self.jvm.au.csiro.pathling.io.Spike08XmlDocCheck(),
                                                 self.jvm.org.apache.spark.sql.types.DataTypes.StringType)
        root = "Bundle" if bundles else t
        # A prefixed FHIR namespace is rewritten to the default one before parsing, since from_xml keeps
        # prefixes in field names. Only documents whose root carries a prefix pay for the rewrite.
        prefixed = r"^\s*(<\?xml[^>]*\?>\s*)?<([A-Za-z_][\w.-]*):" + root + r"[\s/>]"
        pfx = F.regexp_extract(F.col("value"), prefixed, 2)
        unprefixed = F.when(pfx != "", F.regexp_replace(F.regexp_replace(F.regexp_replace(
            F.col("value"), F.concat(F.lit("(</?)"), pfx, F.lit(":")), F.lit("$1")),
            F.concat(F.lit("xmlns:"), pfx, F.lit("=")), F.lit("xmlns=")), F.lit("\\u0000"), F.lit(""))).otherwise(F.col("value"))
        selected = docs.select(unprefixed.alias("value")).filter(
            F.col("value").rlike(r"^\s*(<\?xml[^>]*\?>\s*)?<" + root + r"[\s/>]"))
        presence = self.xml.infer(selected, root)   # The fit pass.
        start = time.perf_counter()
        node = self.xml.node(t)
        checked = selected.withColumn("_doc", F.expr(f"{check}(value)"))
        if bundles:
            out = self._bundles(checked, presence, node, t)
        else:
            parse = _parse_schema(self.xml, node, presence)
            parsed = checked.select(F.from_xml("value", parse, {"ignoreSurroundingSpaces": "false", "mode": "FAILFAST", "valueTag": "_text"}).alias("r"), "value", "_doc")
            ctx = {"type": t}
            text = _field(parse, "text")
            if text is not None and _field(text.dataType, "div") is not None:
                narratives = F.regexp_extract_all(F.col("value"), F.lit(NARRATIVE), 1)
                ctx["narrative"] = F.when(F.col("r.text.div").isNotNull() & (F.size(narratives) == 1),
                                          XmlRoute._unescape(F.try_element_at(narratives, F.lit(1))))
            out = self._finish_xml(parsed, node, parse, lambda n: F.col("r")[n], ctx)
        self.plan_ms = (time.perf_counter() - start) * 1000
        return out

    def _finish_xml(self, parsed, node, parse, acc, ctx):
        doc_ok = F.col("_doc").isNull()
        if self.policy == "fail":
            parsed = parsed.filter(F.when(doc_ok, F.lit(True)).otherwise(
                F.raise_error(F.concat(F.lit("Invalid " + ctx["type"] + " document: "), F.col("_doc"))).cast("boolean")))
        else:
            parsed = parsed.filter(doc_ok)
        ctx["id"] = acc("id")["_value"] if _field(parse, "id") is not None and isinstance(_inner(_field(parse, "id").dataType), StructType) else F.lit("?")
        return self.finish(parsed, node, parse, acc, F.lit(ctx["type"]), ctx, XmlText())

    def _bundles(self, checked, presence, node, t):
        entry_field = _field(presence, "entry")
        resource_field = _field(_inner(entry_field.dataType), "resource")
        types = [f.name for f in resource_field.dataType.fields]
        target_type = None
        resource_types = []
        for f in resource_field.dataType.fields:
            if f.name == t:
                target_type = _parse_schema(self.xml, node, f.dataType)
                resource_types.append(StructField(f.name, target_type, True))
            else:
                fields = [StructField("id", StructType([StructField("_value", StringType())]))]
                text = _field(f.dataType, "text")
                if text is not None and _field(text.dataType, "div") is not None:
                    fields.append(StructField("text", StructType([StructField("div", StringType())])))
                resource_types.append(StructField(f.name, StructType(fields), True))
        full_url = _field(_inner(entry_field.dataType), "fullUrl") is not None
        entry_type = StructType(([StructField("fullUrl", StructType([StructField("_value", StringType())]))] if full_url else [])
                                + [StructField("resource", StructType(resource_types))])
        parse = StructType([StructField("entry", ArrayType(entry_type))])
        b = F.col("b")
        indexed = F.transform(b["entry"], lambda e, i: F.struct(i.alias("i"), e.alias("e")))
        per_bundle = []
        if full_url:
            pairs = F.transform(b["entry"], lambda e: F.struct(e["fullUrl"]["_value"].alias("k"), F.coalesce(*[
                F.when(e["resource"][u]["id"]["_value"].isNotNull(), F.concat(F.lit(u + "/"), e["resource"][u]["id"]["_value"]))
                for u in types]).alias("v")))
            per_bundle.append(F.map_from_entries(F.filter(pairs, lambda p: p["k"].startswith("urn:") & p["v"].isNotNull())).alias("urns"))
        div_types = [f.name for f in resource_types if _field(f.dataType, "text") is not None and _field(_field(f.dataType, "text").dataType, "div") is not None]
        if t in div_types:
            with_div = F.filter(indexed, lambda x: F.coalesce(*[x["e"]["resource"][u]["text"]["div"].isNotNull() for u in div_types], F.lit(False)))
            narratives = F.regexp_extract_all(F.col("value"), F.lit(NARRATIVE), 1)
            per_bundle += [F.map_from_entries(F.transform(with_div, lambda x, k: F.struct(x["i"], k))).alias("ordinal"),
                           narratives.alias("narratives"), (F.size(narratives) == F.size(with_div)).alias("aligned")]
        parsed = checked.select(F.from_xml("value", parse, {"ignoreSurroundingSpaces": "false", "mode": "FAILFAST", "valueTag": "_text"}).alias("b"), "value", "_doc")
        if per_bundle:
            parsed = parsed.select("b", "value", "_doc", F.explode(F.array(F.struct(*per_bundle))).alias("bundle"))
        selected = F.filter(indexed, lambda x: x["e"]["resource"][t].isNotNull())
        extra = ["bundle"] if per_bundle else []
        rows = parsed.select(F.explode(F.transform(selected, lambda x: F.struct(
            x["e"]["resource"][t].alias("r"), x["i"].alias("i")))).alias("x"), "value", "_doc", *extra) \
            .select(F.col("x.r").alias("r"), F.col("x.i").alias("i"), "value", "_doc", *extra)
        ctx = {"type": t}
        if full_url:
            ctx["urns"] = F.col("bundle.urns")
        if t in div_types:
            at = F.try_element_at(F.col("bundle.ordinal"), F.col("i")) + 1
            ctx["narrative"] = F.when(F.col("bundle.aligned"), XmlRoute._unescape(F.try_element_at(F.col("bundle.narratives"), at)))
        return self._finish_xml(rows, node, target_type, lambda n: F.col("r")[n], ctx)

    def decode(self, source, stored, t):
        start = time.perf_counter()
        node = self.xml.node(t)
        if source == "json":
            out = egress_json(stored, node, t)
        else:
            out = egress_xml(stored, node, t)
        self.plan_ms = (time.perf_counter() - start) * 1000
        return out


def _parse_schema(route, node, presence):
    """The from_xml parse schema for A+: everything the documents carry, kept (so that undescribed content is
    found per value), with every described value as text and the narrative as a string."""
    return _keep(route, node, presence)


def _keep(route, node, observed):
    kept = []
    for f in observed.fields:
        name = f.name
        e = node.entry(name) if not name.startswith("_") else None
        inner = _inner(f.dataType)
        if e is not None and e.is_element and not node.is_attribute(e):
            if e.prim == "xhtml":
                kept.append(StructField(name, StringType(), True))
            elif e.prim:
                # The definitions fix a primitive's shape, so it is parsed as that shape whatever inference
                # merged: a row with text content (_VALUE) would otherwise turn the column into text and lose
                # every other row's value attribute. Text content is read as _text (the valueTag option), since
                # Spark's default, _VALUE, is the same name as _value to its case-insensitive resolution.
                base = {g.name: g for g in inner.fields} if isinstance(inner, StructType) else {}
                t = StructType([StructField(n, StringType(), True) for n in ("_value", "_id", "_text")]
                               + [g for n, g in base.items() if n.lower() not in ("_value", "_id", "_text")])
                many = isinstance(f.dataType, ArrayType) or e.repeating
                kept.append(StructField(name, ArrayType(t, True) if many else t, True))
            elif isinstance(inner, StructType):
                t = _keep(route, node.child(e), inner)
                kept.append(StructField(name, ArrayType(t, True) if isinstance(f.dataType, ArrayType) else t, True))
            else:
                kept.append(f)
        elif name == "_xmlns":
            continue
        elif name == "_VALUE":
            kept.append(StructField("_text", f.dataType, True))
        else:
            kept.append(f)
    return StructType(kept)


# ---------------------------------------------------------------------------------------- egress (A+)

def _egress_fields(node, stype, acc, xml):
    cols, names = [], []
    for f in stype.fields:
        e = node.entry(f.name)
        if e is None or e.is_resource_type or not e.is_element:
            continue
        col = acc(f.name)
        if xml and node.is_attribute(e):
            cols.append(col.cast("string").alias("_" + f.name))
            names.append("_" + f.name)
            continue
        if e.prim == "xhtml":
            cols.append((F.when(col.isNotNull(), F.lit(DIV_PLACEHOLDER)) if xml else col).alias(f.name))
            names.append(f.name)
            continue
        if e.prim:
            one = lambda v, p=e.prim: _prim_out(p, v, xml)
        else:
            child, inner = node.child(e), _inner(f.dataType)
            one = _struct_out(child, inner, xml)
        if isinstance(f.dataType, ArrayType):
            out = F.transform(col, lambda v: one(v))
        else:
            out = one(col)
        cols.append(out.alias(f.name))
        names.append(f.name)
    return cols, names


def _struct_out(child, inner, xml):
    """A stored structure that is null stays null; one whose every field is null, and an array holding
    nulls, pass through as they are (decision 71), as A0's egress does."""
    def one(v):
        cols, _ = _egress_fields(child, inner, lambda n: v[n], xml)
        return F.when(v.isNotNull(), F.struct(*cols)) if cols else F.lit(None)
    return one


def _prim_out(prim, v, xml):
    if prim == "base64Binary":
        text = F.regexp_replace(F.base64(v), "[\\r\\n]", "")
    elif prim == "decimal":
        text = v
    elif prim == "boolean" and xml:
        text = F.when(v, F.lit("true")).otherwise(F.lit("false"))
    else:
        text = v.cast("string") if xml else v
    if xml:
        return F.when(v.isNotNull(), F.struct(text.alias("_value")))
    if prim == "decimal":
        # Marked, and unmarked after to_json, so the decimal is written with the text it is stored as.
        return F.when(v.isNotNull(), F.struct(v.alias("$decimal$")))
    return text


def egress_json(stored, node, t):
    cols, _ = _egress_fields(node, stored.schema, lambda n: F.col(f"`{n}`"), False)
    doc = F.to_json(F.struct(F.lit(t).alias("resourceType"), *cols))
    return stored.select(F.regexp_replace(doc, r"\{\"\$decimal\$\":\"(-?[0-9][0-9.eE+-]*)\"\}", "$1").alias("value"))


def egress_xml(stored, node, t):
    cols, _ = _egress_fields(node, stored.schema, lambda n: F.col(f"`{n}`"), True)
    xml = F.to_xml(F.struct(F.lit(FHIR_NS).alias("_xmlns"), *cols), {"rowTag": t, "indent": ""})
    xml = F.regexp_replace(xml, r"\n\z", "")
    text = _field(stored.schema, "text")
    if text is not None and _field(text.dataType, "div") is not None:
        xml = F.when(F.col("text.div").isNotNull(),
                     F.replace(xml, F.lit(f"<div>{DIV_PLACEHOLDER}</div>"), F.col("text.div"))).otherwise(xml)
    return stored.select(xml.alias("value"))


# ---------------------------------------------------------------------------------------- A-variant

class AVariantRoute(_Route):
    def fit(self, source, raw, t):
        assert source == "json", "A-variant is JSON only"
        col = raw.columns[0]
        rtype = F.get_json_object(F.col(col), "$.resourceType")
        docs = raw.filter(rtype.isNull() | (rtype == t)).select(F.col(col).alias("value"))
        check = "spike08_json_doc"
        self.spark._jsparkSession.udf().register(check, self.jvm.au.csiro.pathling.io.Spike08JsonDocCheck(),
                                                 self.jvm.org.apache.spark.sql.types.DataTypes.StringType)
        parse = F.parse_json("value") if self.policy == "fail" else F.try_parse_json("value")
        v = docs.select("value", parse.alias("v"))
        merged = v.select(F.expr("schema_of_variant_agg(v)")).first()[0]   # The fit pass.
        start = time.perf_counter()
        ptree = parse_variant_type(merged) if merged else {}
        node = self.xml.node(t)
        self.plan_ms = (time.perf_counter() - start) * 1000
        ptree = self._resolve(v, node, ptree)   # Part of the fit: one job per path whose types conflict.
        start = time.perf_counter()
        checked = v.withColumn("_doc", F.expr(f"{check}(value)"))
        if self.policy == "fail":
            checked = checked.filter(F.when(F.col("_doc").isNull(), F.lit(True)).otherwise(
                F.raise_error(F.concat(F.lit("Invalid " + t + " document: "), F.col("_doc"))).cast("boolean")))
        else:
            checked = checked.filter(F.col("_doc").isNull() & F.col("v").isNotNull())
        m = F.try_variant_get(F.col("v"), "$", "map<string,variant>")
        ctx = {"type": t, "_map": m, "id": F.try_variant_get(F.col("v"), "$.id", "string")}
        out = self.finish(checked, node, ptree if isinstance(ptree, dict) else {},
                          lambda n: F.try_element_at(m, F.lit(n)), F.lit(t), ctx, Variant())
        self.plan_ms += (time.perf_counter() - start) * 1000
        return out

    def _resolve(self, v, node, ptree, prefix="$"):
        """Where schema_of_variant_agg merged a complex path to VARIANT (its rows disagree), infers the
        presence beneath it from the rows with the shape the definitions declare. Paths below an array are
        not resolved (a prototype limit)."""
        if not isinstance(ptree, dict):
            return ptree
        out = dict(ptree)
        for name, p in ptree.items():
            e = node.entry(name)
            if e is None or not e.is_element or e.prim:
                continue
            path = f"{prefix}.{name}"
            if p == "VARIANT":
                x = F.variant_get(F.col("v"), path, "variant")
                want = "ARRAY" if e.repeating else "OBJECT"
                rows = v.select(x.alias("x")).where(F.schema_of_variant(F.col("x")).startswith(want))
                merged = rows.select(F.expr("schema_of_variant_agg(x)")).first()[0]
                out[name] = parse_variant_type(merged) if merged else None
            elif isinstance(p, dict):
                out[name] = self._resolve(v, node.child(e), p, path)
        return {k: val for k, val in out.items() if val is not None}

    def decode(self, source, stored, t):
        # HEAD's decoder, built in the JVM: no py4j plan building to record.
        self.plan_ms = 0.0
        return DataFrame(self.pc._jpc.decode(stored._jdf, t, "application/fhir+json"), self.spark)


# ---------------------------------------------------------------------------------------- B

class BRoute(_Route):
    def fit(self, source, raw, t):
        fmt = {"json": "json", "xml-lines": "xml"}.get(source, "xml-bundle")
        ctx = self.pc._jpc.getFhirEncoders().getContext()
        return DataFrame(self.jvm.au.csiro.pathling.io.Spike08B.fit(
            self.strings(raw), ctx, t, fmt, self.policy, self.findings_column), self.spark)

    def decode(self, source, stored, t):
        ctx = self.pc._jpc.getFhirEncoders().getContext()
        return DataFrame(self.jvm.au.csiro.pathling.io.Spike08B.decode(
            stored._jdf, ctx, t, "json" if source == "json" else "xml"), self.spark)


ROUTES = {"aplus": APlusRoute, "avariant": AVariantRoute, "b": BRoute}
