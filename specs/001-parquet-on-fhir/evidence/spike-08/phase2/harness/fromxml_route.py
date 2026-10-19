"""Throwaway prototype for spike 08: FHIR XML in and out of the Parquet on FHIR layout with Spark's own XML
support (from_xml and to_xml), and no FHIR parser in the per-row plan.

This is the A0 XML route of ticket 08: the route xml-native.md describes, extended from its Patient subset to
any resource type, generated from the definitions with nothing written per type, and applying no per-value
validation. It runs against an unmodified Pathling jar, reaching the definitions and the package-private
ResourceTransformer over py4j.

Ingest (fit, then encode):
  1. Presence. Spark's XML reader infers a schema with inferSchema=false over the documents (one full scan),
     so every value is a string and only the shape is learned. This is the fit pass.
  2. Prune. The inferred schema is walked beside the definitions. What they describe is kept; anything else
     (contained, unknown elements, the namespace attribute) is dropped and listed. Narrative.div is parsed as
     a plain string, which serves only to say whether a narrative is there.
  3. Shape. Generated column expressions turn the from_xml struct into the JSON data model that
     ResourceTransformer.toLayout takes: a value from its `value` attribute, typed as Spark's JSON reader would
     type it (boolean, bigint, double, string); Element.id and Extension.url from their attributes; a single
     occurrence of a repeating element wrapped in an array; a primitive's id and extensions as its `_x` group.
  4. toLayout, unchanged, as the JSON route uses it.

The narrative is copied byte for byte from the raw document (xml-native.md option 3, guarded): every XHTML div
closed immediately before </text> is extracted with a regular expression, and the k-th one is given to the k-th
resource whose parse reports a div. Where the counts disagree (a contained narrative, a comment before
</text>), no narrative is stored for that document.

Bundles: the Bundle is parsed whole with from_xml, keeping the requested type in full and only the id (and
version) of every other entry. A map from each urn: fullUrl to Type/id is built per Bundle, and every
Reference.reference that is a key of it is rewritten, as BundleTransformer does through HAPI.

Egress (decode): generated expressions turn the stored layout into an XML-shaped struct (values in `_value`
attributes, Element.id and Extension.url as attributes, positional nulls dropped), which to_xml writes. The
narrative is written as a placeholder and replaced with the stored text.
"""
import json, time
from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

FHIR_NS = "http://hl7.org/fhir"
DIV_PLACEHOLDER = "__PATHLING_SPIKE_DIV__"
# Every XHTML div closed immediately before </text>, after the Narrative's optional status. Lazy, so a
# nested div does not end the match.
NARRATIVE = (r'<text(?:\s+id="[^"]*")?>\s*(?:<status\s+value="[^"]*"\s*/>\s*)?'
             r'(<div\s[^>]*xmlns="http://www.w3.org/1999/xhtml"[^>]*>[\s\S]*?</div>)\s*</text>')
READ_OPTIONS = {"ignoreSurroundingSpaces": "false", "mode": "FAILFAST"}
INTEGRAL = {"integer", "unsignedInt", "positiveInt", "integer64"}


class Entry:
    """One field of a canonical structure, as the walk needs it."""

    def __init__(self, j):
        self.j = j
        self.name = j.getName()
        self.is_resource_type = j.isResourceType()
        self.is_element = j.isElement()
        self.repeating = j.isRepeating()
        primitive = j.primitiveType()
        self.prim = primitive.get().toCode() if primitive.isPresent() else None
        element = j.getElement()
        fhir_type = element.get().getFhirType() if element.isPresent() else None
        self.fhir_type = fhir_type.get().toCode() if fhir_type is not None and fhir_type.isPresent() else None


class Node:
    """A canonical structure from the definitions, fetched over py4j once and cached for the process."""

    def __init__(self, j, resource=False, fhir_type=None):
        self.j = j
        self.resource = resource
        self.fhir_type = fhir_type
        self._entries = None
        self._children = {}

    @property
    def entries(self):
        if self._entries is None:
            self._entries = [Entry(e) for e in self.j.entries()]
            self._index = {e.name: e for e in self._entries}
        return self._entries

    def entry(self, name):
        self.entries
        return self._index.get(name)

    def child(self, entry):
        if entry.name not in self._children:
            self._children[entry.name] = Node(self.j.elementStructure(entry.j).get(), False, entry.fhir_type)
        return self._children[entry.name]

    def is_attribute(self, entry):
        """Element.id and Extension.url are XML attributes; the id of a resource is an element."""
        return ((entry.name == "id" and not self.resource)
                or (entry.name == "url" and self.fhir_type == "Extension"))

    def extension_node(self):
        """The structure of Extension, for the extensions of a primitive's metadata group."""
        ext = self.entry("extension")
        return self.child(ext) if ext is not None and ext.is_element else None


def _field(struct, name):
    return next((f for f in struct.fields if f.name == name), None) if isinstance(struct, StructType) else None


def _inner(t):
    return t.elementType if isinstance(t, ArrayType) else t


def _rewrap(observed, inner):
    return ArrayType(inner, True) if isinstance(observed, ArrayType) else inner


class XmlRoute:
    def __init__(self, pc):
        self.pc = pc
        self.spark = pc.spark
        jvm = self.spark._jvm
        self.jvm = jvm
        self.definitions = jvm.au.csiro.pathling.definition.fhir.FhirDefinitionContext.of(
            pc._jpc.getFhirEncoders().getContext())
        self.transformer = jvm.au.csiro.pathling.io.ResourceTransformer.of(self.definitions)
        self._resources = {}
        # What the last fit dropped as undescribed, by path, for reporting.
        self.undescribed = []
        # Driver time the last fit or decode spent building its plan in Python over py4j, after the scan. A
        # prototype artefact: HEAD builds its plan in the JVM. Recorded so it can be told apart.
        self.plan_ms = 0.0

    def node(self, resource_type):
        if resource_type not in self._resources:
            j = self.jvm.au.csiro.pathling.schema.DefinitionCanonicalStructure.forResource(
                self.definitions, resource_type)
            self._resources[resource_type] = Node(j, True, resource_type)
        return self._resources[resource_type]

    # ---------------------------------------------------------------- presence (the fit pass's scan)

    def infer(self, documents: DataFrame, row_tag: str) -> StructType:
        """One full scan: Spark's XML inference with every value a string, so only presence is learned."""
        strings = getattr(documents.select(documents.columns[0])._jdf, "as")(
            self.jvm.org.apache.spark.sql.Encoders.STRING())
        reader = self.spark._jsparkSession.read().option("rowTag", row_tag).option("inferSchema", "false")
        for k, v in READ_OPTIONS.items():
            reader = reader.option(k, v)
        return StructType.fromJson(json.loads(reader.xml(strings).schema().json()))

    # ---------------------------------------------------------------- pruning to the parse schema

    def prune(self, node: Node, observed: StructType, path: str) -> StructType:
        """Keeps what the definitions describe, typed as the parse needs it; lists the rest."""
        kept = []
        for f in observed.fields:
            name = f.name
            if name.startswith("_"):
                attribute = name[1:]
                entry = node.entry(attribute)
                if attribute == "value" or (entry is not None and node.is_attribute(entry)):
                    kept.append(StructField(name, StringType(), True))
                elif attribute != "xmlns":
                    self.undescribed.append(f"{path}/@{attribute}")
                continue
            entry = node.entry(name)
            if entry is None or not entry.is_element or node.is_attribute(entry):
                self.undescribed.append(f"{path}/{name}")
                continue
            inner = _inner(f.dataType)
            if entry.prim == "xhtml":
                kept.append(StructField(name, StringType(), True))
            elif entry.prim is not None:
                kept.append(StructField(name, _rewrap(f.dataType, self._prune_primitive(node, inner, f"{path}/{name}")), True))
            elif isinstance(inner, StructType):
                kept.append(StructField(name, _rewrap(f.dataType, self.prune(node.child(entry), inner, f"{path}/{name}")), True))
            else:
                # A complex element holding only text is not FHIR XML. It is kept as text, and the shape
                # step leaves it out.
                kept.append(StructField(name, f.dataType, True))
        return StructType(kept)

    def _prune_primitive(self, parent: Node, observed, path):
        if not isinstance(observed, StructType):
            return observed
        kept = []
        for f in observed.fields:
            if f.name in ("_value", "_id"):
                kept.append(StructField(f.name, StringType(), True))
            elif f.name == "extension" and parent.extension_node() is not None and isinstance(_inner(f.dataType), StructType):
                kept.append(StructField(f.name, _rewrap(f.dataType, self.prune(parent.extension_node(), _inner(f.dataType), f"{path}/extension")), True))
            elif f.name != "_xmlns":
                self.undescribed.append(f"{path}/{f.name}")
        return StructType(kept)

    # ---------------------------------------------------------------- shaping to the JSON data model

    def shape(self, node: Node, parsed: StructType, source, ctx) -> list:
        """The fields of one structure, in the JSON data model, in definition order."""
        columns = []
        for entry in node.entries:
            if entry.is_resource_type:
                # Nullable, as the JSON reader's resourceType column is, so the stored schemas are equal.
                # The analyser folds a constant condition, so the raw document's column carries the nullability.
                columns.append(F.when(F.col("value").isNotNull(), F.lit(ctx["type"])).alias("resourceType"))
                continue
            if not entry.is_element:
                continue
            if node.is_attribute(entry):
                if _field(parsed, "_" + entry.name) is not None:
                    columns.append(source("_" + entry.name).alias(entry.name))
                continue
            f = _field(parsed, entry.name)
            if f is None:
                continue
            if entry.prim == "xhtml":
                if ctx.get("narrative") is not None:
                    columns.append(ctx["narrative"].alias(entry.name))
                continue
            value, group = self._element(node, entry, f.dataType, source(entry.name), ctx)
            if value is not None and node.fhir_type == "Reference" and entry.name == "reference" and ctx.get("urns") is not None:
                value = self._resolved(value, ctx["urns"])
            if value is not None:
                columns.append(value.alias(entry.name))
            if group is not None:
                columns.append(group.alias("_" + entry.name))
        return columns

    def _element(self, node, entry, observed, column, ctx):
        inner = _inner(observed)
        group = None
        if entry.prim is not None:
            if not isinstance(inner, StructType):
                return None, None
            has_value = _field(inner, "_value") is not None
            single = (lambda v: self._primitive(entry.prim, v["_value"])) if has_value else None
            meta = [n for n in ("_id", "extension") if _field(inner, n) is not None]
            if meta:
                ext_node = node.extension_node()

                def metadata(v):
                    fields = []
                    if "_id" in meta:
                        fields.append(v["_id"].alias("id"))
                    if "extension" in meta and ext_node is not None:
                        ext_type = _field(inner, "extension").dataType
                        fields.append(self._wrap(ext_type, True, v["extension"],
                                                 lambda e: self._struct(ext_node, _inner(ext_type), e, ctx)).alias("extension"))
                    return F.when(v.isNotNull(), F.struct(*fields))
                group = self._wrap(observed, entry.repeating, column, metadata)
            if single is None:
                return None, group
            return self._wrap(observed, entry.repeating, column, single), group
        if not isinstance(inner, StructType):
            return None, None
        child = node.child(entry)
        if not inner.fields:
            # The parse schema holds only what the definitions describe, so an empty one has nothing to store.
            return None, None
        return self._wrap(observed, entry.repeating, column, lambda v: self._struct(child, inner, v, ctx)), None

    def _struct(self, node, parsed, v, ctx):
        return F.when(v.isNotNull(), F.struct(*self.shape(node, parsed, lambda n: v[n], ctx)))

    @staticmethod
    def _resolved(column, urns):
        """A reference to an entry of the Bundle by its urn: fullUrl becomes Type/id, as BundleTransformer does."""
        return F.when(column.startswith("urn:"), F.coalesce(F.try_element_at(urns, column), column)).otherwise(column)

    @staticmethod
    def _wrap(observed, repeating, column, single):
        """Applies a value plan to one occurrence or to each, and wraps a single occurrence of a repeating element."""
        if isinstance(observed, ArrayType):
            return F.transform(column, single)
        value = single(column)
        return F.when(column.isNotNull(), F.array(value)) if repeating else value

    @staticmethod
    def _primitive(prim, text):
        """Types a value as Spark's JSON reader types it, so that toLayout converts it as for JSON."""
        if prim == "boolean":
            return F.when(text == "true", F.lit(True)).when(text == "false", F.lit(False))
        if prim in INTEGRAL:
            return text.try_cast("bigint")
        if prim == "decimal":
            return text.try_cast("double")
        return text

    # ---------------------------------------------------------------- ingest

    def fit_lines(self, raw: DataFrame, resource_type: str) -> DataFrame:
        """One document per row. Returns the lazy encoded dataset, its schema fixed, as pc.encode does."""
        self.undescribed = []
        node = self.node(resource_type)
        root = r"^\s*(<\?xml[^>]*\?>\s*)?<" + resource_type + r"[\s/>]"
        docs = raw.filter(F.col(raw.columns[0]).rlike(root)).select(F.col(raw.columns[0]).alias("value"))
        presence = self.infer(docs, resource_type)
        start = time.perf_counter()
        parse = self.prune(node, presence, resource_type)
        parsed = docs.select(F.from_xml("value", parse, READ_OPTIONS).alias("r"), "value")
        ctx = {"type": resource_type}
        text = _field(parse, "text")
        if text is not None and _field(text.dataType, "div") is not None:
            narratives = F.regexp_extract_all(F.col("value"), F.lit(NARRATIVE), 1)
            has_div = F.col("r.text.div").isNotNull()
            ctx["narrative"] = F.when(has_div & (F.size(narratives) == 1), self._unescape(F.try_element_at(narratives, F.lit(1))))
        shaped = parsed.select(*self.shape(node, parse, lambda n: F.col("r")[n], ctx))
        self.shaped = shaped
        result = DataFrame(self.transformer.toLayout(resource_type, shaped._jdf), self.spark)
        self.plan_ms = (time.perf_counter() - start) * 1000
        return result

    def fit_bundles(self, raw: DataFrame, resource_type: str) -> DataFrame:
        """One Bundle per row, read whole, as encode_bundle reads it."""
        self.undescribed = []
        node = self.node(resource_type)
        docs = raw.select(F.col(raw.columns[0]).alias("value"))
        presence = self.infer(docs, "Bundle")
        start = time.perf_counter()
        entry_field = _field(presence, "entry")
        resource_field = _field(_inner(entry_field.dataType), "resource") if entry_field else None
        types = [f.name for f in resource_field.dataType.fields] if resource_field else []
        if resource_type not in types:
            raise ValueError(f"No {resource_type} in any Bundle")
        # Every other type contributes only what resolution and narrative alignment need.
        id_only = StructType([StructField("id", StructType([StructField("_value", StringType())]))])
        resource_types = []
        has_div_types = []
        for f in resource_field.dataType.fields:
            if f.name == resource_type:
                resource_types.append(StructField(f.name, self.prune(node, f.dataType, f.name), True))
            else:
                fields = list(id_only.fields)
                meta = _field(f.dataType, "meta")
                if meta is not None and _field(meta.dataType, "versionId") is not None:
                    fields.append(StructField("meta", StructType([StructField("versionId", StructType([StructField("_value", StringType())]))])))
                text = _field(f.dataType, "text")
                if text is not None and _field(text.dataType, "div") is not None:
                    fields.append(StructField("text", StructType([StructField("div", StringType())])))
                resource_types.append(StructField(f.name, StructType(fields), True))
            text = _field(resource_types[-1].dataType, "text")
            if text is not None and _field(text.dataType, "div") is not None:
                has_div_types.append(f.name)
        full_url = _field(_inner(entry_field.dataType), "fullUrl")
        entry_type = StructType(([StructField("fullUrl", StructType([StructField("_value", StringType())]))] if full_url else [])
                                + [StructField("resource", StructType(resource_types))])
        parse = StructType([StructField("entry", ArrayType(entry_type))])
        target = resource_types[[f.name for f in resource_types].index(resource_type)].dataType

        b = F.col("b")
        indexed = F.transform(b["entry"], lambda e, i: F.struct(i.alias("i"), e.alias("e")))
        # Values computed once per Bundle. Referenced from inside the per-entry lambda, the optimiser would
        # inline them there and rebuild them for every reference (the plan showed map_from_entries inside the
        # lambda), so they are materialised first, behind an explode of a one-element array, which a
        # projection cannot be collapsed through.
        per_bundle = []
        if full_url:
            def target_of(e):
                ids = []
                for t in types:
                    r = e["resource"][t]
                    meta = _field(next(f.dataType for f in resource_types if f.name == t), "meta")
                    versioned = meta is not None and _field(meta.dataType, "versionId") is not None
                    history = (F.when(r["meta"]["versionId"]["_value"].isNotNull(), F.concat(F.lit("/_history/"), r["meta"]["versionId"]["_value"])).otherwise(F.lit(""))
                               if versioned else F.lit(""))
                    ids.append(F.when(r["id"]["_value"].isNotNull(), F.concat(F.lit(t + "/"), r["id"]["_value"], history)))
                return F.coalesce(*ids)
            pairs = F.transform(b["entry"], lambda e: F.struct(e["fullUrl"]["_value"].alias("k"), target_of(e).alias("v")))
            per_bundle.append(F.map_from_entries(F.filter(pairs, lambda p: p["k"].startswith("urn:") & p["v"].isNotNull())).alias("urns"))
        if resource_type in has_div_types:
            def has_div(e):
                return F.coalesce(*[e["resource"][t]["text"]["div"].isNotNull() for t in has_div_types], F.lit(False))
            with_div = F.filter(indexed, lambda x: has_div(x["e"]))
            narratives = F.regexp_extract_all(F.col("value"), F.lit(NARRATIVE), 1)
            per_bundle += [F.map_from_entries(F.transform(with_div, lambda x, k: F.struct(x["i"], k))).alias("ordinal"),
                           narratives.alias("narratives"),
                           (F.size(narratives) == F.size(with_div)).alias("aligned")]
        parsed = docs.select(F.from_xml("value", parse, READ_OPTIONS).alias("b"), "value")
        if per_bundle:
            parsed = parsed.select("b", "value", F.explode(F.array(F.struct(*per_bundle))).alias("bundle"))
        ctx = {"type": resource_type}
        if full_url:
            ctx["urns"] = F.col("bundle.urns")
        selected = F.filter(indexed, lambda x: x["e"]["resource"][resource_type].isNotNull())

        def shaped(x):
            entry_ctx = dict(ctx)
            if resource_type in has_div_types:
                at = F.try_element_at(F.col("bundle.ordinal"), x["i"]) + 1
                entry_ctx["narrative"] = F.when(F.col("bundle.aligned"), self._unescape(F.try_element_at(F.col("bundle.narratives"), at)))
            v = x["e"]["resource"][resource_type]
            return F.struct(*self.shape(node, target, lambda n: v[n], entry_ctx))

        rows = parsed.select(F.explode(F.transform(selected, shaped)).alias("r")).select("r.*")
        self.shaped = rows
        result = DataFrame(self.transformer.toLayout(resource_type, rows._jdf), self.spark)
        self.plan_ms = (time.perf_counter() - start) * 1000
        return result

    @staticmethod
    def _unescape(div):
        """The harness writes a narrative's line feed as &#xa; to keep one document per line; store it as one."""
        return F.replace(F.replace(div, F.lit("&#xa;"), F.lit("\n")), F.lit("&#xd;"), F.lit("\r"))

    # ---------------------------------------------------------------- egress

    def decode(self, stored: DataFrame, resource_type: str) -> DataFrame:
        start = time.perf_counter()
        result = self._decode(stored, resource_type)
        self.plan_ms = (time.perf_counter() - start) * 1000
        return result

    def _decode(self, stored: DataFrame, resource_type: str) -> DataFrame:
        node = self.node(resource_type)
        fields = self._xml_fields(node, stored.schema, lambda n: F.col(n))
        text = _field(stored.schema, "text")
        has_div = text is not None and _field(text.dataType, "div") is not None
        xml = F.to_xml(F.struct(F.lit(FHIR_NS).alias("_xmlns"), *fields), {"rowTag": resource_type, "indent": ""})
        xml = F.regexp_replace(xml, r"\n\z", "")
        if has_div:
            xml = F.when(F.col("text.div").isNotNull(),
                         F.replace(xml, F.lit(f"<div>{DIV_PLACEHOLDER}</div>"), F.col("text.div"))).otherwise(xml)
        return stored.select(xml.alias("value"))

    def _xml_fields(self, node, stored: StructType, source):
        columns = []
        for f in stored.fields:
            entry = node.entry(f.name)
            if entry is None or entry.is_resource_type or not entry.is_element:
                continue
            column = source(f.name)
            if node.is_attribute(entry):
                columns.append(column.cast("string").alias("_" + f.name))
            elif entry.prim == "xhtml":
                columns.append(F.when(column.isNotNull(), F.lit(DIV_PLACEHOLDER)).alias(f.name))
            elif entry.prim is not None:
                columns.append(self._xml_each(f.dataType, column, self._xml_primitive(entry.prim)).alias(f.name))
            else:
                child = node.child(entry)
                inner = _inner(f.dataType)
                columns.append(self._xml_each(f.dataType, column, self._xml_structure(child, inner)).alias(f.name))
        return columns

    # PySpark passes an array index to a lambda's second parameter, so plans are made by factories rather than
    # by lambdas with bound defaults.
    def _xml_primitive(self, prim):
        return lambda v: F.when(v.isNotNull(), F.struct(self._text(prim, v).alias("_value")))

    def _xml_structure(self, node, stored):
        return lambda v: F.when(v.isNotNull(), F.struct(*self._xml_fields(node, stored, lambda n: v[n])))

    @staticmethod
    def _xml_each(stored_type, column, single):
        if isinstance(stored_type, ArrayType):
            # Positional nulls are dropped, as HAPI drops them on the way from JSON to XML.
            return F.filter(F.transform(column, single), lambda v: v.isNotNull())
        return single(column)

    @staticmethod
    def _text(prim, v):
        if prim == "boolean":
            return F.when(v, F.lit("true")).otherwise(F.lit("false"))
        if prim == "base64Binary":
            return F.regexp_replace(F.base64(v), "[\\r\\n]", "")
        if prim == "decimal":
            return XmlRoute._plain(v)
        return v.cast("string")

    @staticmethod
    def _plain(text):
        """Writes a decimal stored in a double's exponent form ("8.8E-4") in plain form, as HEAD's egress does
        through HAPI (BigDecimal.toPlainString). Any other stored text is written as it is."""
        pattern = r"^(-?)([0-9])(?:\.([0-9]+))?E(-?[0-9]+)$"
        sign, lead = F.regexp_extract(text, pattern, 1), F.regexp_extract(text, pattern, 2)
        digits = F.concat(lead, F.regexp_extract(text, pattern, 3))
        exponent = F.regexp_extract(text, pattern, 4).cast("int")
        negative = F.concat(sign, F.lit("0."), F.repeat(F.lit("0"), -exponent - 1), digits)
        whole = F.concat(sign, digits, F.repeat(F.lit("0"), exponent + 1 - F.length(digits)))
        split = F.concat(sign, F.substring(digits, F.lit(1), exponent + 1), F.lit("."), F.substring(digits, exponent + 2, F.lit(1000)))
        plain = (F.when(exponent < 0, negative)
                 .when(F.length(digits) <= exponent + 1, whole)
                 .otherwise(split))
        return F.when(text.rlike(pattern), plain).otherwise(text)
