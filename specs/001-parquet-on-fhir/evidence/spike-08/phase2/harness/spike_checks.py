"""Spike 08, shared by A+ and A-variant: decision 133's datatype checks and the stored value of a primitive,
generated as Spark SQL from a value's text. The regexes are those of Spike08Checks.java (B), so the
candidates check the same grammar: the FHIR R4 regex and, for the date family, a calendar date that parses.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BinaryType, BooleanType, IntegerType, LongType, StringType, StructField, StructType

YEAR = "([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)"
TIME = "([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\\.[0-9]+)?"
ZONE = "(Z|(\\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00))"
REGEX = {
    "date": YEAR + "(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1]))?)?",
    "dateTime": YEAR + "(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1])(T" + TIME + ZONE + ")?)?)?",
    "instant": YEAR + "-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])T" + TIME + ZONE,
    "time": TIME,
    "decimal": "-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?",
    "integer": "-?(0|[1-9][0-9]*)",
}
INTEGRAL = {"integer", "unsignedInt", "positiveInt", "integer64"}
NUMERIC = INTEGRAL | {"decimal"}
FINDING = StructType([StructField("path", StringType()), StructField("kind", StringType()),
                      StructField("value", StringType())])
def no_findings():
    return F.array().cast(ArrayType(FINDING))


def stored_type(prim):
    if prim in ("integer", "unsignedInt", "positiveInt"):
        return IntegerType()
    if prim == "integer64":
        return LongType()
    if prim == "boolean":
        return BooleanType()
    if prim == "base64Binary":
        return BinaryType()
    return StringType()


def valid_text(prim, x):
    """Whether non-null text is valid for the type (the token's type is checked by the caller)."""
    nonempty = F.length(x) > 0
    if prim in ("date", "dateTime", "instant"):
        # A full date must exist in the calendar: 1980-02-30 matches the regex but is not a date.
        calendar = (F.length(x) < 10) | F.try_to_timestamp(F.substring(x, 1, 10), F.lit("yyyy-MM-dd")).isNotNull()
        return x.rlike("^" + REGEX[prim] + "$") & calendar
    if prim == "time":
        return x.rlike("^" + REGEX["time"] + "$")
    if prim == "decimal":
        return x.rlike("^" + REGEX["decimal"] + "$")
    if prim in INTEGRAL:
        n = x.try_cast("bigint")
        ok = x.rlike("^" + REGEX["integer"] + "$") & n.isNotNull()
        if prim == "integer64":
            return ok
        lo = 1 if prim == "positiveInt" else 0 if prim == "unsignedInt" else -2147483648
        return ok & (n >= lo) & (n <= 2147483647)
    if prim == "boolean":
        return x.isin("true", "false")
    if prim == "base64Binary":
        return nonempty & F.try_to_binary(F.regexp_replace(x, "\\s", ""), F.lit("base64")).isNotNull()
    return nonempty


def stored_value(prim, x):
    """The stored value of valid text."""
    if prim in ("integer", "unsignedInt", "positiveInt"):
        return x.try_cast("int")
    if prim == "integer64":
        return x.try_cast("bigint")
    if prim == "boolean":
        return x == F.lit("true")
    if prim == "base64Binary":
        return F.try_to_binary(F.regexp_replace(x, "\\s", ""), F.lit("base64"))
    return x


def finding(path, kind, value):
    return F.struct(path.alias("path"), F.lit(kind).alias("kind"), value.cast("string").alias("value"))


def child_path(path, name):
    return F.concat(path, F.lit("." + name))


def index_path(path, i):
    return F.concat(path, F.lit("["), i.cast("string"), F.lit("]"))


def all_of(conds):
    conds = [c for c in conds if c is not None]
    if not conds:
        return None
    out = conds[0]
    for c in conds[1:]:
        out = out & c
    return out


def concat_findings(parts):
    parts = [p for p in parts if p is not None]
    if not parts:
        return None
    return parts[0] if len(parts) == 1 else F.concat(*parts)
