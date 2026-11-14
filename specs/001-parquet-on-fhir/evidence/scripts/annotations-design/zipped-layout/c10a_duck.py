"""The C10a demonstration on DuckDB: the zipped and the parallel layout of the same five patients."""
import duckdb

print("DuckDB", duckdb.__version__)
con = duckdb.connect()
# Zipped: given is a list of struct(value, id, extension). p1 and p4 are identical, p2 differs from
# p1 only in its extension value, p3 has no metadata, p5 has two Ann elements that differ in extension.
con.execute("""
create table z as select * from (values
  ('p1', [{'value':'Ann','id':NULL::varchar,'extension':[{'url':'u','valueString':'A'}]}]),
  ('p2', [{'value':'Ann','id':NULL,'extension':[{'url':'u','valueString':'B'}]}]),
  ('p3', [{'value':'Ann','id':NULL,'extension':NULL::struct(url varchar, valueString varchar)[]}]),
  ('p4', [{'value':'Ann','id':NULL,'extension':[{'url':'u','valueString':'A'}]}]),
  ('p5', [{'value':'Ann','id':NULL,'extension':[{'url':'u','valueString':'A'}]},
          {'value':'Ann','id':NULL,'extension':[{'url':'u','valueString':'B'}]}])
) v(id, given)""")
con.execute("""
create table p as select * from (values
  ('p1', ['Ann'], [{'id':NULL::varchar,'extension':[{'url':'u','valueString':'A'}]}]),
  ('p2', ['Ann'], [{'id':NULL,'extension':[{'url':'u','valueString':'B'}]}]),
  ('p3', ['Ann'], NULL),
  ('p4', ['Ann'], [{'id':NULL,'extension':[{'url':'u','valueString':'A'}]}]),
  ('p5', ['Ann','Ann'], [{'id':NULL,'extension':[{'url':'u','valueString':'A'}]},
                         {'id':NULL,'extension':[{'url':'u','valueString':'B'}]}])
) v(id, given, _given)""")

QUERIES = [
    ("Z1 SELECT DISTINCT given, zipped", "select distinct given from z order by cast(given as varchar)"),
    ("P1 SELECT DISTINCT given, parallel", "select distinct given from p order by cast(given as varchar)"),
    ("Z2 GROUP BY given, zipped", "select given, count(*) n from z group by given order by cast(given as varchar)"),
    ("P2 GROUP BY given, parallel", "select given, count(*) n from p group by given order by cast(given as varchar)"),
    ("Z3 list_distinct(given) on p5, zipped, element count", "select id, len(list_distinct(given)) n from z where id = 'p5'"),
    ("P3 list_distinct(given) on p5, parallel, element count", "select id, len(list_distinct(given)) n from p where id = 'p5'"),
    ("Z4 list_contains(given, 'Ann') directly, zipped", "select id from z where list_contains(given, 'Ann') order by id"),
    ("Z5 list_contains over list_transform, zipped", "select id from z where list_contains(list_transform(given, lambda g: g.value), 'Ann') order by id"),
    ("P4 list_contains(given, 'Ann') directly, parallel", "select id from p where list_contains(given, 'Ann') order by id"),
    ("Z6 self-join on given equality, zipped", "select a.id, b.id other from z a join z b on a.given = b.given and a.id < b.id order by 1, 2"),
    ("P5 self-join on given equality, parallel", "select a.id, b.id other from p a join p b on a.given = b.given and a.id < b.id order by 1, 2"),
]
for label, sql in QUERIES:
    print(f"### {label}\n{sql}")
    try:
        for r in con.execute(sql).fetchall():
            print("  ", r)
    except Exception as e:
        print("ERROR:", str(e).splitlines()[0][:300])
