-- setup zipped: p1 and p2 have the same given value and different extensions, p3 has none, p4 repeats p1
CREATE OR REPLACE TEMP VIEW z AS SELECT id, from_json(g, 'array<struct<value:string,id:string,extension:array<struct<url:string,valueString:string>>>>') AS given FROM VALUES ('p1', '[{"value":"Ann","extension":[{"url":"u","valueString":"A"}]}]'), ('p2', '[{"value":"Ann","extension":[{"url":"u","valueString":"B"}]}]'), ('p3', '[{"value":"Ann"}]'), ('p4', '[{"value":"Ann","extension":[{"url":"u","valueString":"A"}]}]'), ('p5', '[{"value":"Ann","extension":[{"url":"u","valueString":"A"}]},{"value":"Ann","extension":[{"url":"u","valueString":"B"}]}]') AS v(id, g)

-- setup parallel: the same patients, given by value and _given alongside
CREATE OR REPLACE TEMP VIEW p AS SELECT id, from_json(g, 'array<string>') AS given, from_json(m, 'array<struct<id:string,extension:array<struct<url:string,valueString:string>>>>') AS _given FROM VALUES ('p1', '["Ann"]', '[{"extension":[{"url":"u","valueString":"A"}]}]'), ('p2', '["Ann"]', '[{"extension":[{"url":"u","valueString":"B"}]}]'), ('p3', '["Ann"]', NULL), ('p4', '["Ann"]', '[{"extension":[{"url":"u","valueString":"A"}]}]'), ('p5', '["Ann","Ann"]', '[{"extension":[{"url":"u","valueString":"A"}]},{"extension":[{"url":"u","valueString":"B"}]}]') AS v(id, g, m)

-- Z1 SELECT DISTINCT given, zipped (5 patients: by value 2 distinct, [Ann] for p1 to p4 and [Ann, Ann] for p5)
SELECT DISTINCT given FROM z ORDER BY to_json(given)

-- P1 SELECT DISTINCT given, parallel
SELECT DISTINCT given FROM p ORDER BY to_json(given)

-- Z2 GROUP BY given, zipped
SELECT given, count(*) AS patients FROM z GROUP BY given ORDER BY to_json(given)

-- P2 GROUP BY given, parallel
SELECT given, count(*) AS patients FROM p GROUP BY given ORDER BY to_json(given)

-- Z3 array_distinct(given) on p5 (two Ann elements that differ only in extension), zipped
SELECT id, size(array_distinct(given)) AS n_distinct FROM z WHERE id = 'p5'

-- P3 array_distinct(given) on p5, parallel
SELECT id, size(array_distinct(given)) AS n_distinct FROM p WHERE id = 'p5'

-- Z4 array_contains(given, 'Ann') directly, zipped
SELECT id FROM z WHERE array_contains(given, 'Ann')

-- Z5 array_contains over the transform, zipped
SELECT id FROM z WHERE array_contains(transform(given, g -> g.value), 'Ann') ORDER BY id

-- Z6 exists over the value, zipped
SELECT id FROM z WHERE exists(given, g -> g.value = 'Ann') ORDER BY id

-- P4 array_contains(given, 'Ann') directly, parallel
SELECT id FROM p WHERE array_contains(given, 'Ann') ORDER BY id

-- Z7 given = array of struct equality, patients with given equal to p1's (by struct)
SELECT a.id, b.id AS other FROM z a JOIN z b ON a.given = b.given AND a.id < b.id ORDER BY a.id, b.id

-- P5 patients with given equal to each other by value, parallel
SELECT a.id, b.id AS other FROM p a JOIN p b ON a.given = b.given AND a.id < b.id ORDER BY a.id, b.id
