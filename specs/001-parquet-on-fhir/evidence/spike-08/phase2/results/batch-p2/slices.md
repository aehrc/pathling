
## Slice: json, 2026-10-19 21:04:57

### p2-aplus-r?-514f8f8 against p2-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient            82    2,855 34.70       135      981 7.25       108      244 2.26
json             Condition         486    1,663 3.42       828    2,914 3.52       291      398 1.37
json             Encounter         855    3,087 3.61     1,611    8,921 5.54       641      886 1.38
json             Observation       992    3,229 3.26     1,719    6,892 4.01       754    1,129 1.50
json             fit     3.26-34.70
json             encode  3.52-7.25
json             decode  1.37-2.26
-- without py4j plan building:
json             Patient            82      120 1.46       135      981 7.25       108       92 0.85
json             Condition         486      670 1.38       828    2,914 3.52       291      325 1.12
json             Encounter         855    1,232 1.44     1,611    8,921 5.54       641      768 1.20
json             Observation       992    1,474 1.49     1,719    6,892 4.01       754    1,018 1.35
json             fit     1.38-1.49
json             encode  3.52-7.25
json             decode  0.85-1.35
```

### p2-b-r?-514f8f8 against p2-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient            82       55 0.67       135      177 1.31       108      120 1.11
json             Condition         486      198 0.41       828    1,269 1.53       291      280 0.96
json             Encounter         855      371 0.43     1,611    2,381 1.48       641      550 0.86
json             Observation       992      411 0.41     1,719    2,528 1.47       754      636 0.84
json             fit     0.41-0.67
json             encode  1.31-1.53
json             decode  0.84-1.11
```

### p2-avariant-r?-514f8f8 against p2-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient            82    5,368 65.24       135    5,608 41.48       108       99 0.91
json             Condition         486    2,738 5.63       828   17,373 20.98       291      296 1.02
json             Encounter         855    5,140 6.01     1,611   66,738 41.41       641      711 1.11
json             Observation       992    4,982 5.02     1,719   48,280 28.08       754      758 1.01
json             fit     5.02-65.24
json             encode  20.98-41.48
json             decode  0.91-1.11
-- without py4j plan building:
json             Patient            82      121 1.47       135    5,608 41.48       108       99 0.91
json             Condition         486      650 1.34       828   17,373 20.98       291      296 1.02
json             Encounter         855    1,182 1.38     1,611   66,738 41.41       641      711 1.11
json             Observation       992    1,475 1.49     1,719   48,280 28.08       754      758 1.01
json             fit     1.34-1.49
json             encode  20.98-41.48
json             decode  0.91-1.11
```

### A0 r1 against r2 (the noise check; the verdict covers every source so far)
```
source           type         phase   p2-a0-r1- p2-a0-r2- ratio
json             Condition    decode        291       287  0.99  (under 1 s, not judged)
json             Condition    encode        828       788  0.95  (under 1 s, not judged)
json             Condition    fit           486       444  0.91  (under 1 s, not judged)
json             Encounter    decode        641       637  0.99  (under 1 s, not judged)
json             Encounter    encode      1,611     1,577  0.98
json             Encounter    fit           855       853  1.00  (under 1 s, not judged)
json             Observation  decode        754       710  0.94  (under 1 s, not judged)
json             Observation  encode      1,719     1,707  0.99
json             Observation  fit           992       984  0.99  (under 1 s, not judged)
json             Patient      decode        108       108  1.00  (under 1 s, not judged)
json             Patient      encode        135       132  0.98  (under 1 s, not judged)
json             Patient      fit            82        95  1.15  (under 1 s, not judged)
judged items 2; worst |ratio - 1| 0.021; median 0.014; ACCEPTABLE (limits 0.15 and 0.05)
```

## Slice: xml-lines, 2026-10-19 22:06:39

### p2-aplus-r?-514f8f8 against p2-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
xml-lines        Patient           273    3,170 11.61       303    2,646 8.74       313      524 1.68
xml-lines        Condition       2,237    2,196 0.98     2,534    7,290 2.88     2,354    1,269 0.54
xml-lines        Encounter       4,490    4,431 0.99     5,201   15,106 2.90     4,817    4,409 0.92
xml-lines        Observation     5,262    5,202 0.99     6,056   29,621 4.89     5,545    5,028 0.91
xml-lines        fit     0.98-11.61
xml-lines        encode  2.88-8.74
xml-lines        decode  0.54-1.68
-- without py4j plan building:
xml-lines        Patient           273      180 0.66       303    2,646 8.74       313      270 0.86
xml-lines        Condition       2,237    1,080 0.48     2,534    7,290 2.88     2,354    1,157 0.49
xml-lines        Encounter       4,490    2,352 0.52     5,201   15,106 2.90     4,817    4,214 0.87
xml-lines        Observation     5,262    3,100 0.59     6,056   29,621 4.89     5,545    4,846 0.87
xml-lines        fit     0.48-0.66
xml-lines        encode  2.88-8.74
xml-lines        decode  0.49-0.87
```

### p2-b-r?-514f8f8 against p2-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
xml-lines        Patient           273       73 0.27       303      188 0.62       313      105 0.34
xml-lines        Condition       2,237      334 0.15     2,534    1,390 0.55     2,354      296 0.13
xml-lines        Encounter       4,490      568 0.13     5,201    2,538 0.49     4,817      566 0.12
xml-lines        Observation     5,262      704 0.13     6,056    2,809 0.46     5,545      677 0.12
xml-lines        fit     0.13-0.27
xml-lines        encode  0.46-0.62
xml-lines        decode  0.12-0.34
```

### A0 r1 against r2 (the noise check; the verdict covers every source so far)
```
source           type         phase   p2-a0-r1- p2-a0-r2- ratio
xml-lines        Condition    decode      2,354     2,383  1.01
xml-lines        Condition    encode      2,534     2,581  1.02
xml-lines        Condition    fit         2,237     2,247  1.00
xml-lines        Encounter    decode      4,817     4,874  1.01
xml-lines        Encounter    encode      5,201     5,204  1.00
xml-lines        Encounter    fit         4,490     4,590  1.02
xml-lines        Observation  decode      5,545     5,676  1.02
xml-lines        Observation  encode      6,056     6,095  1.01
xml-lines        Observation  fit         5,262     5,307  1.01
xml-lines        Patient      decode        313       318  1.02  (under 1 s, not judged)
xml-lines        Patient      encode        303       299  0.99  (under 1 s, not judged)
xml-lines        Patient      fit           273       248  0.91  (under 1 s, not judged)
judged items 11; worst |ratio - 1| 0.024; median 0.012; ACCEPTABLE (limits 0.15 and 0.05)
```

## Slice: xml-bundles, 2026-10-19 23:33:01

### p2-aplus-r?-514f8f8 against p2-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
xml-bundles      Patient         4,297   10,929 2.54     4,539    8,473 1.87       599      648 1.08
xml-bundles      Condition       6,051    9,115 1.51     6,384    8,632 1.35     3,953    1,776 0.45
xml-bundles      Encounter       7,990   10,065 1.26     8,832   13,086 1.48     9,156    4,197 0.46
xml-bundles      Observation     8,702    9,980 1.15     9,516   19,704 2.07    15,159    6,087 0.40
xml-bundles      fit     1.15-2.54
xml-bundles      encode  1.35-2.07
xml-bundles      decode  0.40-1.08
-- without py4j plan building:
xml-bundles      Patient         4,297    7,779 1.81     4,539    8,473 1.87       599      392 0.65
xml-bundles      Condition       6,051    7,930 1.31     6,384    8,632 1.35     3,953    1,662 0.42
xml-bundles      Encounter       7,990    7,862 0.98     8,832   13,086 1.48     9,156    4,002 0.44
xml-bundles      Observation     8,702    7,841 0.90     9,516   19,704 2.07    15,159    5,907 0.39
xml-bundles      fit     0.90-1.81
xml-bundles      encode  1.35-2.07
xml-bundles      decode  0.39-0.65
```

### p2-b-r?-514f8f8 against p2-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
xml-bundles      Patient         4,297    1,081 0.25     4,539    1,231 0.27       599      274 0.46
xml-bundles      Condition       6,051    1,110 0.18     6,384    2,075 0.32     3,953      468 0.12
xml-bundles      Encounter       7,990    1,215 0.15     8,832    3,097 0.35     9,156    1,064 0.12
xml-bundles      Observation     8,702    1,279 0.15     9,516    3,254 0.34    15,159    1,709 0.11
xml-bundles      fit     0.15-0.25
xml-bundles      encode  0.27-0.35
xml-bundles      decode  0.11-0.46
```

### A0 r1 against r2 (the noise check; the verdict covers every source so far)
```
source           type         phase   p2-a0-r1- p2-a0-r2- ratio
xml-bundles      Condition    decode      3,953     3,978  1.01
xml-bundles      Condition    encode      6,384     6,409  1.00
xml-bundles      Condition    fit         6,051     6,078  1.00
xml-bundles      Encounter    decode      9,156     9,133  1.00
xml-bundles      Encounter    encode      8,832     8,762  0.99
xml-bundles      Encounter    fit         7,990     7,974  1.00
xml-bundles      Observation  decode     15,159    15,140  1.00
xml-bundles      Observation  encode      9,516     9,539  1.00
xml-bundles      Observation  fit         8,702     8,776  1.01
xml-bundles      Patient      decode        599       584  0.98  (under 1 s, not judged)
xml-bundles      Patient      encode      4,539     4,429  0.98
xml-bundles      Patient      fit         4,297     4,230  0.98
judged items 22; worst |ratio - 1| 0.024; median 0.007; ACCEPTABLE (limits 0.15 and 0.05)
```

## Slice: xml-bundles-txn, 2026-10-20 01:05:10

### p2-aplus-r?-514f8f8 against p2-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
xml-bundles-txn  Patient         4,746   11,886 2.50     4,956    9,479 1.91       557      618 1.11
xml-bundles-txn  Condition       6,734    9,864 1.46     7,089   10,269 1.45     3,909    1,719 0.44
xml-bundles-txn  Encounter       8,534   10,839 1.27     9,345   15,165 1.62     8,969    4,170 0.46
xml-bundles-txn  Observation     9,390   10,884 1.16    10,204   23,258 2.28    14,758    5,966 0.40
xml-bundles-txn  fit     1.16-2.50
xml-bundles-txn  encode  1.45-2.28
xml-bundles-txn  decode  0.40-1.11
-- without py4j plan building:
xml-bundles-txn  Patient         4,746    8,708 1.83     4,956    9,479 1.91       557      366 0.66
xml-bundles-txn  Condition       6,734    8,651 1.28     7,089   10,269 1.45     3,909    1,605 0.41
xml-bundles-txn  Encounter       8,534    8,658 1.01     9,345   15,165 1.62     8,969    3,977 0.44
xml-bundles-txn  Observation     9,390    8,734 0.93    10,204   23,258 2.28    14,758    5,786 0.39
xml-bundles-txn  fit     0.93-1.83
xml-bundles-txn  encode  1.45-2.28
xml-bundles-txn  decode  0.39-0.66
```

### p2-b-r?-514f8f8 against p2-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
xml-bundles-txn  Patient         4,746    1,123 0.24     4,956    1,328 0.27       557      233 0.42
xml-bundles-txn  Condition       6,734    1,167 0.17     7,089    2,152 0.30     3,909      453 0.12
xml-bundles-txn  Encounter       8,534    1,278 0.15     9,345    3,165 0.34     8,969    1,048 0.12
xml-bundles-txn  Observation     9,390    1,370 0.15    10,204    3,386 0.33    14,758    1,674 0.11
xml-bundles-txn  fit     0.15-0.24
xml-bundles-txn  encode  0.27-0.34
xml-bundles-txn  decode  0.11-0.42
```

### A0 r1 against r2 (the noise check; the verdict covers every source so far)
```
source           type         phase   p2-a0-r1- p2-a0-r2- ratio
xml-bundles-txn  Condition    decode      3,909     3,955  1.01
xml-bundles-txn  Condition    encode      7,089     7,092  1.00
xml-bundles-txn  Condition    fit         6,734     6,650  0.99
xml-bundles-txn  Encounter    decode      8,969     9,156  1.02
xml-bundles-txn  Encounter    encode      9,345     9,366  1.00
xml-bundles-txn  Encounter    fit         8,534     8,581  1.01
xml-bundles-txn  Observation  decode     14,758    15,037  1.02
xml-bundles-txn  Observation  encode     10,204    10,132  0.99
xml-bundles-txn  Observation  fit         9,390     9,369  1.00
xml-bundles-txn  Patient      decode        557       567  1.02  (under 1 s, not judged)
xml-bundles-txn  Patient      encode      4,956     4,907  0.99
xml-bundles-txn  Patient      fit         4,746     4,677  0.99
judged items 33; worst |ratio - 1| 0.024; median 0.008; ACCEPTABLE (limits 0.15 and 0.05)
```

## Slice: messy JSON, under drop, 2026-10-20 01:36:22

### p2-messy-aplus-514f8f8 against p2-messy-pathling-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient           108    2,044 18.87       137      859 6.27        96      260 2.72
json             Condition         503    2,222 4.42       815    2,776 3.41       252      559 2.22
json             Encounter         874    4,129 4.72     1,547    7,998 5.17       568    1,252 2.20
json             Observation     1,001    4,559 4.56     1,655    6,443 3.89       616    1,126 1.83
json             fit     4.42-18.87
json             encode  3.41-6.27
json             decode  1.83-2.72
-- without py4j plan building:
json             Patient           108      322 2.97       137      859 6.27        96      107 1.12
json             Condition         503    1,606 3.19       815    2,776 3.41       252      493 1.96
json             Encounter         874    2,965 3.39     1,547    7,998 5.17       568    1,136 2.00
json             Observation     1,001    3,470 3.47     1,655    6,443 3.89       616    1,019 1.65
json             fit     2.97-3.47
json             encode  3.41-6.27
json             decode  1.12-2.00
```

### p2-messy-avariant-514f8f8 against p2-messy-pathling-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient           108    3,531 32.59       137    3,182 23.22        96      110 1.15
json             Condition         503    2,874 5.71       815    8,557 10.50       252      359 1.43
json             Encounter         874    5,449 6.23     1,547   37,173 24.02       568      663 1.17
json             Observation     1,001    5,537 5.53     1,655   26,702 16.13       616      781 1.27
json             fit     5.53-32.59
json             encode  10.50-24.02
json             decode  1.15-1.43
-- without py4j plan building:
json             Patient           108      251 2.32       137    3,182 23.22        96      110 1.15
json             Condition         503    1,582 3.15       815    8,557 10.50       252      359 1.43
json             Encounter         874    2,910 3.33     1,547   37,173 24.02       568      663 1.17
json             Observation     1,001    3,498 3.50     1,655   26,702 16.13       616      781 1.27
json             fit     2.32-3.50
json             encode  10.50-24.02
json             decode  1.15-1.43
```

### p2-messy-b-514f8f8 against p2-messy-pathling-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient           108       54 0.50       137      180 1.31        96      104 1.08
json             Condition         503      208 0.41       815    1,291 1.58       252      284 1.13
json             Encounter         874      366 0.42     1,547    2,369 1.53       568      553 0.97
json             Observation     1,001      401 0.40     1,655    2,529 1.53       616      639 1.04
json             fit     0.40-0.50
json             encode  1.31-1.58
json             decode  0.97-1.13
```
