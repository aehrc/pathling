
## Slice: json, 2026-10-20 12:13:43

### p4-a0hml-r?-514f8f8 against p4-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient            94      120 1.27       154      135 0.88       107      108 1.01
json             Condition         467      498 1.07       823      852 1.04       299      304 1.02
json             Encounter         804      837 1.04     1,505    1,551 1.03       638      655 1.03
json             Observation     1,023    1,001 0.98     1,697    1,699 1.00       713      743 1.04
json             fit     0.98-1.27
json             encode  0.88-1.04
json             decode  1.01-1.04
```

### p4-orig-r?-514f8f8 against p4-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient            94       63 0.67       154      114 0.74       107      102 0.95
json             Condition         467      184 0.39       823      503 0.61       299      287 0.96
json             Encounter         804      323 0.40     1,505    1,021 0.68       638      660 1.03
json             Observation     1,023      401 0.39     1,697    1,060 0.62       713      772 1.08
json             fit     0.39-0.67
json             encode  0.61-0.74
json             decode  0.95-1.08
```

### p4-b-r?-514f8f8 against p4-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient            94       62 0.65       154      109 0.71       107      101 0.94
json             Condition         467      194 0.41       823      375 0.46       299      254 0.85
json             Encounter         804      361 0.45     1,505      765 0.51       638      528 0.83
json             Observation     1,023      413 0.40     1,697      783 0.46       713      624 0.87
json             fit     0.40-0.65
json             encode  0.46-0.71
json             decode  0.83-0.94
```

### p4-bnc-r?-514f8f8 against p4-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient            94       66 0.70       154      113 0.74       107      116 1.09
json             Condition         467      214 0.46       823      235 0.29       299      270 0.90
json             Encounter         804      357 0.44     1,505      460 0.31       638      540 0.85
json             Observation     1,023      425 0.42     1,697      503 0.30       713      648 0.91
json             fit     0.42-0.70
json             encode  0.29-0.74
json             decode  0.85-1.09
```

### A0 r1 against r2 (the noise check; the verdict covers every source so far)
```
source           type         phase   p4-a0-r1- p4-a0-r2- ratio
json             Condition    decode        299       283  0.95  (under 1 s, not judged)
json             Condition    encode        823       812  0.99  (under 1 s, not judged)
json             Condition    fit           467       447  0.96  (under 1 s, not judged)
json             Encounter    decode        638       633  0.99  (under 1 s, not judged)
json             Encounter    encode      1,505     1,578  1.05
json             Encounter    fit           804       807  1.00  (under 1 s, not judged)
json             Observation  decode        713       722  1.01  (under 1 s, not judged)
json             Observation  encode      1,697     1,730  1.02
json             Observation  fit         1,023       987  0.96  (under 1 s, not judged)
json             Patient      decode        107       104  0.97  (under 1 s, not judged)
json             Patient      encode        154       144  0.94  (under 1 s, not judged)
json             Patient      fit            94       101  1.07  (under 1 s, not judged)
judged items 2; worst |ratio - 1| 0.049; median 0.034; ACCEPTABLE (limits 0.15 and 0.05)
```
