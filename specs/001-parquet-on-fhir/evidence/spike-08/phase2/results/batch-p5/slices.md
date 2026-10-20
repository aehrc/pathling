
## Slice: json, 2026-10-20 13:03:59

### p5-aplus-r?-514f8f8 against p5-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient            89    2,895 32.47       143    1,012 7.09       101      231 2.29
json             Condition         445    1,660 3.73       805    2,931 3.64       276      386 1.40
json             Encounter         806    3,123 3.87     1,566    8,859 5.66       632      867 1.37
json             Observation       993    3,229 3.25     1,741    6,840 3.93       712      976 1.37
json             fit     3.25-32.47
json             encode  3.64-7.09
json             decode  1.37-2.29
-- without py4j plan building:
json             Patient            89      113 1.27       143    1,012 7.09       101       93 0.92
json             Condition         445      670 1.51       805    2,931 3.64       276      318 1.15
json             Encounter         806    1,261 1.56     1,566    8,859 5.66       632      751 1.19
json             Observation       993    1,456 1.47     1,741    6,840 3.93       712      861 1.21
json             fit     1.27-1.56
json             encode  3.64-7.09
json             decode  0.92-1.21
```

### p5-aplusnc-r?-514f8f8 against p5-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient            89    1,013 11.36       143      186 1.30       101      240 2.37
json             Condition         445    1,034 2.33       805    1,114 1.38       276      389 1.41
json             Encounter         806    1,827 2.27     1,566    2,201 1.41       632      899 1.42
json             Observation       993    2,019 2.03     1,741    2,318 1.33       712      950 1.33
json             fit     2.03-11.36
json             encode  1.30-1.41
json             decode  1.33-2.37
-- without py4j plan building:
json             Patient            89      114 1.28       143      186 1.30       101       96 0.95
json             Condition         445      709 1.59       805    1,114 1.38       276      314 1.14
json             Encounter         806    1,230 1.53     1,566    2,201 1.41       632      781 1.24
json             Observation       993    1,446 1.46     1,741    2,318 1.33       712      840 1.18
json             fit     1.28-1.59
json             encode  1.30-1.41
json             decode  0.95-1.24
```

### p5-aplusdef-r?-514f8f8 against p5-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient            89    2,890 32.41       143      996 6.98       101      234 2.31
json             Condition         445    1,671 3.76       805    2,932 3.64       276      398 1.44
json             Encounter         806    3,108 3.86     1,566    8,817 5.63       632      877 1.39
json             Observation       993    3,249 3.27     1,741    6,864 3.94       712      965 1.36
json             fit     3.27-32.41
json             encode  3.64-6.98
json             decode  1.36-2.31
-- without py4j plan building:
json             Patient            89      125 1.40       143      996 6.98       101       88 0.87
json             Condition         445      679 1.53       805    2,932 3.64       276      323 1.17
json             Encounter         806    1,252 1.55     1,566    8,817 5.63       632      758 1.20
json             Observation       993    1,475 1.49     1,741    6,864 3.94       712      855 1.20
json             fit     1.40-1.55
json             encode  3.64-6.98
json             decode  0.87-1.20
```

### A0 r1 against r2 (the noise check; the verdict covers every source so far)
```
source           type         phase   p5-a0-r1- p5-a0-r2- ratio
json             Condition    decode        276       285  1.03  (under 1 s, not judged)
json             Condition    encode        805       798  0.99  (under 1 s, not judged)
json             Condition    fit           445       444  1.00  (under 1 s, not judged)
json             Encounter    decode        632       641  1.01  (under 1 s, not judged)
json             Encounter    encode      1,566     1,532  0.98
json             Encounter    fit           806       809  1.00  (under 1 s, not judged)
json             Observation  decode        712       720  1.01  (under 1 s, not judged)
json             Observation  encode      1,741     1,740  1.00
json             Observation  fit           993     1,013  1.02  (under 1 s, not judged)
json             Patient      decode        101       111  1.09  (under 1 s, not judged)
json             Patient      encode        143       140  0.98  (under 1 s, not judged)
json             Patient      fit            89        90  1.01  (under 1 s, not judged)
judged items 2; worst |ratio - 1| 0.022; median 0.011; ACCEPTABLE (limits 0.15 and 0.05)
```
