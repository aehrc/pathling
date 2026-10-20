
## Slice: json, 2026-10-20 10:51:11

### p3-b-r?-514f8f8 against p3-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient            94       58 0.62       142      176 1.24       109      105 0.96
json             Condition         445      210 0.47       800    1,265 1.58       283      264 0.93
json             Encounter         816      362 0.44     1,531    2,347 1.53       631      546 0.86
json             Observation       991      409 0.41     1,741    2,538 1.46       740      669 0.90
json             fit     0.41-0.62
json             encode  1.24-1.58
json             decode  0.86-0.96
```

### p3-orig-r?-514f8f8 against p3-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient            94       66 0.71       142      122 0.86       109      100 0.91
json             Condition         445      171 0.39       800      503 0.63       283      293 1.03
json             Encounter         816      328 0.40     1,531      994 0.65       631      665 1.05
json             Observation       991      397 0.40     1,741    1,060 0.61       740      777 1.05
json             fit     0.39-0.71
json             encode  0.61-0.86
json             decode  0.91-1.05
```

### A0 r1 against r2 (the noise check; the verdict covers every source so far)
```
source           type         phase   p3-a0-r1- p3-a0-r2- ratio
json             Condition    decode        283       282  1.00  (under 1 s, not judged)
json             Condition    encode        800       770  0.96  (under 1 s, not judged)
json             Condition    fit           445       444  1.00  (under 1 s, not judged)
json             Encounter    decode        631       640  1.01  (under 1 s, not judged)
json             Encounter    encode      1,531     1,497  0.98
json             Encounter    fit           816       796  0.98  (under 1 s, not judged)
json             Observation  decode        740       693  0.94  (under 1 s, not judged)
json             Observation  encode      1,741     1,688  0.97
json             Observation  fit           991       984  0.99  (under 1 s, not judged)
json             Patient      decode        109       100  0.92  (under 1 s, not judged)
json             Patient      encode        142       134  0.94  (under 1 s, not judged)
json             Patient      fit            94        99  1.06  (under 1 s, not judged)
judged items 2; worst |ratio - 1| 0.031; median 0.026; ACCEPTABLE (limits 0.15 and 0.05)
```
