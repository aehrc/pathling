
## Slice: json, 2026-10-20 14:15:50

### p6-budf-r?-514f8f8 against p6-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient           102       58 0.57       147       69 0.47       105      108 1.03
json             Condition         455      203 0.45       816      404 0.50       280      258 0.92
json             Encounter         838      358 0.43     1,584      756 0.48       669      531 0.79
json             Observation       980      394 0.40     1,724      773 0.45       697      621 0.89
json             fit     0.40-0.57
json             encode  0.45-0.50
json             decode  0.79-1.03
```

### p6-budfnc-r?-514f8f8 against p6-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient           102       62 0.61       147       56 0.38       105      115 1.10
json             Condition         455      213 0.47       816      239 0.29       280      261 0.93
json             Encounter         838      363 0.43     1,584      457 0.29       669      525 0.79
json             Observation       980      403 0.41     1,724      520 0.30       697      636 0.91
json             fit     0.41-0.61
json             encode  0.29-0.38
json             decode  0.79-1.10
```

### p6-b-r?-514f8f8 against p6-a0-r1-514f8f8 (pooled minimum in ms, and the ratio)
```
source           type                            fit                  encode                  decode
json             Patient           102       59 0.58       147      142 0.97       105       99 0.95
json             Condition         455      198 0.43       816      396 0.49       280      265 0.95
json             Encounter         838      353 0.42     1,584      774 0.49       669      537 0.80
json             Observation       980      400 0.41     1,724      764 0.44       697      628 0.90
json             fit     0.41-0.58
json             encode  0.44-0.97
json             decode  0.80-0.95
```

### A0 r1 against r2 (the noise check; the verdict covers every source so far)
```
source           type         phase   p6-a0-r1- p6-a0-r2- ratio
json             Condition    decode        280       276  0.99  (under 1 s, not judged)
json             Condition    encode        816       805  0.99  (under 1 s, not judged)
json             Condition    fit           455       442  0.97  (under 1 s, not judged)
json             Encounter    decode        669       638  0.95  (under 1 s, not judged)
json             Encounter    encode      1,584     1,518  0.96
json             Encounter    fit           838       788  0.94  (under 1 s, not judged)
json             Observation  decode        697       707  1.01  (under 1 s, not judged)
json             Observation  encode      1,724     1,706  0.99
json             Observation  fit           980       992  1.01  (under 1 s, not judged)
json             Patient      decode        105       110  1.05  (under 1 s, not judged)
json             Patient      encode        147       147  1.00  (under 1 s, not judged)
json             Patient      fit           102        96  0.95  (under 1 s, not judged)
judged items 2; worst |ratio - 1| 0.042; median 0.026; ACCEPTABLE (limits 0.15 and 0.05)
```
