# Phase timings at size l, per type

Generated from the three labels' result files. A ratio is taken on the minimum of five. A0/bar compares
514f8f8 with 26d7332, both on HEAD's own routes. from_xml/HAPI compares the from_xml route with HEAD's HAPI
route, both on the 514f8f8 jar. The extensions setting does not change the code on either commit, so each
pair of tables is two replicates of the same code. See `pooled.txt` for the ratios on the pooled ten.

**Extensions off.** Seconds, min (mean) of five.

| Source | Type | Phase | Bar 26d7332 | A0 514f8f8 | A0/bar | A0 from_xml | from_xml/HAPI |
|---|---|---|---|---|---|---|---|
| json | Patient | fit | 0.11 (0.12) | 0.11 (0.14) | 1.02 | - | - |
| json | Patient | encode | 0.16 (0.20) | 0.17 (0.20) | 1.09 | - | - |
| json | Patient | decode | 0.11 (0.13) | 0.14 (0.15) | 1.27 | - | - |
| json | Condition | fit | 0.51 (0.52) | 0.58 (0.60) | 1.13 | - | - |
| json | Condition | encode | 0.87 (0.91) | 0.98 (1.04) | 1.13 | - | - |
| json | Condition | decode | 0.30 (0.33) | 0.35 (0.35) | 1.15 | - | - |
| json | Encounter | fit | 0.86 (0.88) | 1.03 (1.06) | 1.20 | - | - |
| json | Encounter | encode | 1.66 (1.69) | 1.92 (1.97) | 1.16 | - | - |
| json | Encounter | decode | 0.67 (0.71) | 0.78 (0.83) | 1.17 | - | - |
| json | Observation | fit | 1.07 (1.16) | 1.20 (1.22) | 1.12 | - | - |
| json | Observation | encode | 1.92 (1.98) | 2.01 (2.06) | 1.05 | - | - |
| json | Observation | decode | 0.75 (1.12) | 0.86 (0.93) | 1.14 | - | - |
| xml-lines | Patient | fit | 0.31 (0.35) | 0.32 (0.40) | 1.04 | 0.38 (0.45) | 1.19 |
| xml-lines | Patient | encode | 0.32 (0.37) | 0.39 (0.45) | 1.22 | 0.37 (0.45) | 0.94 |
| xml-lines | Patient | decode | 0.32 (0.38) | 0.39 (0.46) | 1.22 | 0.65 (0.70) | 1.67 |
| xml-lines | Condition | fit | 2.36 (2.47) | 2.71 (2.80) | 1.15 | 1.23 (1.26) | 0.45 |
| xml-lines | Condition | encode | 2.68 (2.80) | 3.06 (3.19) | 1.14 | 2.00 (2.04) | 0.65 |
| xml-lines | Condition | decode | 2.58 (2.62) | 2.84 (2.95) | 1.10 | 2.27 (2.30) | 0.80 |
| xml-lines | Encounter | fit | 4.85 (4.87) | 4.90 (5.27) | 1.01 | 2.61 (2.72) | 0.53 |
| xml-lines | Encounter | encode | 5.52 (5.59) | 5.59 (5.91) | 1.01 | 4.66 (4.74) | 0.83 |
| xml-lines | Encounter | decode | 5.12 (5.16) | 5.05 (5.12) | 0.99 | 4.60 (4.62) | 0.91 |
| xml-lines | Observation | fit | 5.69 (5.77) | 5.64 (5.69) | 0.99 | 3.44 (3.55) | 0.61 |
| xml-lines | Observation | encode | 6.46 (6.51) | 6.36 (6.40) | 0.98 | 4.93 (5.00) | 0.78 |
| xml-lines | Observation | decode | 5.91 (5.96) | 5.84 (5.90) | 0.99 | 5.18 (5.23) | 0.89 |
| xml-bundles | Patient | fit | 4.35 (4.41) | 4.39 (4.49) | 1.01 | 7.87 (8.07) | 1.79 |
| xml-bundles | Patient | encode | 4.56 (4.65) | 4.66 (4.72) | 1.02 | 4.90 (4.99) | 1.05 |
| xml-bundles | Patient | decode | 0.62 (0.67) | 0.61 (0.74) | 0.99 | 0.87 (1.00) | 1.42 |
| xml-bundles | Condition | fit | 6.24 (6.29) | 6.29 (6.35) | 1.01 | 7.27 (7.49) | 1.16 |
| xml-bundles | Condition | encode | 6.67 (6.72) | 6.69 (6.83) | 1.00 | 5.25 (5.44) | 0.78 |
| xml-bundles | Condition | decode | 4.04 (4.06) | 4.05 (4.15) | 1.00 | 2.52 (2.56) | 0.62 |
| xml-bundles | Encounter | fit | 8.12 (8.17) | 8.25 (8.29) | 1.01 | 7.71 (8.04) | 0.93 |
| xml-bundles | Encounter | encode | 9.00 (9.06) | 9.11 (9.12) | 1.01 | 8.07 (8.22) | 0.89 |
| xml-bundles | Encounter | decode | 9.22 (9.27) | 9.24 (9.29) | 1.00 | 5.65 (5.72) | 0.61 |
| xml-bundles | Observation | fit | 9.01 (9.21) | 8.89 (8.94) | 0.99 | 7.36 (7.40) | 0.83 |
| xml-bundles | Observation | encode | 9.88 (10.18) | 9.54 (9.68) | 0.97 | 8.33 (8.50) | 0.87 |
| xml-bundles | Observation | decode | 15.23 (15.55) | 15.01 (15.07) | 0.99 | 7.71 (7.74) | 0.51 |
| xml-bundles-txn | Patient | fit | 5.19 (5.77) | 4.86 (5.02) | 0.94 | 8.90 (9.34) | 1.83 |
| xml-bundles-txn | Patient | encode | 5.71 (6.31) | 5.02 (5.17) | 0.88 | 5.90 (6.42) | 1.17 |
| xml-bundles-txn | Patient | decode | 0.73 (0.85) | 0.63 (0.73) | 0.87 | 0.83 (1.01) | 1.30 |
| xml-bundles-txn | Condition | fit | 7.68 (8.03) | 6.65 (6.87) | 0.87 | 9.09 (9.43) | 1.37 |
| xml-bundles-txn | Condition | encode | 8.26 (8.92) | 7.21 (7.29) | 0.87 | 6.89 (7.17) | 0.96 |
| xml-bundles-txn | Condition | decode | 4.27 (4.49) | 3.95 (4.02) | 0.93 | 2.57 (2.66) | 0.65 |
| xml-bundles-txn | Encounter | fit | 10.25 (10.39) | 8.71 (8.79) | 0.85 | 9.14 (9.46) | 1.05 |
| xml-bundles-txn | Encounter | encode | 10.82 (11.33) | 9.50 (9.61) | 0.88 | 9.20 (9.71) | 0.97 |
| xml-bundles-txn | Encounter | decode | 9.68 (9.80) | 9.11 (9.16) | 0.94 | 5.57 (5.81) | 0.61 |
| xml-bundles-txn | Observation | fit | 11.13 (11.71) | 9.52 (10.21) | 0.86 | 8.78 (9.27) | 0.92 |
| xml-bundles-txn | Observation | encode | 11.94 (12.75) | 10.16 (10.94) | 0.85 | 10.33 (10.90) | 1.02 |
| xml-bundles-txn | Observation | decode | 15.50 (15.60) | 15.00 (15.16) | 0.97 | 7.56 (7.61) | 0.50 |

**Extensions on.** Seconds, min (mean) of five.

| Source | Type | Phase | Bar 26d7332 | A0 514f8f8 | A0/bar | A0 from_xml | from_xml/HAPI |
|---|---|---|---|---|---|---|---|
| json | Patient | fit | 0.11 (0.14) | 0.11 (0.14) | 0.94 | - | - |
| json | Patient | encode | 0.17 (0.20) | 0.15 (0.20) | 0.90 | - | - |
| json | Patient | decode | 0.14 (0.16) | 0.11 (0.12) | 0.77 | - | - |
| json | Condition | fit | 0.53 (0.56) | 0.46 (0.50) | 0.87 | - | - |
| json | Condition | encode | 0.90 (0.94) | 0.84 (0.87) | 0.94 | - | - |
| json | Condition | decode | 0.46 (0.47) | 0.30 (0.35) | 0.66 | - | - |
| json | Encounter | fit | 0.94 (0.99) | 0.84 (0.87) | 0.90 | - | - |
| json | Encounter | encode | 1.78 (1.84) | 1.58 (1.66) | 0.89 | - | - |
| json | Encounter | decode | 1.00 (1.05) | 0.73 (0.76) | 0.73 | - | - |
| json | Observation | fit | 1.26 (1.30) | 1.04 (1.17) | 0.82 | - | - |
| json | Observation | encode | 1.99 (2.09) | 1.82 (1.94) | 0.92 | - | - |
| json | Observation | decode | 0.80 (0.89) | 0.81 (0.87) | 1.01 | - | - |
| xml-lines | Patient | fit | 0.29 (0.37) | 0.27 (0.39) | 0.93 | 0.39 (0.43) | 1.43 |
| xml-lines | Patient | encode | 0.35 (0.40) | 0.32 (0.38) | 0.92 | 0.32 (0.38) | 1.01 |
| xml-lines | Patient | decode | 0.34 (0.42) | 0.34 (0.42) | 1.00 | 0.61 (0.65) | 1.78 |
| xml-lines | Condition | fit | 2.47 (2.57) | 2.28 (2.38) | 0.92 | 1.14 (1.24) | 0.50 |
| xml-lines | Condition | encode | 2.82 (2.95) | 2.62 (2.66) | 0.93 | 1.74 (1.91) | 0.67 |
| xml-lines | Condition | decode | 2.55 (2.60) | 2.42 (2.46) | 0.95 | 1.38 (1.41) | 0.57 |
| xml-lines | Encounter | fit | 5.01 (5.07) | 4.64 (4.78) | 0.93 | 2.62 (2.66) | 0.56 |
| xml-lines | Encounter | encode | 5.79 (5.89) | 5.35 (5.53) | 0.92 | 4.38 (4.53) | 0.82 |
| xml-lines | Encounter | decode | 5.28 (5.41) | 5.08 (5.23) | 0.96 | 2.80 (2.88) | 0.55 |
| xml-lines | Observation | fit | 5.78 (5.84) | 5.85 (6.08) | 1.01 | 3.46 (3.66) | 0.59 |
| xml-lines | Observation | encode | 6.54 (6.64) | 6.52 (6.75) | 1.00 | 4.58 (4.70) | 0.70 |
| xml-lines | Observation | decode | 6.09 (6.15) | 6.04 (6.28) | 0.99 | 3.06 (3.14) | 0.51 |
| xml-bundles | Patient | fit | 5.24 (5.41) | 4.89 (5.08) | 0.93 | 8.06 (8.37) | 1.65 |
| xml-bundles | Patient | encode | 5.53 (5.61) | 5.12 (5.26) | 0.92 | 4.87 (5.25) | 0.95 |
| xml-bundles | Patient | decode | 0.89 (1.05) | 0.58 (0.69) | 0.65 | 0.80 (0.91) | 1.38 |
| xml-bundles | Condition | fit | 7.93 (8.05) | 6.72 (6.88) | 0.85 | 7.99 (8.25) | 1.19 |
| xml-bundles | Condition | encode | 8.24 (8.37) | 7.29 (7.43) | 0.88 | 5.61 (5.81) | 0.77 |
| xml-bundles | Condition | decode | 4.65 (4.82) | 4.18 (4.22) | 0.90 | 2.53 (2.69) | 0.60 |
| xml-bundles | Encounter | fit | 9.80 (9.99) | 8.72 (8.82) | 0.89 | 7.99 (8.41) | 0.92 |
| xml-bundles | Encounter | encode | 10.96 (11.15) | 9.64 (9.74) | 0.88 | 8.23 (8.57) | 0.85 |
| xml-bundles | Encounter | decode | 10.83 (11.00) | 9.85 (10.15) | 0.91 | 5.39 (5.47) | 0.55 |
| xml-bundles | Observation | fit | 10.83 (11.00) | 9.82 (10.49) | 0.91 | 8.01 (8.36) | 0.82 |
| xml-bundles | Observation | encode | 12.10 (12.18) | 10.81 (11.21) | 0.89 | 9.03 (9.44) | 0.83 |
| xml-bundles | Observation | decode | 17.43 (17.64) | 16.21 (16.47) | 0.93 | 7.46 (7.80) | 0.46 |
| xml-bundles-txn | Patient | fit | 5.73 (5.89) | 5.12 (5.40) | 0.89 | 8.43 (8.65) | 1.65 |
| xml-bundles-txn | Patient | encode | 5.99 (6.14) | 5.37 (5.49) | 0.90 | 5.81 (5.98) | 1.08 |
| xml-bundles-txn | Patient | decode | 0.72 (0.84) | 0.65 (0.77) | 0.90 | 0.81 (0.95) | 1.26 |
| xml-bundles-txn | Condition | fit | 8.12 (8.53) | 6.91 (7.19) | 0.85 | 8.34 (8.55) | 1.21 |
| xml-bundles-txn | Condition | encode | 8.70 (9.11) | 7.43 (7.57) | 0.85 | 6.61 (6.72) | 0.89 |
| xml-bundles-txn | Condition | decode | 4.42 (4.62) | 3.97 (4.09) | 0.90 | 2.59 (2.81) | 0.65 |
| xml-bundles-txn | Encounter | fit | 10.05 (10.73) | 8.84 (9.29) | 0.88 | 8.37 (8.62) | 0.95 |
| xml-bundles-txn | Encounter | encode | 11.15 (11.96) | 10.08 (10.30) | 0.90 | 8.70 (9.38) | 0.86 |
| xml-bundles-txn | Encounter | decode | 9.92 (10.03) | 9.18 (9.43) | 0.93 | 5.58 (5.92) | 0.61 |
| xml-bundles-txn | Observation | fit | 10.75 (10.83) | 9.81 (10.20) | 0.91 | 8.42 (9.03) | 0.86 |
| xml-bundles-txn | Observation | encode | 11.62 (11.75) | 10.89 (11.21) | 0.94 | 10.04 (10.42) | 0.92 |
| xml-bundles-txn | Observation | decode | 15.59 (15.72) | 15.08 (15.42) | 0.97 | 7.56 (7.94) | 0.50 |
