# SNOMED CT Identifiers (SCTID)

This document describes the structure, validation, and generation of SNOMED CT identifiers.

## SCTID data type

An SCTID is a 64-bit positive integer. When rendered as a string:

- Must always use decimal digits.
- Minimum length: 6 digits.
- Maximum length: 18 digits.
- Leading zeros are always omitted.

Example: `101291009` must not be rendered as `0101291009`.

## SCTID structure

Reading from right to left:

```
[item-identifier-digits][namespace-identifier][partition-identifier][check-digit]
```

### Check-digit (1 digit, rightmost)

The final digit is a check-digit computed using Verhoeff's dihedral group D5 algorithm. See the Check-digit Computation section below.

### Partition identifier (2 digits, second and third from right)

The partition identifier indicates both the component type and whether the ID uses short format (international) or long format (extension).

**Short format (international)**:

| PartitionId | Component type |
| ----------- | -------------- |
| 00          | Concept        |
| 01          | Description    |
| 02          | Relationship   |

**Long format (extension)**:

| PartitionId | Component type             |
| ----------- | -------------------------- |
| 10          | Concept                    |
| 11          | Description                |
| 12          | Relationship               |
| 16          | Postcoordinated expression |

All other partition identifier values are reserved for future use.

### Namespace identifier (7 digits, long format only)

For long-format SCTIDs (partition `1x`), the seven digits immediately to the left of the partition identifier are the namespace identifier. It is an integer value left-padded with zeros to 7 digits.

- Each authorized organization is allocated a namespace identifier by SNOMED International.
- The namespace does not hold semantic meaning.
- Allocated namespaces are represented in the Namespace Concept metadata sub-hierarchy.

Short-format SCTIDs (partition `0x`) do not have a namespace identifier.

### Item identifier digits

The remaining digits to the left of the namespace (for long format) or to the left of the partition (for short format) form the item identifier. These are allocated sequentially within each namespace and partition.

## Check-digit computation

SNOMED CT uses Verhoeff's dihedral group D5 check-digit algorithm.

### Arrays

```text
Dihedral D5 multiplication table:
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
  [1, 2, 3, 4, 0, 6, 7, 8, 9, 5]
  [2, 3, 4, 0, 1, 7, 8, 9, 5, 6]
  [3, 4, 0, 1, 2, 8, 9, 5, 6, 7]
  [4, 0, 1, 2, 3, 9, 5, 6, 7, 8]
  [5, 9, 8, 7, 6, 0, 4, 3, 2, 1]
  [6, 5, 9, 8, 7, 1, 0, 4, 3, 2]
  [7, 6, 5, 9, 8, 2, 1, 0, 4, 3]
  [8, 7, 6, 5, 9, 3, 2, 1, 0, 4]
  [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]

Function F array (8 rows, computed iteratively):
  F[0] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
  F[1] = [1, 5, 7, 6, 2, 8, 3, 0, 9, 4]
  F[i][j] = F[i - 1][F[1][j]] for i = 2..7

Inverse D5 array:
  [0, 4, 3, 2, 1, 5, 6, 7, 8, 9]
```

### Validating an SCTID

To validate the check-digit of an existing SCTID:

1. Start with `check = 0`.
2. Iterate over the digits from right to left.
3. For each digit at position `pos` (counting from the right, starting at 0):
    - `check = Dihedral[check][FnF[pos % 8][digit]]`
4. If the final `check` value is `0`, the identifier is valid.

### Computing a check-digit

To compute the check-digit for a partial identifier (without the check-digit):

1. Start with `check = 0`.
2. Iterate over the digits from right to left.
3. For each digit at position `pos` (counting from the right, starting at 0), use the position the digit will have **after** the check-digit is appended:
    - `check = Dihedral[check][FnF[(pos + 1) % 8][digit]]`
4. The check-digit is `InverseD5[check]`.

### Example implementations

**Python implementation**:

```python
def compute_verhoeff_check_digit(partial_id: str) -> int:
    """Compute the Verhoeff check-digit for a partial SCTID (without check-digit)."""
    dihedral = [
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        [1, 2, 3, 4, 0, 6, 7, 8, 9, 5],
        [2, 3, 4, 0, 1, 7, 8, 9, 5, 6],
        [3, 4, 0, 1, 2, 8, 9, 5, 6, 7],
        [4, 0, 1, 2, 3, 9, 5, 6, 7, 8],
        [5, 9, 8, 7, 6, 0, 4, 3, 2, 1],
        [6, 5, 9, 8, 7, 1, 0, 4, 3, 2],
        [7, 6, 5, 9, 8, 2, 1, 0, 4, 3],
        [8, 7, 6, 5, 9, 3, 2, 1, 0, 4],
        [9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
    ]

    fnf = [[0] * 10 for _ in range(8)]
    fnf[0] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    fnf[1] = [1, 5, 7, 6, 2, 8, 3, 0, 9, 4]
    for i in range(2, 8):
        for j in range(10):
            fnf[i][j] = fnf[i - 1][fnf[1][j]]

    inverse_d5 = [0, 4, 3, 2, 1, 5, 6, 7, 8, 9]

    check = 0
    length = len(partial_id)
    for i in range(length - 1, -1, -1):
        pos = length - i  # position after check-digit is appended
        digit = int(partial_id[i])
        check = dihedral[check][fnf[pos % 8][digit]]

    return inverse_d5[check]


def validate_sctid(sctid: str) -> bool:
    """Validate an SCTID string using the Verhoeff check-digit."""
    if not sctid or not sctid.isdigit():
        return False
    if len(sctid) < 6 or len(sctid) > 18:
        return False
    if sctid[0] == '0':
        return False

    dihedral = [
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        [1, 2, 3, 4, 0, 6, 7, 8, 9, 5],
        [2, 3, 4, 0, 1, 7, 8, 9, 5, 6],
        [3, 4, 0, 1, 2, 8, 9, 5, 6, 7],
        [4, 0, 1, 2, 3, 9, 5, 6, 7, 8],
        [5, 9, 8, 7, 6, 0, 4, 3, 2, 1],
        [6, 5, 9, 8, 7, 1, 0, 4, 3, 2],
        [7, 6, 5, 9, 8, 2, 1, 0, 4, 3],
        [8, 7, 6, 5, 9, 3, 2, 1, 0, 4],
        [9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
    ]

    fnf = [[0] * 10 for _ in range(8)]
    fnf[0] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    fnf[1] = [1, 5, 7, 6, 2, 8, 3, 0, 9, 4]
    for i in range(2, 8):
        for j in range(10):
            fnf[i][j] = fnf[i - 1][fnf[1][j]]

    check = 0
    length = len(sctid)
    for i in range(length - 1, -1, -1):
        pos = length - i - 1  # position counting from right, starting at 0
        digit = int(sctid[i])
        check = dihedral[check][fnf[pos % 8][digit]]

    return check == 0
```

## Why Verhoeff?

Verhoeff's check catches:

- All single errors (60-95% of human errors).
- All adjacent transpositions (10-20%).
- Over 95% of twin errors.
- Over 94% of jump transpositions and jump twin errors.
- Most phonetic errors.

This reduces the undetected error rate to approximately 2-3%, comparable to modulus 11, but without the drawback of generating a check-digit value of 10 that cannot be represented as a single decimal digit.

## Namespace hierarchy

Namespaces are represented in the metadata hierarchy under the Namespace Concept sub-hierarchy. Each allocated namespace is a descendant of the root namespace concept and is associated with the organization that owns it.
