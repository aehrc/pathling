#!/usr/bin/env python

#  Copyright 2023 Commonwealth Scientific and Industrial Research
#  Organisation (CSIRO) ABN 41 687 119 230.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


import os

from pathling import PathlingContext
from pathling.bulkexport import BulkExportClient
from datetime import datetime, timezone, timedelta
from time import time

from pathling.fhir import as_fhir_instant, from_fhir_instant
from pathling.config import auth_config

HERE = os.path.abspath(os.path.dirname(__file__))
BASE_DIR = os.path.abspath(os.path.join(HERE, os.pardir))
TARGET_DIR = os.path.abspath(os.path.join(BASE_DIR, "target"))

CLIENT_ID = """eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InJlZ2lzdHJhdGlvbi10b2tlbiJ9.eyJqd2tzIjp7ImtleXMiOlt7Imt0eSI6IlJTQSIsImFsZyI6IlJTMzg0Iiw
ibiI6InUwcWVzcGNUNzNSY0xQQlU3VVpheVVWLVJ5NXJfWG5OUkEtdUF6RjJxMEFLdUlFSEp3SjlwRDRYeURQa1hJc3FqWDFYSGhhZWMzS19LZGFncVVHcVRxd3ZRa2pXV2pJc1NiWnZqOWktb
V8tOU9pT2JOVmNiRm1ESXBvY1hZaDk1TE9xNlNveDB3ZFI5SnFjZEtYX1YxcU8yclRuYkZTdGdRZlRmT1B4b2pfYkFoa3hwbmhpZEVmMkFDZDRWQXdROVFkRHhUY19MOW9kVzAySWpTbUN0OGR
uVE1ISW5nM0l0NWxQRC1qaE1IRFhzd3NGSW8tbFJVZ0RPb1Yzekw3X1VUT21VZWZXcHVfNkY4R3M3QUo3NnJ3ZzM1RXc4d2xSNTV5ZV8zdjl5UjctR1ZJQmhndXNzTnFMYWxkcVVQN29yRmFmO
Fp2MV9DVGRGVFdVMENVdXRVdyIsImUiOiJBUUFCIiwia2V5X29wcyI6WyJ2ZXJpZnkiXSwiZXh0Ijp0cnVlLCJraWQiOiI2YzdiNjhjMmJjOWY2MTdjM2IyMzg1OGY4ZDFhMGNkMSJ9LHsia3R
5IjoiUlNBIiwiYWxnIjoiUlMzODQiLCJuIjoidTBxZXNwY1Q3M1JjTFBCVTdVWmF5VVYtUnk1cl9Ybk5SQS11QXpGMnEwQUt1SUVISndKOXBENFh5RFBrWElzcWpYMVhIaGFlYzNLX0tkYWdxV
UdxVHF3dlFraldXaklzU2Jadmo5aS1tXy05T2lPYk5WY2JGbURJcG9jWFloOTVMT3E2U294MHdkUjlKcWNkS1hfVjFxTzJyVG5iRlN0Z1FmVGZPUHhval9iQWhreHBuaGlkRWYyQUNkNFZBd1E
5UWREeFRjX0w5b2RXMDJJalNtQ3Q4ZG5UTUhJbmczSXQ1bFBELWpoTUhEWHN3c0ZJby1sUlVnRE9vVjN6TDdfVVRPbVVlZldwdV82RjhHczdBSjc2cndnMzVFdzh3bFI1NXllXzN2OXlSNy1HV
klCaGd1c3NOcUxhbGRxVVA3b3JGYWY4WnYxX0NUZEZUV1UwQ1V1dFV3IiwiZSI6IkFRQUIiLCJkIjoiR1NHSTl0OXZ5akhidG5Ta2ZnRnlrczd6Z190TGUtNTVQUmlQaEcwc01Rd1RCREV5czZ
qUlpzQlFNa25JUUNTMUFvUkppWEl4TTc0M2FXdnhFSlVNNmFrNnRDaGxpWGZIQlRob3lDMVFza3BZRlVlTUtZQktmd1VMUWlaMEJ2TnJMLWJxaXJDYkdzWjNrLXF1US1YR2gwVUl4ajRIcUNrS
VdGYW52SlB6TnpIWUJsWmlxV1VIVFRiX2xyS3U2MXlTUWYzWDN2Y3NUUk9fSjdxcEk3QXNXRWw2Y20waEcyN2hDQy04OHk4bzFnQ0VpaGpSS1k3SllocXg1NkhUUF9nSGphNWc1MUJFbEIzMzg
teTJNSG5ISERIbDhWT1RtNFZveGdqSFg5eEh4bnRpTTVfalg5N1FLSzdBQVpIQklRNkZtZ2JZSGZQaU9OQVJUejJ3Q3RrUm1RIiwicCI6InhKallTb1JZcHVERzZOd1Q1QnNncDBpNjV5V3h2a
WxYdjZYMXJ1TC1NOFBvSE1OSS01c0xVOFJXMnVqU3JMdzBZTG5jWDlaZ0VTMFdZNGQtTmpSbW80SnJXT3JubDVLNnpjZkhRblhTUDhSckhWcU1VaGtzaG52MEtVM0NxNE5UdXlTZjJRWWxBNWk
3S0VaSDVWNklyUWpJRmM0NVVSSTJRX1dTSlZLTzhpcyIsInEiOiI4LUg0eUNDVFpYR1lDQ1Q1eU54V1FiUWd6MXktbUZOQzhWcnpDNEt2MUhSdGhXTG0xeVhfTVV6QkdPWnM3SThYem1aOGdqa
2VVNUhJN1BBTWc4eDAzWGFqMDlNa0xlU2ZvR2d6UkptSDhBZmxBbXJzMHFCa25fTFZuVm5mZGdZMmZZckZDWnpmbnRrQThMT2R1WjlVVnItazFhZXZsQVg4NGVkUWJVR3hKWGsiLCJkcCI6Imd
wOXJWbGVJNzVmNWtVNW9pTzdkUzBpdnBsVU5PLVFNUVhIclF6X2UxbFljS3NBMkJiYUdXQ25qNjU4Mkd6OFJMaE5Qc1AybTVzS21WWXI0LU5yb04wdTROMlFjYkVFNWpQQm9IR2hjUWJJbjJXR
UlTcVFmU1Z1aVpxOGI0UWxvNExiODNoMlBDa3k0VFpJa3d1bUV0ak5YZmVWX2Y3WXlDQVZxRFJKRmxfRSIsImRxIjoial9WYVlkLUF3Z2U4ZzBvNE80MThTUXBudDU5NlRmWVA5T0lIeDBxY09
aLUVLcEZIeThYOURmX05sOElrbDdxYmhkVlBONXM1d0lDMGhzX253MEREMXhvNS1FYVEtNW9SYzZFdWFoYnFmQkJjNlFGdXk2R3I1QkoyYjR4bWNJZVlVS0RDVUR0T2NMaU5hb2ZMMlk4b1BKQ
2hYeEM5Zm5YT1lSYW1qZWVTQnBrIiwicWkiOiJNZkVtQ1lPRlJCN05WX29sVldyd0lOdWQ2dHJkUFpKWEZTS2gyR2hHc0tOcHVfaG84TlRCQW81SW9fdVU2STc4QmgwUnR2NW43UUZsZHFmZHV
mdUQxbzB4Q2ZqdDV5OHpvNjRzSUIxS2JHX3Z2dWtTcjNaVE54bDJoUmdScE9LZ3ZIRVdoWWZUYktBOUtKYzZ2bm4tM3N1MVFoQzAwWlNVU3JkTEd0bXg0cmMiLCJrZXlfb3BzIjpbInNpZ24iX
SwiZXh0Ijp0cnVlLCJraWQiOiI2YzdiNjhjMmJjOWY2MTdjM2IyMzg1OGY4ZDFhMGNkMSJ9XX0sImFjY2Vzc1Rva2Vuc0V4cGlyZUluIjoxNSwiaWF0IjoxNzA5MTc5ODQxfQ.OWC0-bLt3-PS
lVkp_EnFhiBijCQFBtNbk4bnZxm4iiM"""

PRIVATE_KEY_JWK = """{
  "kty": "RSA",
  "alg": "RS384",
  "n": "u0qespcT73RcLPBU7UZayUV-Ry5r_XnNRA-uAzF2q0AKuIEHJwJ9pD4XyDPkXIsqjX1XHhaec3K_KdagqUGqTqwvQkjWWjIsSbZvj9i-m_-9OiObNVcbFmDIpocXYh95LOq6Sox0wdR9JqcdKX_V1qO2rTnbFStgQfTfOPxoj_bAhkxpnhidEf2ACd4VAwQ9QdDxTc_L9odW02IjSmCt8dnTMHIng3It5lPD-jhMHDXswsFIo-lRUgDOoV3zL7_UTOmUefWpu_6F8Gs7AJ76rwg35Ew8wlR55ye_3v9yR7-GVIBhgussNqLaldqUP7orFaf8Zv1_CTdFTWU0CUutUw",
  "e": "AQAB",
  "d": "GSGI9t9vyjHbtnSkfgFyks7zg_tLe-55PRiPhG0sMQwTBDEys6jRZsBQMknIQCS1AoRJiXIxM743aWvxEJUM6ak6tChliXfHBThoyC1QskpYFUeMKYBKfwULQiZ0BvNrL-bqirCbGsZ3k-quQ-XGh0UIxj4HqCkIWFanvJPzNzHYBlZiqWUHTTb_lrKu61ySQf3X3vcsTRO_J7qpI7AsWEl6cm0hG27hCC-88y8o1gCEihjRKY7JYhqx56HTP_gHja5g51BElB338-y2MHnHHDHl8VOTm4VoxgjHX9xHxntiM5_jX97QKK7AAZHBIQ6FmgbYHfPiONARTz2wCtkRmQ",
  "p": "xJjYSoRYpuDG6NwT5Bsgp0i65yWxvilXv6X1ruL-M8PoHMNI-5sLU8RW2ujSrLw0YLncX9ZgES0WY4d-NjRmo4JrWOrnl5K6zcfHQnXSP8RrHVqMUhkshnv0KU3Cq4NTuySf2QYlA5i7KEZH5V6IrQjIFc45URI2Q_WSJVKO8is",
  "q": "8-H4yCCTZXGYCCT5yNxWQbQgz1y-mFNC8VrzC4Kv1HRthWLm1yX_MUzBGOZs7I8XzmZ8gjkeU5HI7PAMg8x03Xaj09MkLeSfoGgzRJmH8AflAmrs0qBkn_LVnVnfdgY2fYrFCZzfntkA8LOduZ9UVr-k1aevlAX84edQbUGxJXk",
  "dp": "gp9rVleI75f5kU5oiO7dS0ivplUNO-QMQXHrQz_e1lYcKsA2BbaGWCnj6582Gz8RLhNPsP2m5sKmVYr4-NroN0u4N2QcbEE5jPBoHGhcQbIn2WEISqQfSVuiZq8b4Qlo4Lb83h2PCky4TZIkwumEtjNXfeV_f7YyCAVqDRJFl_E",
  "dq": "j_VaYd-Awge8g0o4O418SQpnt596TfYP9OIHx0qcOZ-EKpFHy8X9Df_Nl8Ikl7qbhdVPN5s5wIC0hs_nw0DD1xo5-EaQ-5oRc6EuahbqfBBc6QFuy6Gr5BJ2b4xmcIeYUKDCUDtOcLiNaofL2Y8oPJChXxC9fnXOYRamjeeSBpk",
  "qi": "MfEmCYOFRB7NV_olVWrwINud6trdPZJXFSKh2GhGsKNpu_ho8NTBAo5Io_uU6I78Bh0Rtv5n7QFldqfdufuD1o0xCfjt5y8zo64sIB1KbG_vvukSr3ZTNxl2hRgRpOKgvHEWhYfTbKA9KJc6vnn-3su1QhC00ZSUSrdLGtmx4rc",
  "key_ops": [
    "sign"
  ],
  "ext": true,
  "kid": "6c7b68c2bc9f617c3b23858f8d1a0cd1"
}"""


def main():
    """
    Main function to export data from a FHIR server to a local directory
    from the Smart Health IT Bulk Data Server using the `client-confidential-asymmetric`
    authentication method
    """

    # create the context to initialize Java backend for SparkSession
    PathlingContext.create(log_level="DEBUG")
    outputDirUrl = os.path.join(TARGET_DIR, "export-%s" % int(time()))
    fhirEndpointUrl = "https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjAsInNlY3VyZSI6MX0/fhir"

    print(f"Exporting from: {fhirEndpointUrl} to {outputDirUrl}")
    result = BulkExportClient(
        fhirEndpointUrl,
        outputDirUrl,
        types=["Patient", "Condition"],
        timeout=timedelta(minutes=30),
        auth_config=auth_config(
            auth_enabled=True,
            auth_client_id=CLIENT_ID.replace("\n", ""),
            auth_private_key_jwk=PRIVATE_KEY_JWK,
            auth_scope="system/*.read",
            auth_token_expiry_tolerance=30,
        ),
    ).export()
    print(f"TransactionTime: {as_fhir_instant(result.transaction_time)}")


if __name__ == "__main__":
    main()
