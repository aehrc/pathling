#  Copyright 2025 Commonwealth Scientific and Industrial Research
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

import os
import tempfile
from datetime import datetime, timezone

from pathling import PathlingContext

jwk = """
{
  "kty": "RSA",
  "alg": "RS384",
  "n": "llps-ufRYIVplRdtF2FB1xn5iCuojOyHmCmkLsMLU2zsUNcpwxlFUmv9xQRHTArfGm_vRkKjtx4dsW8LMG45EvDK_a6TmLF5H5hDlCqr0aXuInpN-c3f6f9d0zRtBCc18IKHL_IBskoaHGK4LVdQypIPcLqMiKkPFXI-NRwtJLUpQt6NH_p8vW0fiIRbkdC1t2pSrPX0307et38IE_vv8_RZm3CAKef2pnbWzRUBleeQybqaR28VNNallixegt1Sh5ShQLfQvA0QmrST25Kzs5K0d_6eAKl4xPDp1Q_dC1N4mygMZAkbRXKdq49Pg9C-56pbzEmvOYiM_CtMWkzr9w",
  "e": "AQAB",
  "d": "By9zHdqOSwqVLSbdc8yWFO2M21Ea0QFMyZzT19hCZk5CTOq7eDNw-KtoiU3XCm9KkjzfNoBgypOJ37zqz_m0iI8xZEY_j4CLxVLFiAMyCubfJo6pw1JvbQNjPIC45QXqsf_K7iOmqRqZfNnK63_MwKGSU1TW-oD505COIIOkNKjQ7KpIOm56EfyH2_cPUfmlHsBCRGy6eQ2M8cSK-uxXchSrNqt46nD8ArCE8qtrGJn1zJTgWOkH2lS73uzkc_P6rGg3IdiAbmPl4HWU-PlJ2jwykFbbXhnzL3Tpruc8okR_cda5u7KSa8dfV5WPjnygTxPHNt5_iuszPKxa0X9nwQ",
  "p": "1DcRbY_DevTMMni3WynbKGm-MXmnH7NMU-4IU1hdegZfrStoBC2DngP77JILRO_TApaMPiAkIpxIpgvovnWKtCZ3-2BXDWnd4x_Ews3BUUVzCjvxAatLTiSq_lZTAL93Htqf3FQPa86Q2x4lyvJ-rFWBfpONzMGr-5g9ut1sGbk",
  "q": "tV_kQ3ggaBSYRkckrpWnKJI3-uREyZVI_-PTK8kUS43Glz12sxVpYIIRqt57XtArkpHG9_YjUxj_ROF_LjSFaGbCxmccPqu9tHr7JIsuVWQlz8ooxXNW3lURMCtKd3k2xm9FhoFmtncP7nLbCfVaBIlTLhaXZXVZSSUv-vDDSy8",
  "dp": "dVk-OeeVoRhdEkvOmIq8tcxDb_hlghIT0xV9ZRkoF6IOpiOqkSTZ8zcgx-C6epRjirrVMkVzte_V_Hv5Z9h3qsba8haEDNbN7BpVI6PDkr1kr_QVgWbHbZ65L4tsuq0lodojLCMPo_3F_GTfYSpXAdUGlofhkahHAgldmUd3z4E",
  "dq": "O6MdHiYombBz5V_NKu6gORHjAEcAazv_9cvGirYiSzmB3AbkubvHm2kJQCLJdAKE4Tu3rZ6sPM2SWea_d8TjPNHVJ4GN4vl7dhWd8IUnJgK5ABrbzxi-rnpQHYOOh7w-i37Y4II58LMzdNclOKAJCkbRJ-1buIueYROuNBfoTxc",
  "qi": "CPlT4vGuJbV-WMLIRL4c-VW0H0fwRUljqvv-_nNDQyZ98uFlXYLtmQS2h3VX4WjK1UR8Ca3m9110JNe8Va_7Tepuk13p4CyMG0ccGojzl50fvfrINj1zN6jz0lRI4cAPWdfGwgEs0tpvtW1saVrg9y89XefEx8Iq2Z0bLrlKGrU",
  "key_ops": [
    "sign"
  ],
  "ext": true,
  "kid": "b2979595c62deb396306ba3edbdfb4a0"
}
"""

client_id = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InJlZ2lzdHJhdGlvbi10b2tlbiJ9.eyJqd2tzIjp7ImtleXMiOlt7Imt0eSI6IlJTQSIsImFsZyI6IlJTMzg0IiwibiI6ImxscHMtdWZSWUlWcGxSZHRGMkZCMXhuNWlDdW9qT3lIbUNta0xzTUxVMnpzVU5jcHd4bEZVbXY5eFFSSFRBcmZHbV92UmtLanR4NGRzVzhMTUc0NUV2REtfYTZUbUxGNUg1aERsQ3FyMGFYdUlucE4tYzNmNmY5ZDB6UnRCQ2MxOElLSExfSUJza29hSEdLNExWZFF5cElQY0xxTWlLa1BGWEktTlJ3dEpMVXBRdDZOSF9wOHZXMGZpSVJia2RDMXQycFNyUFgwMzA3ZXQzOElFX3Z2OF9SWm0zQ0FLZWYycG5iV3pSVUJsZWVReWJxYVIyOFZOTmFsbGl4ZWd0MVNoNVNoUUxmUXZBMFFtclNUMjVLenM1SzBkXzZlQUtsNHhQRHAxUV9kQzFONG15Z01aQWtiUlhLZHE0OVBnOUMtNTZwYnpFbXZPWWlNX0N0TVdrenI5dyIsImUiOiJBUUFCIiwia2V5X29wcyI6WyJ2ZXJpZnkiXSwiZXh0Ijp0cnVlLCJraWQiOiJiMjk3OTU5NWM2MmRlYjM5NjMwNmJhM2VkYmRmYjRhMCJ9LHsia3R5IjoiUlNBIiwiYWxnIjoiUlMzODQiLCJuIjoibGxwcy11ZlJZSVZwbFJkdEYyRkIxeG41aUN1b2pPeUhtQ21rTHNNTFUyenNVTmNwd3hsRlVtdjl4UVJIVEFyZkdtX3ZSa0tqdHg0ZHNXOExNRzQ1RXZES19hNlRtTEY1SDVoRGxDcXIwYVh1SW5wTi1jM2Y2ZjlkMHpSdEJDYzE4SUtITF9JQnNrb2FIR0s0TFZkUXlwSVBjTHFNaUtrUEZYSS1OUnd0SkxVcFF0Nk5IX3A4dlcwZmlJUmJrZEMxdDJwU3JQWDAzMDdldDM4SUVfdnY4X1JabTNDQUtlZjJwbmJXelJVQmxlZVF5YnFhUjI4Vk5OYWxsaXhlZ3QxU2g1U2hRTGZRdkEwUW1yU1QyNUt6czVLMGRfNmVBS2w0eFBEcDFRX2RDMU40bXlnTVpBa2JSWEtkcTQ5UGc5Qy01NnBiekVtdk9ZaU1fQ3RNV2t6cjl3IiwiZSI6IkFRQUIiLCJkIjoiQnk5ekhkcU9Td3FWTFNiZGM4eVdGTzJNMjFFYTBRRk15WnpUMTloQ1prNUNUT3E3ZUROdy1LdG9pVTNYQ205S2tqemZOb0JneXBPSjM3enF6X20waUk4eFpFWV9qNENMeFZMRmlBTXlDdWJmSm82cHcxSnZiUU5qUElDNDVRWHFzZl9LN2lPbXFScVpmTm5LNjNfTXdLR1NVMVRXLW9ENTA1Q09JSU9rTktqUTdLcElPbTU2RWZ5SDJfY1BVZm1sSHNCQ1JHeTZlUTJNOGNTSy11eFhjaFNyTnF0NDZuRDhBckNFOHF0ckdKbjF6SlRnV09rSDJsUzczdXprY19QNnJHZzNJZGlBYm1QbDRIV1UtUGxKMmp3eWtGYmJYaG56TDNUcHJ1Yzhva1JfY2RhNXU3S1NhOGRmVjVXUGpueWdUeFBITnQ1X2l1c3pQS3hhMFg5bndRIiwicCI6IjFEY1JiWV9EZXZUTU1uaTNXeW5iS0dtLU1YbW5IN05NVS00SVUxaGRlZ1pmclN0b0JDMkRuZ1A3N0pJTFJPX1RBcGFNUGlBa0lweElwZ3Zvdm5XS3RDWjMtMkJYRFduZDR4X0V3czNCVVVWekNqdnhBYXRMVGlTcV9sWlRBTDkzSHRxZjNGUVBhODZRMng0bHl2Si1yRldCZnBPTnpNR3ItNWc5dXQxc0diayIsInEiOiJ0Vl9rUTNnZ2FCU1lSa2NrcnBXbktKSTMtdVJFeVpWSV8tUFRLOGtVUzQzR2x6MTJzeFZwWUlJUnF0NTdYdEFya3BIRzlfWWpVeGpfUk9GX0xqU0ZhR2JDeG1jY1BxdTl0SHI3SklzdVZXUWx6OG9veFhOVzNsVVJNQ3RLZDNrMnhtOUZob0ZtdG5jUDduTGJDZlZhQklsVExoYVhaWFZaU1NVdi12RERTeTgiLCJkcCI6ImRWay1PZWVWb1JoZEVrdk9tSXE4dGN4RGJfaGxnaElUMHhWOVpSa29GNklPcGlPcWtTVFo4emNneC1DNmVwUmppcnJWTWtWenRlX1ZfSHY1WjloM3FzYmE4aGFFRE5iTjdCcFZJNlBEa3Ixa3JfUVZnV2JIYlo2NUw0dHN1cTBsb2RvakxDTVBvXzNGX0dUZllTcFhBZFVHbG9maGthaEhBZ2xkbVVkM3o0RSIsImRxIjoiTzZNZEhpWW9tYkJ6NVZfTkt1NmdPUkhqQUVjQWF6dl85Y3ZHaXJZaVN6bUIzQWJrdWJ2SG0ya0pRQ0xKZEFLRTRUdTNyWjZzUE0yU1dlYV9kOFRqUE5IVko0R040dmw3ZGhXZDhJVW5KZ0s1QUJyYnp4aS1ybnBRSFlPT2g3dy1pMzdZNElJNThMTXpkTmNsT0tBSkNrYlJKLTFidUl1ZVlST3VOQmZvVHhjIiwicWkiOiJDUGxUNHZHdUpiVi1XTUxJUkw0Yy1WVzBIMGZ3UlVsanF2di1fbk5EUXlaOTh1RmxYWUx0bVFTMmgzVlg0V2pLMVVSOENhM205MTEwSk5lOFZhXzdUZXB1azEzcDRDeU1HMGNjR29qemw1MGZ2ZnJJTmoxek42anowbFJJNGNBUFdkZkd3Z0VzMHRwdnRXMXNhVnJnOXk4OVhlZkV4OElxMlowYkxybEtHclUiLCJrZXlfb3BzIjpbInNpZ24iXSwiZXh0Ijp0cnVlLCJraWQiOiJiMjk3OTU5NWM2MmRlYjM5NjMwNmJhM2VkYmRmYjRhMCJ9XX0sImFjY2Vzc1Rva2Vuc0V4cGlyZUluIjoxNSwiaWF0IjoxNzQwMzY3MDU0fQ.avoHoKI9g_2fmoRxZB0QnscRgEqb9xHip9CU_f-2U1I"


def test_bulk_exports():
    # Initialize PathlingContext.
    pc = PathlingContext.create()

    # Base parameters from the demo server
    fhir_server = "https://bulk-data.smarthealthit.org/fhir"
    output_base = os.path.join(tempfile.gettempdir(), "bulk_export_test")
    
    if os.path.exists(output_base):
        import shutil
        shutil.rmtree(output_base)
    os.makedirs(output_base)

    # Test 1: System level export with all parameters.
    print("\n=== Testing system level export with all parameters ===")
    pc.read.bulk(
        fhir_endpoint_url=fhir_server,
        output_dir=f"{output_base}/system_detailed",
        output_format="application/fhir+ndjson",
        since=datetime(2015, 1, 1, tzinfo=timezone.utc),
        types=["Patient", "Observation"],
        elements=["id", "status"],
        include_associated_data=["LatestProvenanceResources"],
        type_filters=["Patient?status=active"],
        output_extension="ndjson",
        timeout=3600,
        auth_config={
            "enabled": True,
            "client_id": client_id,
            "private_key_jwk": jwk,
            "use_smart": True,
            "use_form_for_basic_auth": False,
            "scope": "system/*.read",
            "token_expiry_tolerance": 30
        }
    )
    print("System export completed successfully")

    # Test 2: Group level export with minimal parameters.
    print("\n=== Testing group level export with minimal parameters ===")
    pc.read.bulk(
        fhir_endpoint_url=fhir_server,
        output_dir=f"{output_base}/group_basic",
        group_id="BMCHealthNet"
    )
    print("Group export completed successfully")

    # Test 3: Group level export with all parameters.
    print("\n=== Testing group level export with all parameters ===")
    pc.read.bulk(
        fhir_endpoint_url=fhir_server,
        output_dir=f"{output_base}/group_detailed",
        group_id="BMCHealthNet",
        output_format="application/fhir+ndjson",
        since=datetime(2015, 1, 1, tzinfo=timezone.utc),
        types=["Patient", "Condition", "Observation"],
        elements=["id", "status"],
        include_associated_data=["LatestProvenanceResources"],
        type_filters=["Patient?status=active"],
        output_extension="ndjson",
        timeout=1800,
        max_concurrent_downloads=8
    )
    print("Group export completed successfully")

    # Test 4: Patient level export with minimal parameters.
    print("\n=== Testing patient level export with minimal parameters ===")
    pc.read.bulk(
        fhir_endpoint_url=fhir_server,
        output_dir=f"{output_base}/patient_basic",
        patients=[
            "Patient/58c297c4-d684-4677-8024-01131d93835e",
            "Patient/118616a4-f0b2-411f-8050-39d5d27c738c"
        ]
    )
    print("Patient export completed successfully")

    # Test 5: Patient level export with all parameters.
    print("\n=== Testing patient level export with all parameters ===")
    pc.read.bulk(
        fhir_endpoint_url=fhir_server,
        output_dir=f"{output_base}/patient_detailed",
        patients=[
            "Patient/58c297c4-d684-4677-8024-01131d93835e",
            "Patient/118616a4-f0b2-411f-8050-39d5d27c738c",
            "Patient/21fba439-ca79-411f-a081-37a432a78f3a"
        ],
        output_format="application/fhir+ndjson",
        since=datetime(2020, 1, 1, tzinfo=timezone.utc),
        types=["Observation", "MedicationRequest"],
        elements=["id", "status", "code"],
        include_associated_data=["LatestProvenanceResources"],
        type_filters=["Observation?category=vital-signs"],
        output_extension="ndjson",
        timeout=2400,
        max_concurrent_downloads=3
    )
    print("Patient export completed successfully")

    print("\nAll bulk exports completed successfully!")
    print(f"Output written to: {output_base}")


if __name__ == "__main__":
    test_bulk_exports()
