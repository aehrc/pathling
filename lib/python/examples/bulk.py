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
  "n": "jcrw7Jio4RVAMlo2clxqkmT9nmg_w1pXhpChg0jp41fKKfDXAtlIRhL_Ij8_N71l5KVxNQWeNeGsO0op73Rj28HR885fxJ2jimYFyD0fsftjjHvYkV_GskFubhcURbHAvx3lVrwLFyILq8sydF2G48A-XSfVAHPE6yEimusRRNihPmbM-MDlBuQkLBtwnT0bDXUEIlpDvlPB30Im2QOgvYTsMAnI-MzemOAtF5Xe5wCsj27nityK5AlnAJLFXfeeFqySoIyR7FaaQ1eay40MV-ZyDULSPtV4C-58eh3V2SL-qkQEsfQSuu3rqb-lgOz1-gl4FqTIz2JGtpEsTM7Uww",
  "e": "AQAB",
  "d": "kwNFEgpaxeAeHTtrypSZoXjLM7u-YM3czV9w8huCrjSg1SSXgFykAJX6zT40BHJbMv8xhcgEQZBMub69vBqoAOirWPky5KiNMPG7VirlRDaGJSJDH-UJQzaUJCM3c-bYzQXpDE3rxBBkCXHJcJQabAkwDa8-4F26YFjWGqUMsFOE1sxTXPnJG8qBTYxTSFxnWNf6U_kbGOQlWtHd1TgxPjXzmU6H472igte6SZEATh9eyYgPJrAqnw4qRNGy5pnAHkuIrCHIMaktR34LKFHHl3_xsLSHo9QmPfEdR5soKIKQIph2KRYArx4U03larr7vbZMSOypLqBtoRVlzvx0h",
  "p": "wmc-aV7SbViyP50B0s_6wrDlOjGid9kO7QePuohFLmJWuC8TP8VYeSBscCPf6gX40O8agiCrsBuz2ZUTZDlYBXPHRiYprdV11SgCXkfTw6-G5CD2Xjq43gcTzFOy2q2FlU5YtBkPVTrsYMH8p6F09sZRu-4rnCOpgoahbAawGXs",
  "q": "urhDpiHoZj0SBjmfn8GTHNh3FoUE8xiG3s0e64xSIBE3PzXCmskZpJuKGqgPX-wSXer2_WtmJUzOCucajcd4HQp222PWMKhc1HVZMj4073XQKDGqe_M7ZH29RbS9x93zhNgvFFiSdubQTg9SHJXL3Ja0f3IYxReha13G9YDSG1k",
  "dp": "thWN15QA9HpHOl4M_y_eZ8zYZ5Fl42tjF5Alh0lrwu5I22r8VJa7L3i3GLIBYGkHjGroIUoIhYLtCbcf2pf7Yd_3njTQhQmSvHwk-7m7F2aoqbRWDhxiW1O1r4QV2cz9ecNQQh_WxLXUASyxQTFxJFLM64FBR5X_h0oil9QLzVE",
  "dq": "W_t8L_JSR1Ncdr6aWRwGOdaVS_25g3wYrNeFnOoiZvO0MKpuNMxOmp2Y-irCcDGelq-yfwMSbduZQRu6JBAYps3J4agcExpNqMgqaarlbvWt1q8o2ijnoEilHhq8xyIa3d2Vy8MaXAK2qU242KYeqIuBXas6cpWCip7G7ZhJaPk",
  "qi": "bZvyduEpMUYyGXhd-MnHyKOiJtCUF_kbM0hUGr8AfJ6_bi8MEjBNMt5qZKGYYT6bXFJWiTTUFq6nZLmQJ7cY5lv57gAQOTlLy6hp-nqkNrH1P-5UAzEbUMhIdnPQcDEkBEjpfObHlwtrmaFFVKgpm5vqFFD-szMHPuZ43o0vS98",
  "key_ops": [
    "sign"
  ],
  "ext": true,
  "kid": "b31ab1cd8db2c39287b3267a2914600c"
}
"""

client_id = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InJlZ2lzdHJhdGlvbi10b2tlbiJ9.eyJqd2tzIjp7ImtleXMiOlt7Imt0eSI6IlJTQSIsImFsZyI6IlJTMzg0IiwibiI6Impjcnc3SmlvNFJWQU1sbzJjbHhxa21UOW5tZ193MXBYaHBDaGcwanA0MWZLS2ZEWEF0bElSaExfSWo4X043MWw1S1Z4TlFXZU5lR3NPMG9wNzNSajI4SFI4ODVmeEoyamltWUZ5RDBmc2Z0ampIdllrVl9Hc2tGdWJoY1VSYkhBdngzbFZyd0xGeUlMcThzeWRGMkc0OEEtWFNmVkFIUEU2eUVpbXVzUlJOaWhQbWJNLU1EbEJ1UWtMQnR3blQwYkRYVUVJbHBEdmxQQjMwSW0yUU9ndllUc01BbkktTXplbU9BdEY1WGU1d0NzajI3bml0eUs1QWxuQUpMRlhmZWVGcXlTb0l5UjdGYWFRMWVheTQwTVYtWnlEVUxTUHRWNEMtNThlaDNWMlNMLXFrUUVzZlFTdXUzcnFiLWxnT3oxLWdsNEZxVEl6MkpHdHBFc1RNN1V3dyIsImUiOiJBUUFCIiwia2V5X29wcyI6WyJ2ZXJpZnkiXSwiZXh0Ijp0cnVlLCJraWQiOiJiMzFhYjFjZDhkYjJjMzkyODdiMzI2N2EyOTE0NjAwYyJ9LHsia3R5IjoiUlNBIiwiYWxnIjoiUlMzODQiLCJuIjoiamNydzdKaW80UlZBTWxvMmNseHFrbVQ5bm1nX3cxcFhocENoZzBqcDQxZktLZkRYQXRsSVJoTF9JajhfTjcxbDVLVnhOUVdlTmVHc08wb3A3M1JqMjhIUjg4NWZ4SjJqaW1ZRnlEMGZzZnRqakh2WWtWX0dza0Z1YmhjVVJiSEF2eDNsVnJ3TEZ5SUxxOHN5ZEYyRzQ4QS1YU2ZWQUhQRTZ5RWltdXNSUk5paFBtYk0tTURsQnVRa0xCdHduVDBiRFhVRUlscER2bFBCMzBJbTJRT2d2WVRzTUFuSS1NemVtT0F0RjVYZTV3Q3NqMjduaXR5SzVBbG5BSkxGWGZlZUZxeVNvSXlSN0ZhYVExZWF5NDBNVi1aeURVTFNQdFY0Qy01OGVoM1YyU0wtcWtRRXNmUVN1dTNycWItbGdPejEtZ2w0RnFUSXoySkd0cEVzVE03VXd3IiwiZSI6IkFRQUIiLCJkIjoia3dORkVncGF4ZUFlSFR0cnlwU1pvWGpMTTd1LVlNM2N6Vjl3OGh1Q3JqU2cxU1NYZ0Z5a0FKWDZ6VDQwQkhKYk12OHhoY2dFUVpCTXViNjl2QnFvQU9pcldQa3k1S2lOTVBHN1ZpcmxSRGFHSlNKREgtVUpRemFVSkNNM2MtYll6UVhwREUzcnhCQmtDWEhKY0pRYWJBa3dEYTgtNEYyNllGaldHcVVNc0ZPRTFzeFRYUG5KRzhxQlRZeFRTRnhuV05mNlVfa2JHT1FsV3RIZDFUZ3hQalh6bVU2SDQ3MmlndGU2U1pFQVRoOWV5WWdQSnJBcW53NHFSTkd5NXBuQUhrdUlyQ0hJTWFrdFIzNExLRkhIbDNfeHNMU0hvOVFtUGZFZFI1c29LSUtRSXBoMktSWUFyeDRVMDNsYXJyN3ZiWk1TT3lwTHFCdG9SVmx6dngwaCIsInAiOiJ3bWMtYVY3U2JWaXlQNTBCMHNfNndyRGxPakdpZDlrTzdRZVB1b2hGTG1KV3VDOFRQOFZZZVNCc2NDUGY2Z1g0ME84YWdpQ3JzQnV6MlpVVFpEbFlCWFBIUmlZcHJkVjExU2dDWGtmVHc2LUc1Q0QyWGpxNDNnY1R6Rk95MnEyRmxVNVl0QmtQVlRyc1lNSDhwNkYwOXNaUnUtNHJuQ09wZ29haGJBYXdHWHMiLCJxIjoidXJoRHBpSG9aajBTQmptZm44R1RITmgzRm9VRTh4aUczczBlNjR4U0lCRTNQelhDbXNrWnBKdUtHcWdQWC13U1hlcjJfV3RtSlV6T0N1Y2FqY2Q0SFFwMjIyUFdNS2hjMUhWWk1qNDA3M1hRS0RHcWVfTTdaSDI5UmJTOXg5M3poTmd2RkZpU2R1YlFUZzlTSEpYTDNKYTBmM0lZeFJlaGExM0c5WURTRzFrIiwiZHAiOiJ0aFdOMTVRQTlIcEhPbDRNX3lfZVo4ellaNUZsNDJ0akY1QWxoMGxyd3U1STIycjhWSmE3TDNpM0dMSUJZR2tIakdyb0lVb0loWUx0Q2JjZjJwZjdZZF8zbmpUUWhRbVN2SHdrLTdtN0YyYW9xYlJXRGh4aVcxTzFyNFFWMmN6OWVjTlFRaF9XeExYVUFTeXhRVEZ4SkZMTTY0RkJSNVhfaDBvaWw5UUx6VkUiLCJkcSI6IldfdDhMX0pTUjFOY2RyNmFXUndHT2RhVlNfMjVnM3dZck5lRm5Pb2ladk8wTUtwdU5NeE9tcDJZLWlyQ2NER2VscS15ZndNU2JkdVpRUnU2SkJBWXBzM0o0YWdjRXhwTnFNZ3FhYXJsYnZXdDFxOG8yaWpub0VpbEhocTh4eUlhM2QyVnk4TWFYQUsycVUyNDJLWWVxSXVCWGFzNmNwV0NpcDdHN1poSmFQayIsInFpIjoiYlp2eWR1RXBNVVl5R1hoZC1Nbkh5S09pSnRDVUZfa2JNMGhVR3I4QWZKNl9iaThNRWpCTk10NXFaS0dZWVQ2YlhGSldpVFRVRnE2blpMbVFKN2NZNWx2NTdnQVFPVGxMeTZocC1ucWtOckgxUC01VUF6RWJVTWhJZG5QUWNERWtCRWpwZk9iSGx3dHJtYUZGVktncG01dnFGRkQtc3pNSFB1WjQzbzB2Uzk4Iiwia2V5X29wcyI6WyJzaWduIl0sImV4dCI6dHJ1ZSwia2lkIjoiYjMxYWIxY2Q4ZGIyYzM5Mjg3YjMyNjdhMjkxNDYwMGMifV19LCJhY2Nlc3NUb2tlbnNFeHBpcmVJbiI6MTUsImlhdCI6MTc0MDMwNTQ3OH0.qI-820847HN1S37IGMVMKJRGeXQBrgbx91UZ7Av9djs"


def test_bulk_exports():
    # Initialize PathlingContext.
    pc = PathlingContext.create()

    # Base parameters from the demo server
    fhir_server = "https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwidGx0IjoxNSwibSI6MSwiZGVsIjowLCJzZWN1cmUiOjEsIm9wcCI6MTB9/fhir"
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
        max_concurrent_downloads=5,
        auth_config={
            "enabled": True,
            "client_id": client_id,
            "private_key_jwk": jwk,
            "token_endpoint": "https://bulk-data.smarthealthit.org/auth/token",
            "use_smart": True,
            "use_form_for_basic_auth": False,
            "scope": "system/Patient.r system/Observation.r",
            "token_expiry_tolerance": 120
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
