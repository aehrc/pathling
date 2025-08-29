#  Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

"""
Upload Pathling source code to CSIRO Data Access Portal (DAP).

This script downloads the source code archive from GitHub and uploads it to
CSIRO DAP using the presigned URL method.

Required environment variables:
- GITHUB_REPOSITORY: GitHub repository (e.g., "aehrc/pathling")
- GITHUB_REF_NAME: Git ref name (e.g., "v8.0.0")
- GITHUB_TOKEN: GitHub personal access token
- DAP_USERNAME: DAP username
- DAP_PASSWORD: DAP password
- DAP_BASE_URL: DAP base URL (default: "https://data.csiro.au")
- COLLECTION_PID: DAP collection PID (default: "csiro:49524")
"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request
import base64


def create_auth_header(username, password):
    """Create Basic authentication header."""
    credentials = f"{username}:{password}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Basic {encoded}"


def make_request(url, method="GET", data=None, headers=None, auth_header=None):
    """Make HTTP request with optional authentication."""
    if headers is None:
        headers = {}
    
    if auth_header:
        headers["Authorization"] = auth_header
    
    if data is not None:
        if isinstance(data, dict):
            data = json.dumps(data).encode()
            headers["Content-Type"] = "application/json"
        elif isinstance(data, str):
            data = data.encode()
            
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    
    try:
        with urllib.request.urlopen(req) as response:
            return response.read().decode(), response.getcode()
    except urllib.error.HTTPError as e:
        return e.read().decode(), e.code


def upload_file_with_presigned_url(file_path, presigned_url):
    """Upload file using presigned URL with PUT request."""
    try:
        with open(file_path, 'rb') as f:
            file_data = f.read()
        
        req = urllib.request.Request(presigned_url, data=file_data, method='PUT')
        req.add_header('Content-Type', 'application/zip')
        
        with urllib.request.urlopen(req) as response:
            return response.read().decode(), response.getcode()
    except urllib.error.HTTPError as e:
        return e.read().decode(), e.code


def main():
    """Main function to orchestrate the DAP upload process."""
    # Get configuration from environment variables
    GITHUB_REPOSITORY = os.environ.get("GITHUB_REPOSITORY")
    GITHUB_REF_NAME = os.environ.get("GITHUB_REF_NAME")
    GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
    DAP_USERNAME = os.environ.get("DAP_USERNAME")
    DAP_PASSWORD = os.environ.get("DAP_PASSWORD")
    DAP_BASE_URL = os.environ.get("DAP_BASE_URL", "https://data.csiro.au")
    COLLECTION_PID = os.environ.get("COLLECTION_PID", "csiro:49524")

    # Validate required environment variables
    required_vars = {
        "GITHUB_REPOSITORY": GITHUB_REPOSITORY,
        "GITHUB_REF_NAME": GITHUB_REF_NAME,
        "GITHUB_TOKEN": GITHUB_TOKEN,
        "DAP_USERNAME": DAP_USERNAME,
        "DAP_PASSWORD": DAP_PASSWORD,
    }
    
    missing_vars = [name for name, value in required_vars.items() if not value]
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    # Step 1: Download source code ZIP from release
    print("=== CSIRO DAP Upload ===")
    print(f"Repository: {GITHUB_REPOSITORY}")
    print(f"Release: {GITHUB_REF_NAME}")
    print(f"DAP Environment: {DAP_BASE_URL}")
    print(f"Collection PID: {COLLECTION_PID}")
    print()

    # Extract version from tag (remove 'v' prefix if present)
    version = GITHUB_REF_NAME
    if version.startswith('v'):
        version = version[1:]

    zip_filename = f"pathling-{version}.zip"

    print(f"Step 1: Downloading source code archive...")
    print(f"Target file: {zip_filename}")

    # Download the automatic source code archive from GitHub
    github_api_url = f"https://api.github.com/repos/{GITHUB_REPOSITORY}/zipball/{GITHUB_REF_NAME}"

    req = urllib.request.Request(github_api_url)
    req.add_header('Authorization', f'token {GITHUB_TOKEN}')
    req.add_header('Accept', 'application/vnd.github.v3+json')

    try:
        with urllib.request.urlopen(req) as response:
            if response.getcode() != 200:
                print(f"Error: Failed to download source archive (HTTP {response.getcode()})")
                sys.exit(1)
            
            with open(zip_filename, 'wb') as f:
                f.write(response.read())
    except Exception as e:
        print(f"Error: Failed to download source code archive: {e}")
        sys.exit(1)

    # Verify that the ZIP file was downloaded
    if not os.path.exists(zip_filename):
        print(f"Error: Failed to download source code archive for {GITHUB_REF_NAME}")
        sys.exit(1)

    file_size = os.path.getsize(zip_filename)
    print(f"Downloaded source code archive: {zip_filename}")
    print(f"File size: {file_size:,} bytes ({file_size / (1024*1024):.1f} MB)")
    print()

    # Step 2: Upload to CSIRO DAP
    auth_header = create_auth_header(DAP_USERNAME, DAP_PASSWORD)

    # Get collection information
    print(f"Step 2: Getting collection information for {COLLECTION_PID}...")
    response_text, status_code = make_request(
        f"{DAP_BASE_URL}/dap/api/v2/my-collections",
        auth_header=auth_header
    )

    if status_code != 200:
        print(f"Error: Failed to get collections (HTTP {status_code})")
        print(response_text[:500])
        sys.exit(1)

    collections = json.loads(response_text)

    # Find the collection with the specified PID
    collection_data = None
    for collection in collections:
        if collection.get("dataCollectionPid") == COLLECTION_PID:
            collection_data = collection
            break

    if not collection_data:
        print(f"Error: Collection {COLLECTION_PID} not found or not accessible")
        sys.exit(1)

    collection_id = collection_data["dataCollectionId"]
    collection_status = collection_data["status"]

    print(f"Found collection ID: {collection_id} (Status: {collection_status})")
    print()

    # Create a new version of the collection (or use existing draft)
    if collection_status == "Draft":
        print("Step 3: Using existing draft version...")
        new_collection_id = collection_id
    else:
        print("Step 3: Creating new version of collection...")
        response_text, status_code = make_request(
            f"{DAP_BASE_URL}/dap/api/v2/collections/{collection_id}/update",
            method="POST",
            auth_header=auth_header
        )
        
        if status_code != 201:
            print(f"Error: Failed to create new version (HTTP {status_code})")
            print(response_text[:500])
            sys.exit(1)
        
        update_response = json.loads(response_text)
        new_collection_id = update_response["dataCollectionId"]
        
        if not new_collection_id:
            print("Error: Failed to get new collection ID")
            print(response_text[:500])
            sys.exit(1)
        
        print(f"Created new collection version with ID: {new_collection_id}")
    print()

    # Unlock files for modification
    print("Step 4: Unlocking files for modification...")
    response_text, status_code = make_request(
        f"{DAP_BASE_URL}/dap/api/v2/collections/{new_collection_id}/files/unlock",
        method="POST",
        auth_header=auth_header
    )

    if status_code not in [200, 204]:
        print(f"Warning: Unlock request returned HTTP {status_code}")
        print(response_text[:200])

    # Wait for files to be unlocked
    print("Step 5: Waiting for files to be unlocked...")
    max_attempts = 24  # 2 minutes with 5-second intervals
    attempt = 0

    while attempt < max_attempts:
        time.sleep(5)
        response_text, status_code = make_request(
            f"{DAP_BASE_URL}/dap/api/v2/collections/{new_collection_id}/files/fileState",
            auth_header=auth_header
        )
        
        print(f"File state response: {response_text}")
        
        try:
            file_state_data = json.loads(response_text)
            file_state = file_state_data.get("value", response_text.strip())
        except json.JSONDecodeError:
            file_state = response_text.strip()
        
        if file_state == "unlocked":
            break
        elif file_state == "error":
            print("Error: Failed to unlock files")
            sys.exit(1)
        
        attempt += 1

    if attempt >= max_attempts:
        print("Error: Timeout waiting for files to unlock")
        sys.exit(1)

    print("Files unlocked successfully!")
    print()

    # Upload file using presigned URL method
    print("Step 6: Uploading file using presigned URL method...")

    # Get presigned URL (URL encode the path)
    file_path = f"/{zip_filename}"
    encoded_path = urllib.parse.quote(file_path)
    print(f"Getting presigned URL for path: {file_path}")

    response_text, status_code = make_request(
        f"{DAP_BASE_URL}/dap/api/v2/collections/{new_collection_id}/files/s3uploadurl?path={encoded_path}",
        auth_header=auth_header
    )

    # Check if we got a valid URL (not HTML error)
    if status_code != 200 or response_text.startswith("<!doctype") or response_text.startswith("<html"):
        print("Error: Failed to get presigned URL. Response:")
        print(response_text[:500])
        sys.exit(1)

    presigned_url = response_text.strip()
    print(f"Presigned URL obtained (length: {len(presigned_url)} characters)")
    print(f"URL preview: {presigned_url[:100]}...")
    print()

    # Upload file using the presigned URL
    print("Uploading file using presigned URL...")
    upload_response, upload_status = upload_file_with_presigned_url(zip_filename, presigned_url)

    print(f"HTTP Response Code: {upload_status}")
    if upload_status == 200:
        print("✓ File uploaded successfully!")
    else:
        print("✗ Upload failed. Response:")
        print(upload_response[:500])
        sys.exit(1)
    print()

    # Validate the collection
    print("Step 7: Validating collection...")
    response_text, status_code = make_request(
        f"{DAP_BASE_URL}/dap/api/v2/collections/{new_collection_id}/validate",
        auth_header=auth_header
    )

    print(f"Validation response: {response_text}")

    try:
        validation_data = json.loads(response_text)
        validation_errors = validation_data.get("errors", [])
        if validation_errors and validation_errors != []:
            print(f"Warning: Validation errors found: {validation_errors}")
        else:
            print("✓ Collection validation passed!")
    except json.JSONDecodeError:
        print(f"Note: Could not parse validation response as JSON: {response_text}")
    print()

    # Submit collection for publication
    print("Step 8: Submitting collection for publication...")
    submit_payload = {
        "approver": None,
        "businessUnit": None,
        "notesToApprover": None,
        "supportingFilesForApprover": []
    }

    response_text, status_code = make_request(
        f"{DAP_BASE_URL}/dap/api/v2/collections/{new_collection_id}/submit",
        method="POST",
        data=submit_payload,
        auth_header=auth_header
    )

    print(f"Submit response (HTTP {status_code}): {response_text}")
    if status_code in [200, 201, 204]:
        print("✓ Collection submitted for publication!")
    else:
        print(f"Warning: Submit returned HTTP {status_code}")
    print()

    print("=== Upload Complete ===")
    print(f"New collection ID: {new_collection_id}")
    print(f"Collection URL: {DAP_BASE_URL}/dap/collections/{new_collection_id}")
    print()
    print("The collection should now be visible in the DAP environment.")


if __name__ == "__main__":
    main()
