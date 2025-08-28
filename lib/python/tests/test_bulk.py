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
#
import os

from flask import Response

from pathling.bulk import BulkExportClient


def test_bulk_client(pathling_ctx, mock_server, temp_dir):
    @mock_server.route("/fhir/$export", methods=["GET"])
    def export():
        resp = Response(status=202)
        resp.headers["content-location"] = mock_server.url("/pool")
        return resp

    @mock_server.route("/pool", methods=["GET"])
    def pool():
        return dict(
            transactionTime="1970-01-01T01:02:03.004Z",
            output=[
                dict(type="Patient", url=mock_server.url("/download"), count=1),
            ],
        )

    @mock_server.route("/download", methods=["GET"])
    def download():
        return '{"id":"123"}'

    output_dir = os.path.join(temp_dir, "export-output")

    with mock_server.run():
        result = BulkExportClient.for_system(
            pathling_ctx.spark,
            fhir_endpoint_url=mock_server.url("/fhir"),
            output_dir=output_dir
        ).export()

        assert os.path.isdir(output_dir)
        assert os.path.exists(os.path.join(output_dir, "_SUCCESS"))
        assert os.path.exists(os.path.join(output_dir, "Patient.0000.ndjson"))
        with open(os.path.join(output_dir, "Patient.0000.ndjson")) as f:
            assert f.read() == '{"id":"123"}'
        assert result.transaction_time.isoformat() == "1970-01-01T01:02:03.004000+00:00"
        assert 1 == len(result.results)
        file_result = result.results[0]
        assert 12 == file_result.size
        assert os.path.join(output_dir, "Patient.0000.ndjson") == file_result.destination
        assert mock_server.url("/download") == file_result.source
