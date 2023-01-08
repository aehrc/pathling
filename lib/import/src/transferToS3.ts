/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @author John Grimes
 */

import {
  AbortMultipartUploadCommand,
  CompletedPart,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  HeadObjectCommand,
  HeadObjectCommandOutput,
  S3Client,
  UploadPartCommand,
} from "@aws-sdk/client-s3";
import { AxiosInstance, AxiosResponse } from "axios";
import { createHash } from "crypto";
import { Parameters } from "fhir/r4";
import { createReadStream, createWriteStream, Stats } from "fs";
import { rm, stat } from "fs/promises";
import path from "node:path";
import tempDirectory from "temp-dir";
import { URL } from "url";
import { v4 as uuidv4 } from "uuid";
import {
  buildClient,
  FHIR_NDJSON_CONTENT_TYPE,
  MaybeAuthenticated,
} from "./common.js";
import { FhirBulkResult } from "./export";
import ReadableStream = NodeJS.ReadableStream;

export type TransferParams = {
  endpoint: string;
  result: FhirBulkResult;
  stagingUrl: string;
  importMode?: string;
} & MaybeAuthenticated;

export type TransferResult = Parameters;

export type ImportMode = "overwrite" | "merge";

interface S3UploadJob {
  urls: string[];
  key: string;
}

export interface S3ResultLocation {
  resourceType: string;
  url: string;
  changed: boolean;
}

const MINIMUM_PART_SIZE = 5242880;

/**
 * Downloads the files within a FHIR bulk export and transfers them to S3.
 *
 * @return a FHIR {@link Parameters} resource suitable for input to the Pathling import operation
 */
export async function transferExportToS3(
  options: TransferParams
): Promise<TransferResult> {
  const { stagingUrl, result, importMode = "merge" } = options,
    client = await buildClient(options),
    s3Client = new S3Client({});

  const parsedStagingUrl = new URL(stagingUrl),
    stagingPath = parsedStagingUrl.pathname.replace("/", ""),
    bucket = parsedStagingUrl.hostname;

  const categorisedOutputs: { [resourceType: string]: S3UploadJob } =
    result.output.reduce(
      (prev: { [resourceType: string]: S3UploadJob }, output) => {
        // This assumes that the download URL provided by the source FHIR server is unique based
        // upon the contents of the file.
        const hash = createHash("sha256")
          .update(output.url)
          .digest("hex")
          .slice(0, 7);
        const key = `${stagingPath}/${output.type}-${hash}.ndjson`;

        // Build up a map of resource types to the set of URLs which need to be downloaded and
        // streamed to the S3 key.
        if (!Object.keys(prev).includes(output.type)) {
          prev[output.type] = { urls: [], key };
        }
        prev[output.type].urls.push(output.url);
        return prev;
      },
      {}
    );

  // Wait for all the downloaded files to be checked and possibly transferred to S3.
  const s3ResultLocations = await Promise.all(
    Object.keys(categorisedOutputs).map((resourceType) => {
      const job = categorisedOutputs[resourceType];
      return conditionallyTransferToS3(
        resourceType,
        job.urls,
        bucket,
        job.key,
        client,
        s3Client
      );
    })
  );
  return s3ResultLocationsToParameters(
    s3ResultLocations,
    validateImportMode(importMode)
  );
}

async function conditionallyTransferToS3(
  resourceType: string,
  urls: string[],
  bucket: string,
  key: string,
  client: AxiosInstance,
  s3Client: S3Client
): Promise<S3ResultLocation> {
  const stagingUrl = urlFromBucketAndKey(bucket, key);

  try {
    // If the file already exists in S3, skip download and return the location.
    await checkFileExistsInS3(bucket, key, s3Client);
    console.info(
      "[%s] File already exists in S3, skipping download and transfer: %s",
      resourceType,
      stagingUrl
    );
    return {
      resourceType,
      url: stagingUrl,
      changed: false,
    };
  } catch (e) {
    // If the file does not exist, download all the source URLs and put their concatenated content
    // into the location in S3.
    console.info(
      "[%s] File does not exist in S3, downloading and transferring now: %s => %s",
      resourceType,
      urls,
      stagingUrl
    );
    return transferToS3(
      urls,
      client,
      s3Client,
      bucket,
      key,
      resourceType,
      stagingUrl
    );
  }
}

async function transferToS3(
  urls: string[],
  client: AxiosInstance,
  s3Client: S3Client,
  bucket: string,
  key: string,
  resourceType: string,
  stagingUrl: string
): Promise<S3ResultLocation> {
  if (urls.length === 0) throw new Error("URLs must not be empty");

  // We upload the concatenated data up to S3 using a multipart upload, which needs to be started
  // and ended using API requests.
  const createMultipartUpload = new CreateMultipartUploadCommand({
    Bucket: bucket,
    Key: key,
  });
  const createResult = await s3Client.send(createMultipartUpload);
  const uploadId = createResult.UploadId;

  try {
    // Files are downloaded from the FHIR server and stored in temporary files.
    const downloadedFiles = await downloadFiles(resourceType, urls, client);

    // The temporary files are uploaded as parts of the multipart upload to S3. Because
    // `downloadFiles` is a generator function, upload will be kicked off as soon as a file is
    // available.
    const uploadedParts: CompletedPart[] = [];
    let partNumber = 0;
    for await (const downloadedFile of downloadedFiles) {
      partNumber++;
      const uploadedPart = await uploadFile(
        downloadedFile,
        partNumber,
        bucket,
        key,
        uploadId,
        s3Client,
        resourceType
      );
      uploadedParts.push(uploadedPart);
    }

    // The final multipart API request needs to include a manifest of the files uploaded, along with
    // their ETags.
    const completeMultipartUpload = new CompleteMultipartUploadCommand({
      Bucket: bucket,
      Key: key,
      UploadId: uploadId,
      MultipartUpload: {
        Parts: uploadedParts,
      },
    });
    await s3Client.send(completeMultipartUpload);
  } catch (e) {
    // If there is an error, tell S3 to abort the multipart upload.
    console.warn(
      "[%s] Error detected, aborting multipart upload",
      resourceType
    );
    const abortMultipartUpload = new AbortMultipartUploadCommand({
      Bucket: bucket,
      Key: key,
      UploadId: uploadId,
    });
    await s3Client.send(abortMultipartUpload);
    throw e;
  }

  return {
    resourceType,
    url: stagingUrl,
    changed: true,
  };
}

async function checkFileExistsInS3(
  bucket: string,
  key: string,
  s3Client: S3Client
): Promise<HeadObjectCommandOutput> {
  // We use a head command to check for the existence of the file on S3.
  const head = new HeadObjectCommand({
    Bucket: bucket,
    Key: key,
  });
  return s3Client.send(head);
}

async function* downloadFiles(
  resourceType: string,
  urls: string[],
  client: AxiosInstance
) {
  let downloadedFiles: string[] = [],
    currentFile = tempFile();
  if (!currentFile) {
    throw new Error("Problem creating temp file");
  }

  try {
    // Files from the FHIR server are downloaded to temporary files, but care is taken to make sure
    // that each of the files is either the last file, or greater than the minimum part size for S3
    // (~5MB).
    for (let i = 0; i < urls.length; i++) {
      const url = urls[i];

      // Data is appended to the file, as a single temporary file could contain the data from many
      // source files from the FHIR server.
      const writeStream = createWriteStream(currentFile, { flags: "a" });
      console.info("[%s] Downloading %s => %s", resourceType, url, currentFile);
      const response = await client.get<
        undefined,
        AxiosResponse<ReadableStream>
      >(url, {
        headers: {
          Accept: FHIR_NDJSON_CONTENT_TYPE,
        },
        responseType: "stream",
      });
      response.data.pipe(writeStream);

      const completedFile = await new Promise<string | undefined>((resolve) => {
        response.data.on("end", async () => {
          // Check the size of the current temporary file.
          let stats: Stats;
          try {
            stats = await stat(currentFile);
            if (stats.size > MINIMUM_PART_SIZE) {
              downloadedFiles.push(currentFile);
              resolve(currentFile);
              currentFile = tempFile();
            }
            resolve(undefined);
          } catch (e) {
            // This can happen if nothing has been written to the file.
            resolve(undefined);
          }
        });
      });
      if (completedFile) {
        yield completedFile;
      }
    }

    // In the case of a single temporary file, we need to make sure it becomes part of the list. It is
    // ok for this file to be less than the minimum part size.
    let stats: Stats;
    try {
      stats = await stat(currentFile);
      if (stats.size > 0) {
        yield currentFile;
      }
    } catch (e) {
      // This can happen if nothing has been written to the file.
    }
    return downloadedFiles;
  } catch (e) {
    // Clean up temporary files.
    console.info(
      "[%s] Cleaning up temporary files: %j",
      resourceType,
      downloadedFiles
    );
    await Promise.all(downloadedFiles.map((file) => rm(file))).catch((e) =>
      console.warn("Problem deleting files: %j (%s)", downloadedFiles, e)
    );
    throw e;
  }
}

async function uploadFile(
  downloadedFile: string,
  partNumber: number,
  bucket: string,
  key: string,
  uploadId: string | undefined,
  s3Client: S3Client,
  resourceType: string
) {
  try {
    const readStream = createReadStream(downloadedFile);
    const uploadPart = new UploadPartCommand({
      Bucket: bucket,
      Key: key,
      PartNumber: partNumber,
      UploadId: uploadId,
      Body: readStream,
    });

    const uploadResponse = await s3Client.send(uploadPart);
    console.info(
      "[%s] Uploaded %s => %s",
      resourceType,
      downloadedFile,
      urlFromBucketAndKey(bucket, key)
    );

    return { PartNumber: partNumber, ETag: uploadResponse.ETag };
  } finally {
    // Clean up temporary file as soon as it is uploaded, or if there is an error.
    console.info(
      "[%s] Cleaning up temporary file: %s",
      resourceType,
      downloadedFile
    );
    await rm(downloadedFile).catch((e) =>
      console.warn("Problem deleting file: %s (%s)", downloadedFile, e)
    );
  }
}

function s3ResultLocationsToParameters(
  locations: S3ResultLocation[],
  importMode: ImportMode
): Parameters {
  return {
    resourceType: "Parameters",
    parameter: locations
      .filter((location) => location.changed)
      .map((location) => ({
        name: "source",
        part: [
          { name: "resourceType", valueCode: location.resourceType },
          { name: "url", valueUrl: location.url },
          { name: "mode", valueCode: importMode },
        ],
      })),
  };
}

function urlFromBucketAndKey(bucket: string, key: string) {
  return `s3://${bucket}/${key}`;
}

function tempFile() {
  if (!path.join) {
    throw new Error("path.join not available");
  }
  return path.join(tempDirectory, uuidv4());
}

function validateImportMode(value: string): ImportMode {
  if (value === "overwrite" || value === "merge") {
    return value;
  }
  throw `Invalid import mode: ${value}`;
}
