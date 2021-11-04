/*
 * Copyright © 2021-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. All rights reserved.
 */

/**
 * @author John Grimes
 */

import { IParameters } from "@ahryman40k/ts-fhir-types/lib/R4";
import { FhirBulkOutput } from "./export";
import { createHash } from "crypto";
import { buildAuthenticatedClient, FHIR_NDJSON_CONTENT_TYPE } from "./common";
import { AxiosInstance, AxiosResponse } from "axios";
import {
  AbortMultipartUploadCommand,
  CompletedPart,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  HeadObjectCommand,
  HeadObjectCommandOutput,
  S3Client,
  UploadPartCommand
} from "@aws-sdk/client-s3";
import { rm, stat } from "fs/promises";
import { v4 as uuidv4 } from "uuid";
import { createReadStream, createWriteStream } from "fs";
import { URL } from "url";
import tempDirectory = require("temp-dir");
import ReadableStream = NodeJS.ReadableStream;

const path = require("path");

export interface TransferParams {
  endpoint: string;
  clientId: string;
  clientSecret: string;
  scopes: string;
  result: FhirBulkResult;
  stagingUrl: string;
}

export type TransferResult = IParameters;

export interface FhirBulkResult {
  output: FhirBulkOutput[];
}

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
 * @return a FHIR {@link IParameters} resource suitable for input to the Pathling import operation
 */
export async function transferExportToS3({
  endpoint,
  clientId,
  clientSecret,
  scopes,
  result,
  stagingUrl,
}: TransferParams): Promise<TransferResult> {
  const client = await buildAuthenticatedClient(
    endpoint,
    clientId,
    clientSecret,
    scopes
  );
  const s3Client = new S3Client({});

  const parsedStagingUrl = new URL(stagingUrl);
  const stagingPath = parsedStagingUrl.pathname.replace("/", "");
  const bucket = parsedStagingUrl.hostname;

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
  return s3ResultLocationsToParameters(s3ResultLocations);
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
  if (urls.length === 0) throw "URLs must not be empty";

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
    throw "Problem creating temp file";
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
      const response = await client.get<undefined, AxiosResponse<ReadableStream>>(url, {
        headers: {
          Accept: FHIR_NDJSON_CONTENT_TYPE,
        },
        responseType: "stream",
      });
      response.data.pipe(writeStream);

      const completedFile = await new Promise<string | null>((resolve) => {
        response.data.on("end", async () => {
          // Check the size of the current temporary file.
          const stats = await stat(currentFile);
          if (stats.size > MINIMUM_PART_SIZE) {
            downloadedFiles.push(currentFile);
            resolve(currentFile);
            currentFile = tempFile();
          }
          resolve(null);
        });
      });
      if (completedFile) {
        yield completedFile;
      }
    }

    // In the case of a single temporary file, we need to make sure it becomes part of the list. It is
    // ok for this file to be less than the minimum part size.
    const stats = await stat(currentFile);
    if (stats.size > 0) {
      yield currentFile;
    }
    return downloadedFiles;
  } catch (e) {
    // Clean up temporary files.
    console.info(
      "[%s] Cleaning up temporary files: %j",
      resourceType,
      downloadedFiles
    );
    await Promise.all(downloadedFiles.map((file) => rm(file)));
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
    await rm(downloadedFile);
  }
}

function s3ResultLocationsToParameters(
  locations: S3ResultLocation[]
): IParameters {
  return {
    resourceType: "Parameters",
    parameter: locations
      .filter((location) => location.changed)
      .map((location) => ({
        name: "source",
        part: [
          { name: "resourceType", valueCode: location.resourceType },
          { name: "url", valueUrl: location.url },
        ],
      })),
  };
}

function urlFromBucketAndKey(bucket: string, key: string) {
  return `s3://${bucket}/${key}`;
}

function tempFile() {
  if (!path.join) {
    throw "path.join not available";
  }
  return path.join(tempDirectory, uuidv4());
}
