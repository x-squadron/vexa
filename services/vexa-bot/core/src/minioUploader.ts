/**
 * MinIO multipart upload for streaming audio to S3-compatible storage.
 * Uses same env vars as backend: APP_MINIO_ENDPOINT, APP_MINIO_BUCKET, APP_MINIO_ACCESS_KEY, APP_MINIO_SECRET_KEY.
 * S3 minimum part size is 5 MB (except last part).
 */

import { log } from "./utils";
import * as fs from "fs";
import * as fsp from "fs/promises";

const MIN_PART_SIZE = 5 * 1024 * 1024; // 5 MB

let minioClient: any = null;
let bucket: string = "";
let objectKey: string = "";
let uploadId: string = "";
let parts: { part: number; etag: string }[] = [];
let partNumber: number = 1;
let buffer: Buffer = Buffer.alloc(0);
let uploadInitialized = false;
let resolveFinalized: ((key: string | null) => void) | null = null;
let finalizedPromise: Promise<string | null> | null = null;
let uploadInProgress: Promise<void> = Promise.resolve();
let completedAudioObjectKey: string | null = null;
let completedVideoObjectKey: string | null = null;

function getClient(): any {
  if (minioClient) return minioClient;
  const endpoint = process.env.APP_MINIO_ENDPOINT;
  const accessKey = process.env.APP_MINIO_ACCESS_KEY;
  const secretKey = process.env.APP_MINIO_SECRET_KEY;
  if (!endpoint || !accessKey || !secretKey) return null;
  try {
    const Minio = require("minio");
    const url = new URL(endpoint.startsWith("http") ? endpoint : `http://${endpoint}`);
    const secure = url.protocol === "https:";
    const host = url.hostname;
    const port = url.port ? parseInt(url.port, 10) : secure ? 443 : 9000;
    minioClient = new Minio.Client({
      endPoint: host,
      port,
      useSSL: secure,
      accessKey,
      secretKey,
    });
    return minioClient;
  } catch (e: any) {
    log(`[MinIO] Client init failed: ${e?.message}`);
    return null;
  }
}

export function isMinioConfigured(): boolean {
  return !!(
    process.env.APP_MINIO_ENDPOINT &&
    process.env.APP_MINIO_BUCKET &&
    process.env.APP_MINIO_ACCESS_KEY &&
    process.env.APP_MINIO_SECRET_KEY
  );
}

export function getMinioUploadInitialized(): boolean {
  return uploadInitialized;
}

export function getAudioObjectKey(): string | null {
  return completedAudioObjectKey || objectKey || null;
}

export function getVideoObjectKey(): string | null {
  return completedVideoObjectKey || null;
}

function getRecordingTokenFromAudioKey(audioKey: string | null): string | null {
  if (!audioKey) return null;
  const m = audioKey.match(/\/(.+)_audio\.webm$/);
  return m?.[1] ?? null;
}

export function getRecordingToken(): string | null {
  return getRecordingTokenFromAudioKey(getAudioObjectKey());
}

export function getVideoObjectKeyFromAudioKey(audioKey: string | null): string | null {
  if (!audioKey) return null;
  return audioKey.replace(/_audio\.webm$/, "_video.webm");
}

/**
 * Start a multipart upload. Object key: {org_id}/{meeting_id}/recordings/{date-time}_audio.webm
 */
export async function initMinioUpload(
  orgId: string | null | undefined,
  meetingId: number | string | null | undefined,
  connectionId: string
): Promise<boolean> {
  const client = getClient();
  if (!client) return false;
  bucket = process.env.APP_MINIO_BUCKET || "faktions";
  const dateTime = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const oid = orgId || connectionId;
  const mid = meetingId != null ? String(meetingId) : connectionId;
  objectKey = `${oid}/${mid}/recordings/${dateTime}_audio.webm`;
  try {
    const meta: Record<string, string> = { "Content-Type": "audio/webm" };
    const initMultipart = client.initiateNewMultipartUpload ?? client.initiateMultipartUpload;
    uploadId = await initMultipart.call(client, bucket, objectKey, meta);
    parts = [];
    partNumber = 1;
    buffer = Buffer.alloc(0);
    uploadInitialized = true;
    completedAudioObjectKey = null;
    completedVideoObjectKey = null;
    finalizedPromise = new Promise<string | null>((resolve) => {
      resolveFinalized = resolve;
    });
    log(`[MinIO] Multipart upload started: ${objectKey}`);
    return true;
  } catch (e: any) {
    log(`[MinIO] Init multipart failed: ${e?.message}`);
    return false;
  }
}

async function flushBuffer(): Promise<void> {
  if (buffer.length === 0) return;
  const client = getClient();
  if (!client || !uploadInitialized) return;
  const part = Buffer.from(buffer);
  buffer = Buffer.alloc(0);
  try {
    const result = await client.uploadPart(
      {
        bucketName: bucket,
        objectName: objectKey,
        uploadID: uploadId,
        partNumber,
        headers: {},
      },
      part,
    );
    const etag =
      typeof result === "object" && result?.etag != null
        ? String(result.etag)
        : String(result);
    parts.push({ part: partNumber, etag });
    partNumber += 1;
  } catch (e: any) {
    log(`[MinIO] Upload part failed: ${e?.message}`);
  }
}

/**
 * Append chunk (call from onAudioChunk). Buffers and uploads parts >= 5 MB in order.
 * Returns a Promise so the caller can await to keep parts in order.
 */
export async function minioOnChunk(data: Buffer): Promise<void> {
  if (!uploadInitialized) return;
  buffer = Buffer.concat([buffer, data]);
  while (buffer.length >= MIN_PART_SIZE) {
    const part = buffer.subarray(0, MIN_PART_SIZE);
    buffer = buffer.subarray(MIN_PART_SIZE);
    const client = getClient();
    if (!client) return;
    uploadInProgress = uploadInProgress.then(async () => {
      try {
        const result = await client.uploadPart(
          {
            bucketName: bucket,
            objectName: objectKey,
            uploadID: uploadId,
            partNumber,
            headers: {},
          },
          part,
        );
        const etag =
          typeof result === "object" && result?.etag != null
            ? String(result.etag)
            : String(result);
        parts.push({ part: partNumber, etag });
        partNumber += 1;
      } catch (e: any) {
        log(`[MinIO] Upload part failed: ${e?.message}`);
      }
    });
    await uploadInProgress;
  }
}

/**
 * Finalize upload (upload remaining buffer, complete multipart). Call before sending exit callback.
 * Returns the object key or null. Resolves the promise used by performGracefulLeave to wait for key.
 */
export async function finalizeMinioUpload(): Promise<string | null> {
  if (!uploadInitialized || !resolveFinalized) {
    resolveFinalized?.(null);
    return null;
  }
  const client = getClient();
  if (!client) {
    resolveFinalized(null);
    return null;
  }
  try {
    await uploadInProgress;
    await flushBuffer();
    if (parts.length === 0) {
      await client.abortMultipartUpload(bucket, objectKey, uploadId);
      log(`[MinIO] Aborted upload (no parts): ${objectKey}`);
      if (resolveFinalized) resolveFinalized(null);
      return null;
    }
    const sortedParts = [...parts].sort((a, b) => a.part - b.part);
    await client.completeMultipartUpload(bucket, objectKey, uploadId, sortedParts);
    log(`[MinIO] Completed upload: ${objectKey}`);
    const resolve = resolveFinalized;
    uploadInitialized = false;
    completedAudioObjectKey = objectKey;
    resolveFinalized = null;
    if (resolve) resolve(objectKey);
    return objectKey;
  } catch (e: any) {
    log(`[MinIO] Finalize failed: ${e?.message}`);
    try {
      await client.abortMultipartUpload(bucket, objectKey, uploadId);
    } catch (_) {}
    if (resolveFinalized) resolveFinalized(null);
    return null;
  }
}

/**
 * Promise that resolves when finalizeMinioUpload has been called (e.g. from page close handler).
 */
export function getFinalizedPromise(): Promise<string | null> | null {
  return finalizedPromise || null;
}

export function resetFinalizedPromise(): void {
  finalizedPromise = null;
  resolveFinalized = null;
}

export async function uploadVideoFile(localPath: string): Promise<string | null> {
  const client = getClient();
  if (!client || !isMinioConfigured()) return null;

  const audioKey = getAudioObjectKey();
  const videoKey = getVideoObjectKeyFromAudioKey(audioKey);
  if (!videoKey) {
    log("[MinIO] Skipping video upload: audio object key is not available.");
    return null;
  }

  try {
    const st = await fsp.stat(localPath);
    if (!st.isFile() || st.size <= 0) {
      log(`[MinIO] Skipping video upload: missing/empty file at ${localPath}`);
      return null;
    }

    const stream = fs.createReadStream(localPath);
    await client.putObject(bucket || process.env.APP_MINIO_BUCKET || "faktions", videoKey, stream, st.size, {
      "Content-Type": "video/webm",
    });
    completedVideoObjectKey = videoKey;
    log(`[MinIO] Uploaded video object: ${videoKey}`);
    return videoKey;
  } catch (e: any) {
    log(`[MinIO] Video upload failed: ${e?.message}`);
    return null;
  }
}
