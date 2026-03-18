import {
  BotCallbacks,
  TranscriptionSegment,
} from "../../gateways/BotCallbacks";
import { log } from "../../utils";
import fs from "fs/promises";
import * as minioUploader from "../../minioUploader";

export class VexaBotCallbacks implements BotCallbacks {
  transcriptionSegments: TranscriptionSegment[] = [];
  private videoFilePath: string | null = null;

  onStartRecording = async (videoFilePath: string, botConnectionId: string) => {
    log(
      `[VexaBotCallbacks] onStartRecording - ${videoFilePath} - ${botConnectionId}`
    );
    this.videoFilePath = await saveVideoAs(videoFilePath, botConnectionId);
    return;
  };

  onTranscriptionSegmentsReceived = (
    data: TranscriptionSegment[] | TranscriptionSegment
  ) => {
    log(
      `[VexaBotCallbacks] onTranscriptionSegmentsReceived - ${JSON.stringify(data)}`
    );
    if (Array.isArray(data)) {
      log(`Adding ${data.length} segments to SRT collection`);
      this.transcriptionSegments.push(...data);
    } else if (
      data.start !== undefined &&
      data.end !== undefined &&
      data.text
    ) {
      // Handle single segment format
      log("Adding single segment to SRT collection");
      this.transcriptionSegments.push(data);
    }
  };

  onMeetingEnd = async (connectionId: string) => {
    log(`[VexaBotCallbacks] Meeting ended - ${connectionId}`);

    const transcriptionSegments = this.transcriptionSegments;
    log(
      `[VexaBotCallbacks] Writing SRT file with ${transcriptionSegments.length} segments`
    );

    if (transcriptionSegments.length > 0) {
      try {
        // Send SRT data to Node.js context for file writing
        log("Sending SRT data to Node.js for file writing");

        log(
          `Received ${transcriptionSegments.length} transcription segments for SRT file`
        );
        await writeSRTFile(transcriptionSegments, connectionId);
      } catch (error: any) {
        log(`Error sending SRT data: ${error.message}`);
      }
    } else {
      log("No transcription segments to write to SRT");
    }

    if (this.videoFilePath && minioUploader.isMinioConfigured()) {
      const videoKey = await minioUploader.uploadVideoFile(this.videoFilePath);
      if (videoKey) {
        log(`[VexaBotCallbacks] Video uploaded to MinIO: ${videoKey}`);
      } else {
        log("[VexaBotCallbacks] Video upload to MinIO was skipped/failed.");
      }
    }
  };
}

function formatSRTTime(seconds: number): string {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  const ms = Math.floor((seconds % 1) * 1000);

  return `${hours.toString().padStart(2, "0")}:${minutes
    .toString()
    .padStart(2, "0")}:${secs.toString().padStart(2, "0")},${ms
    .toString()
    .padStart(3, "0")}`;
}

async function writeSRTFile(
  segments: any[],
  connectionId: string
): Promise<void> {
  try {
    let srtContent = "";
    segments.forEach((segment, index) => {
      const segNum = index + 1;
      const startTime = formatSRTTime(segment.start);
      const endTime = formatSRTTime(segment.end);
      srtContent += `${segNum}\n${startTime} --> ${endTime}\n${segment.text}\n\n`;
    });

    const srtFilePath = `/app/recordings/transcript_${connectionId}.srt`;
    await fs.writeFile(srtFilePath, srtContent, "utf-8");
    log(`SRT file saved: ${srtFilePath}`);
  } catch (error: any) {
    log(`Error writing SRT file: ${error.message}`);
  }
}

async function saveVideoAs(videoFilePath: string, botConnectionId: string) {
  const videoDir = "/app/recordings";
  const recordingToken = minioUploader.getRecordingToken() || botConnectionId;
  const newVideoPath = `${videoDir}/video_${recordingToken}.webm`;
  try {
    await fs.rename(videoFilePath, newVideoPath);
  } catch (e) {
    console.error(e);
    console.log(`"${videoFilePath}" -> "${newVideoPath}" rename failed`);
  }
  return newVideoPath;
}
