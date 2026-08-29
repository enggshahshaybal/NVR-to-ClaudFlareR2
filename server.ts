import express from "express";
import dotenv from "dotenv";
import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { randomUUID } from "crypto";
import { Readable } from "stream";

import DigestFetch from "digest-fetch";
import { XMLParser } from "fast-xml-parser";

dotenv.config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

// ===========================
// Cloudflare R2
// ===========================

const r2 = new S3Client({
  region: "auto",
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY!,
    secretAccessKey: process.env.R2_SECRET_KEY!,
  },
});

// ===========================
// Hikvision NVR
// ===========================

const nvr = new DigestFetch(
  process.env.NVR_USERNAME!,
  process.env.NVR_PASSWORD!
);

const parser = new XMLParser();

const NVR_BASE = `http://${process.env.NVR_IP}`;

// ===========================
// Helpers
// ===========================

function escapeXML(str: string) {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

// 2026-07-14T10:23:45.123Z  ->  2026-07-14T10:23:45Z
function toISAPITime(date: Date) {
  return date.toISOString().split(".")[0] + "Z";
}

function buildSearchXML(
  trackID: number,
  startTime: string,
  endTime: string,
  maxResults = 40,
  position = 0
) {
  return `<?xml version="1.0" encoding="UTF-8"?>
<CMSearchDescription>
  <searchID>${randomUUID()}</searchID>
  <trackIDList>
    <trackID>${trackID}</trackID>
  </trackIDList>
  <timeSpanList>
    <timeSpan>
      <startTime>${startTime}</startTime>
      <endTime>${endTime}</endTime>
    </timeSpan>
  </timeSpanList>
  <maxResults>${maxResults}</maxResults>
  <searchResultPostion>${position}</searchResultPostion>
  <metadataList>
    <metadataDescriptor>//recordType.meta.std-cgi.com</metadataDescriptor>
  </metadataList>
</CMSearchDescription>`;
}

// ===========================
// 1) Recording search — to find playbackURI 
// GET /recordings?trackID=101&startTime=2026-03-01T00:00:00Z&endTime=2026-03-02T00:00:00Z
// ===========================

app.get("/recordings", async (req, res) => {
  try {
    const trackID = Number(req.query.trackID) || 101; // ch1 mainstream=101, substream=102, ch2=201...

    const startTime =
      (req.query.startTime as string) ||
      toISAPITime(new Date(Date.now() - 24 * 3600 * 1000));

    const endTime = (req.query.endTime as string) || toISAPITime(new Date());

    const maxResults = Number(req.query.maxResults) || 40;
    const position = Number(req.query.position) || 0;

    const xmlBody = buildSearchXML(
      trackID,
      startTime,
      endTime,
      maxResults,
      position
    );

    const response = await nvr.fetch(`${NVR_BASE}/ISAPI/ContentMgmt/search`, {
      method: "POST",
      headers: { "Content-Type": "application/xml" },
      body: xmlBody,
    });

    if (!response.ok) {
      return res
        .status(502)
        .json({ error: "NVR search failed", status: response.status });
    }

    const xml = await response.text();
    res.json(parser.parse(xml));
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to search recordings" });
  }
});

// ===========================
// 2) stream upload to R2
// POST /recordings/sync
// body: { "playbackURI": "...", "key": "recordings/optional-name.mp4" }
// ===========================

app.post("/recordings/sync", async (req, res) => {
  try {
    const { playbackURI, key: customKey } = req.body as {
      playbackURI?: string;
      key?: string;
    };

    if (!playbackURI) {
      return res
        .status(400)
        .json({ error: "playbackURI is required in request body" });
    }

    const key = customKey || `recordings/${randomUUID()}.mp4`;

    // ---- NVR থেকে fetch ----
    const downloadXML = `<?xml version="1.0" encoding="UTF-8"?>
<downloadRequest version="1.0" xmlns="http://www.hikvision.com/ver20/XMLSchema">
  <playbackURI>${escapeXML(playbackURI)}</playbackURI>
</downloadRequest>`;

    const nvrResponse = await nvr.fetch(
      `${NVR_BASE}/ISAPI/ContentMgmt/download`,
      {
        method: "POST",
        headers: { "Content-Type": "application/xml" },
        body: downloadXML,
      }
    );

    if (!nvrResponse.ok || !nvrResponse.body) {
      return res
        .status(502)
        .json({ error: "NVR download failed", status: nvrResponse.status });
    }

    
    const nvrBody = nvrResponse.body as any;
    const nodeStream: Readable =
      typeof nvrBody.pipe === "function"
        ? nvrBody
        : Readable.fromWeb(nvrBody);

    // ---- R2, streaming (multipart) upload ----
    const upload = new Upload({
      client: r2,
      params: {
        Bucket: process.env.R2_BUCKET,
        Key: key,
        Body: nodeStream,
        ContentType: "video/mp4",
      },
      queueSize: 4, // parallel parts
      partSize: 10 * 1024 * 1024, // 10MB per part
    });

    upload.on("httpUploadProgress", (progress) => {
      console.log(`Uploading ${key}:`, progress.loaded, "/", progress.total);
    });

    await upload.done();

    const publicUrl = process.env.R2_PUBLIC_DOMAIN
      ? `https://${process.env.R2_PUBLIC_DOMAIN}/${key}`
      : undefined;

    res.json({ success: true, key, url: publicUrl });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Sync failed" });
  }
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});