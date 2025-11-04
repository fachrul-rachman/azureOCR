import express from "express";
import fs from "fs";
import path from "path";
import multer from "multer";
import axios from "axios";
import dotenv from "dotenv";
import { execSync } from "child_process";
import sharp from "sharp";

dotenv.config();

const ENDPOINT = (process.env.ENDPOINT || "").replace(/\/+$/, "");
const API_KEY = process.env.API_KEY || "";
const API_VERSION = process.env.API_VERSION || "2024-07-31";
const MODEL = process.env.MODEL || "prebuilt-layout";
const PORT = process.env.PORT || 8080;

// Compression threshold (bytes). Default 10 MB — tweak to taste.
const MAX_SIZE_BYTES = parseInt(process.env.MAX_SIZE_BYTES || String(10 * 1024 * 1024), 10);
// Polling configs
const TIMEOUT_MS = parseInt(process.env.TIMEOUT_MS || "120000", 10);
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "1000", 10);

if (!ENDPOINT || !API_KEY) {
  console.error("Please set ENDPOINT and API_KEY in .env");
  process.exit(1);
}

const app = express();
app.use(express.json());

const upload = multer({ dest: path.join(process.cwd(), "uploads"), limits: { fileSize: 60 * 1024 * 1024 } }); // temp limit 60MB

// simple sentence splitter
function splitIntoSentences(text) {
  if (!text) return [];
  return text
    .replace(/\r\n/g, " ")
    .replace(/\n/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(/(?<=[\.!\?])\s+/g)
    .map(s => s.trim())
    .filter(Boolean);
}

// compress PDF using Ghostscript (command-line). Returns path to compressed file.
// Requires `gs` or `gswin64c` available in PATH.
function compressPdfWithGhostscript(inputPath, outPath) {
  // choose executable name
  const gsCmd = (() => {
    try {
      execSync("gs -v", { stdio: "ignore" });
      return "gs";
    } catch (e) {
      try {
        execSync("gswin64c -v", { stdio: "ignore" });
        return "gswin64c";
      } catch (e2) {
        throw new Error("Ghostscript not found. Install Ghostscript and ensure 'gs' or 'gswin64c' is in PATH.");
      }
    }
  })();

  // /screen gives lowest file size (use /ebook or /printer for better quality)
  const args = [
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    "-dPDFSETTINGS=/screen",
    "-dNOPAUSE",
    "-dQUIET",
    "-dBATCH",
    `-sOutputFile=${outPath}`,
    `${inputPath}`
  ].join(" ");

  execSync(`${gsCmd} ${args}`);
  return outPath;
}

// compress image using sharp (resize if large, set quality)
async function compressImage(inputPath, outPath) {
  const img = sharp(inputPath);
  const meta = await img.metadata();
  // If huge resolution, resize to max 4000px on longest side
  const maxDim = 4000;
  if ((meta.width && meta.width > maxDim) || (meta.height && meta.height > maxDim)) {
    await img.resize({ width: meta.width > meta.height ? maxDim : null, height: meta.height >= meta.width ? maxDim : null }).jpeg({ quality: 80 }).toFile(outPath);
  } else {
    // just convert to jpeg with quality to save size
    await img.jpeg({ quality: 80 }).toFile(outPath);
  }
  return outPath;
}

// try multiple analyze endpoints (modern & legacy); returns operation-location
async function postAnalyzeCandidateBinary(filePath, contentType) {
  const candidates = [
    `${ENDPOINT}/documentModels/${MODEL}:analyze?api-version=${API_VERSION}`,
    `${ENDPOINT}/formrecognizer/documentModels/${MODEL}:analyze?api-version=${API_VERSION}`,
    `${ENDPOINT}/formrecognizer/v2.1/layout/analyze`
  ];

  const stat = fs.statSync(filePath);
  for (const url of candidates) {
    try {
      const stream = fs.createReadStream(filePath);
      const r = await axios.post(url, stream, {
        headers: {
          "Ocp-Apim-Subscription-Key": API_KEY,
          "Content-Type": contentType || "application/pdf",
          "Content-Length": stat.size
        },
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
        validateStatus: s => (s >= 200 && s < 300) || s === 202,
        timeout: 60000
      });
      const op = r.headers["operation-location"] || r.headers["Operation-Location"];
      if (!op) throw new Error("No operation-location header");
      return op;
    } catch (err) {
      // log but continue trying next candidate
      // console.log("candidate fail:", url, err?.response?.status, err?.response?.data);
    }
  }
  throw new Error("All analyze endpoints failed (404 or incompatible resource).");
}

async function pollOperation(opUrl) {
  const start = Date.now();
  while (true) {
    if (Date.now() - start > TIMEOUT_MS) throw new Error("Timeout polling operation result.");
    const r = await axios.get(opUrl, { headers: { "Ocp-Apim-Subscription-Key": API_KEY } });
    const data = r.data;
    const status = (data.status || "").toLowerCase();
    if (status === "succeeded") return data;
    if (status === "failed") throw new Error("Operation failed: " + JSON.stringify(data));
    await new Promise(rp => setTimeout(rp, POLL_INTERVAL_MS));
  }
}

function extractPagesFromAzureResult(data) {
  const analyze = data.analyzeResult || data;
  const pagesCandidates = analyze.pages || analyze.readResults || analyze.pageResults || analyze.readResults || [];
  const normalized = (pagesCandidates || []).map(p => {
    const raw = p.text ?? p.content ?? (Array.isArray(p.lines) ? p.lines.map(l => l.text || l.content).join(" ") : "");
    return { pageNumber: p.page ?? p.pageNumber ?? p.pageIndex ?? null, rawText: raw };
  });
  return normalized;
}

// main endpoint
app.post("/analyze", upload.single("file"), async (req, res) => {
  const file = req.file;
  if (!file) return res.status(400).json({ error: "file is required as multipart/form-data field 'file'" });

  const origPath = file.path;
  const origName = file.originalname || file.filename;
  const mimeType = file.mimetype || "application/pdf";

  let workPath = origPath;
  let compressed = false;

  try {
    const size = fs.statSync(origPath).size;
    if (size > MAX_SIZE_BYTES) {
      // compress
      const ext = path.extname(origName).toLowerCase();
      const outName = `${path.basename(origPath)}-compressed${ext}`;
      const outPath = path.join(path.dirname(origPath), outName);
      if (ext === ".pdf") {
        // try ghostscript
        try {
          compressPdfWithGhostscript(origPath, outPath);
          workPath = outPath;
          compressed = true;
        } catch (e) {
          // if ghostscript missing, fall back: don't compress PDF (warn)
          console.warn("PDF compress failed or Ghostscript missing:", e.message);
          // leave workPath as origPath
        }
      } else if ([".png", ".jpg", ".jpeg", ".tiff", ".tif"].includes(ext)) {
        await compressImage(origPath, outPath);
        workPath = outPath;
        compressed = true;
      } else {
        // unknown type: skip compression
      }
    }

    // check file size again (if still too large and > Azure limit, bail)
    const finalSize = fs.statSync(workPath).size;
    if (finalSize > 50 * 1024 * 1024) { // Azure limit
      throw new Error(`File too large after compression: ${Math.round(finalSize / 1024 / 1024)} MB. Azure limit is 50 MB.`);
    }

    // call Azure (binary)
    const contentType = mimeType;
    const operationLocation = await postAnalyzeCandidateBinary(workPath, contentType);

    // polling
    const opResult = await pollOperation(operationLocation);

    // parse pages
    const pages = extractPagesFromAzureResult(opResult);
    const pagesOut = pages.map(p => ({ pageNumber: p.pageNumber, rawText: p.rawText, sentences: splitIntoSentences(p.rawText) }));

    // cleanup
    try { fs.unlinkSync(origPath); } catch (e) {}
    if (compressed && workPath && workPath !== origPath) {
      try { fs.unlinkSync(workPath); } catch (e) {}
    }

    return res.json({ filename: origName, pageCount: pagesOut.length, pages: pagesOut });
  } catch (err) {
    // cleanup temp files
    try { fs.unlinkSync(origPath); } catch (e) {}
    if (workPath && workPath !== origPath) try { fs.unlinkSync(workPath); } catch (e) {}

    console.error("Analyze error:", err?.response?.data ?? err.message ?? err);
    return res.status(500).json({ error: err?.response?.data ?? String(err.message ?? err) });
  }
});

app.listen(PORT, () => console.log(`DocInt simple backend running on port ${PORT}`));
