// server.js
import express from "express";
import fs from "fs";
import path from "path";
import multer from "multer";
import axios from "axios";
import dotenv from "dotenv";
import { execSync, spawn } from "child_process";
import sharp from "sharp";
import { pipeline } from "stream";
import { promisify } from "util";

dotenv.config();
const pipe = promisify(pipeline);

const PORT = process.env.PORT || 8080;
const API_KEY = process.env.API_KEY || "";
// endpoint base not used for analyze URL: use ANALYZE_URL or fallback to ENDPOINT + path
const ANALYZE_URL =
  process.env.ANALYZE_URL ||
  `${(process.env.ENDPOINT || "").replace(/\/+$/, "")}/formrecognizer/v2.1/layout/analyze`;
// max size threshold for attempting compression (bytes). Default 10MB
const MAX_SIZE_BYTES = parseInt(process.env.MAX_SIZE_BYTES || String(10 * 1024 * 1024), 10);
// Azure upload hard limit (50MB for layout v2.1)
const AZURE_MAX_BYTES = 50 * 1024 * 1024;
// Poll configs
const TIMEOUT_MS = parseInt(process.env.TIMEOUT_MS || "120000", 10);
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "1000", 10);

if (!ANALYZE_URL || !API_KEY) {
  console.error("Please set ANALYZE_URL (or ENDPOINT) and API_KEY in environment.");
  process.exit(1);
}

const app = express();
app.use(express.json());

// uploads temp dir
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
await fs.promises.mkdir(UPLOAD_DIR, { recursive: true });

// Multer: allow uploads up to a reasonable hard cap (e.g. 60MB). We'll enforce MAX_SIZE_BYTES policy after upload.
const multerLimits = { fileSize: 60 * 1024 * 1024 };
const upload = multer({ dest: UPLOAD_DIR, limits: multerLimits });

// startup: detect Ghostscript once
let GS_CMD = null;
try {
  execSync("gs -v", { stdio: "ignore" });
  GS_CMD = "gs";
} catch (e1) {
  try {
    execSync("gswin64c -v", { stdio: "ignore" });
    GS_CMD = "gswin64c";
  } catch (e2) {
    GS_CMD = null;
    console.warn("Ghostscript not found at startup. PDF compression path will be disabled.");
  }
}

// simple sentence splitter (compatible with Node's regex engines)
function splitIntoSentences(text) {
  if (!text) return [];
  return text
    .replace(/\r\n/g, " ")
    .replace(/\n/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(/([.?!])\s+/g) // split by punctuation but keep them
    .reduce((acc, chunk, idx, arr) => {
      if (/[.?!]/.test(chunk) && acc.length) {
        acc[acc.length - 1] += chunk;
      } else if (chunk.trim()) {
        acc.push(chunk.trim());
      }
      return acc;
    }, [])
    .map(s => s.trim())
    .filter(Boolean);
}

// compress image using sharp (async)
async function compressImage(inputPath, outPath) {
  const img = sharp(inputPath);
  const meta = await img.metadata();
  const maxDim = 4000;
  if ((meta.width && meta.width > maxDim) || (meta.height && meta.height > maxDim)) {
    await img
      .resize({
        width: meta.width > meta.height ? maxDim : null,
        height: meta.height >= meta.width ? maxDim : null
      })
      .jpeg({ quality: 80 })
      .toFile(outPath);
  } else {
    await img.jpeg({ quality: 80 }).toFile(outPath);
  }
  return outPath;
}

// compress PDF using Ghostscript via spawn (non-blocking). Returns outPath
function compressPdfWithGhostscriptSpawn(inputPath, outPath) {
  if (!GS_CMD) throw new Error("Ghostscript not available on server.");
  // args array (not joined) for spawn
  const args = [
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    "-dPDFSETTINGS=/screen",
    "-dNOPAUSE",
    "-dQUIET",
    "-dBATCH",
    `-sOutputFile=${outPath}`,
    `${inputPath}`
  ];
  return new Promise((resolve, reject) => {
    const cp = spawn(GS_CMD, args, { stdio: "inherit" });
    cp.on("error", err => reject(err));
    cp.on("close", code => {
      if (code === 0) return resolve(outPath);
      return reject(new Error(`Ghostscript exited with code ${code}`));
    });
  });
}

// post file to the single analyze endpoint you requested; return operation-location header
async function postAnalyzeBinary(filePath, contentType) {
  const stat = await fs.promises.stat(filePath);
  const stream = fs.createReadStream(filePath);

  const headers = {
    "Ocp-Apim-Subscription-Key": API_KEY,
    "Content-Type": contentType || "application/pdf",
    "Content-Length": stat.size
  };

  const r = await axios.post(ANALYZE_URL, stream, {
    headers,
    maxBodyLength: Infinity,
    maxContentLength: Infinity,
    validateStatus: s => (s >= 200 && s < 300) || s === 202,
    timeout: 60000
  });

  const op = r.headers["operation-location"] || r.headers["Operation-Location"] || r.headers["operation_location"];
  if (!op) throw new Error("Analyze request did not return operation-location header.");
  return op;
}

// poll with simple exponential backoff on 429
async function pollOperation(opUrl) {
  const start = Date.now();
  let attempt = 0;
  while (true) {
    if (Date.now() - start > TIMEOUT_MS) throw new Error("Timeout polling operation result.");
    try {
      const r = await axios.get(opUrl, { headers: { "Ocp-Apim-Subscription-Key": API_KEY } });
      const data = r.data;
      const status = (data.status || "").toLowerCase();
      if (status === "succeeded") return data;
      if (status === "failed") throw new Error("Operation failed: " + JSON.stringify(data));
      // still running
      await new Promise(rp => setTimeout(rp, POLL_INTERVAL_MS));
      attempt = 0; // reset retry counter on successful non-429 poll
    } catch (err) {
      const status = err?.response?.status;
      if (status === 429) {
        // exponential backoff
        attempt++;
        const backoff = Math.min(1000 * 2 ** attempt, 30_000);
        await new Promise(rp => setTimeout(rp, backoff));
        continue;
      }
      // for other transient http errors, small wait and retry
      if (status >= 500 && status < 600) {
        await new Promise(rp => setTimeout(rp, 2000));
        continue;
      }
      throw err;
    }
  }
}

// normalize different possible shapes of Azure response into page array { pageNumber, rawText }
function extractPagesFromAzureResult(data) {
  const analyze = data.analyzeResult || data;
  // layout API v2.1 returns "readResults" usually
  const candidates = analyze.readResults || analyze.pageResults || analyze.pages || analyze.pages || [];
  const pages = Array.isArray(candidates) ? candidates : [];
  return pages.map(p => {
    // readResults has 'text' or lines array
    let raw = "";
    if (typeof p.text === "string") raw = p.text;
    else if (Array.isArray(p.lines)) raw = p.lines.map(l => l.text || l.content || "").join(" ");
    else if (typeof p.content === "string") raw = p.content;
    return {
      pageNumber: p.page || p.pageNumber || p.pageIndex || null,
      rawText: raw || ""
    };
  });
}

// main endpoint
app.post("/analyze", upload.single("file"), async (req, res) => {
  const file = req.file;
  if (!file) {
    return res.status(400).json({ error: "file is required as multipart/form-data field 'file'" });
  }

  const origPath = file.path;
  const origName = file.originalname || file.filename;
  const mimeType = file.mimetype || "application/pdf";
  let workPath = origPath;
  let compressed = false;

  try {
    const stat = await fs.promises.stat(origPath);
    const size = stat.size;

    // If file exceeds configured MAX_SIZE_BYTES, attempt compression
    if (size > MAX_SIZE_BYTES) {
      const ext = path.extname(origName).toLowerCase();
      const outName = `${path.basename(origPath)}-compressed${ext}`;
      const outPath = path.join(path.dirname(origPath), outName);

      if (ext === ".pdf") {
        // If GS not available, follow your requested policy: reject large PDF
        if (!GS_CMD) {
          throw { status: 413, message: `PDF too large and server has no Ghostscript; cannot accept files > ${Math.round(MAX_SIZE_BYTES / 1024 / 1024)} MB.` };
        }
        // compress via spawn (non-blocking)
        await compressPdfWithGhostscriptSpawn(origPath, outPath);
        workPath = outPath;
        compressed = true;
      } else if ([".png", ".jpg", ".jpeg", ".tiff", ".tif"].includes(ext)) {
        await compressImage(origPath, outPath);
        workPath = outPath;
        compressed = true;
      } else {
        // unknown type; cannot compress — reject
        throw { status: 415, message: "Unsupported file type for compression" };
      }
    }

    const finalStat = await fs.promises.stat(workPath);
    if (finalStat.size > AZURE_MAX_BYTES) {
      throw { status: 413, message: `File too large after compression (${Math.round(finalStat.size / (1024 * 1024))} MB). Azure limit is ${Math.round(AZURE_MAX_BYTES / (1024 * 1024))} MB.` };
    }

    // call Azure analyze (binary)
    const operationLocation = await postAnalyzeBinary(workPath, mimeType);

    // poll until done
    const opResult = await pollOperation(operationLocation);

    // parse pages => sentences
    const pagesRaw = extractPagesFromAzureResult(opResult);
    const pagesOut = pagesRaw.map(p => ({
      pageNumber: p.pageNumber,
      rawText: p.rawText,
      sentences: splitIntoSentences(p.rawText)
    }));

    // cleanup temp files (async)
    try { await fs.promises.unlink(origPath); } catch (e) { /* ignore */ }
    if (compressed && workPath && workPath !== origPath) {
      try { await fs.promises.unlink(workPath); } catch (e) { /* ignore */ }
    }

    return res.json({ filename: origName, pageCount: pagesOut.length, pages: pagesOut });
  } catch (err) {
    // Cleanup
    try { await fs.promises.unlink(origPath); } catch (e) { /* ignore */ }
    if (workPath && workPath !== origPath) try { await fs.promises.unlink(workPath); } catch (e) { /* ignore */ }

    // If err has status (we threw it intentionally), respect it
    if (err && err.status) {
      return res.status(err.status).json({ error: err.message || String(err) });
    }

    console.error("Analyze error:", err?.response?.data ?? err?.message ?? err);
    return res.status(500).json({ error: err?.response?.data ?? String(err?.message ?? err) });
  }
});

app.listen(PORT, () => console.log(`DocInt backend running on port ${PORT}`));
