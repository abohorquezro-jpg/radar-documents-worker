import express from "express";
import { chromium } from "playwright";

const app = express();
app.use(express.json({ limit: "20mb" }));

const PORT = Number(process.env.PORT || 3000);
const DOCUMENTS_API_URL = process.env.DOCUMENTS_API_URL || "https://infxodoiupqivhgzsgza.supabase.co/functions/v1/secop-documents-api";
const INTERNAL_API_SECRET = process.env.INTERNAL_API_SECRET || "";
const DEFAULT_BUCKET = process.env.SUPABASE_STORAGE_BUCKET || "secop-documents";
const SECOP_DATASET_URL = "https://www.datos.gov.co/resource/dmgg-8hin.json";

if (!INTERNAL_API_SECRET) {
  console.warn("[WARN] Missing INTERNAL_API_SECRET. Requests to secop-documents-api will fail.");
}

function intValue(value, fallback, min = 1, max = 10000) {
  const n = Number(value ?? fallback);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(Math.trunc(n), min), max);
}

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

function sanitizeSegment(value) {
  return String(value || "unknown")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120) || "unknown";
}

function guessContentType(fileName = "", fallback = "application/octet-stream") {
  const ext = String(fileName).toLowerCase().split(".").pop();
  if (ext === "pdf") return "application/pdf";
  if (["jpg", "jpeg"].includes(ext)) return "image/jpeg";
  if (ext === "png") return "image/png";
  if (ext === "doc") return "application/msword";
  if (ext === "docx") return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  if (ext === "xls") return "application/vnd.ms-excel";
  if (ext === "xlsx") return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
  if (ext === "zip") return "application/zip";
  return fallback;
}

function secopHeaders() {
  return {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/pdf,application/octet-stream,image/*,*/*",
    "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
    "Referer": "https://community.secop.gov.co/",
    "Origin": "https://community.secop.gov.co",
    "Connection": "keep-alive"
  };
}

async function withTimeout(ms, fn) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ms);
  try { return await fn(controller.signal); }
  finally { clearTimeout(timer); }
}

async function callDocumentsApi(action, payload = {}) {
  const response = await fetch(DOCUMENTS_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-internal-secret": INTERNAL_API_SECRET
    },
    body: JSON.stringify({ action, ...payload })
  });
  const text = await response.text();
  let data;
  try { data = text ? JSON.parse(text) : {}; } catch { data = { raw: text }; }
  if (!response.ok || data.ok === false) {
    throw new Error(`${action} failed: HTTP ${response.status} ${data.error || data.message || text.slice(0, 300)}`);
  }
  return data;
}

function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
  return chunks;
}

async function runLimited(items, concurrency, worker) {
  const results = new Array(items.length);
  let index = 0;
  const runners = Array.from({ length: Math.max(1, concurrency) }, async () => {
    while (index < items.length) {
      const i = index++;
      try { results[i] = await worker(items[i], i); }
      catch (error) { results[i] = { ok: false, error: error instanceof Error ? error.message : String(error) }; }
    }
  });
  await Promise.all(runners);
  return results;
}

function escapeSoql(value) { return String(value || "").trim().replace(/'/g, "''"); }

async function fetchSecopDocumentsBatch(viableRows, signal) {
  const ids = viableRows.map(v => escapeSoql(v.secop_process_id)).filter(Boolean);
  if (!ids.length) return [];
  const inList = ids.map(id => `'${id}'`).join(",");
  const query = `SELECT id_documento,proceso,nombre_archivo,tamanno_archivo,extensi_n,descripci_n,fecha_carga,entidad,nit_entidad,url_descarga_documento WHERE proceso in(${inList}) LIMIT 50000`;
  const params = new URLSearchParams({ "$query": query });
  const url = `${SECOP_DATASET_URL}?${params.toString()}`;
  const response = await fetch(url, { method: "GET", headers: { "Accept": "application/json" }, signal });
  const text = await response.text();
  if (!response.ok) throw new Error(`datos.gov.co documents batch failed: HTTP ${response.status} ${text.slice(0, 300)}`);
  try { return text ? JSON.parse(text) : []; }
  catch { throw new Error(`datos.gov.co documents batch returned invalid JSON: ${text.slice(0, 300)}`); }
}

function getDownloadUrl(value) {
  if (!value) return null;
  if (typeof value === "object") return value.url || value.uri || null;
  const text = String(value).trim();
  return text || null;
}

function toNumberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(String(value).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : null;
}

function classifyDocument(fileName, description) {
  const text = `${fileName || ""} ${description || ""}`
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  if (text.includes("pliego")) return "pliego";
  if (text.includes("estudio previo") || text.includes("estudios previos")) return "estudios_previos";
  if (text.includes("invitacion")) return "invitacion";
  if (text.includes("adenda")) return "adenda";
  if (text.includes("contrato")) return "contrato";
  if (text.includes("anexo")) return "anexo";
  if (text.includes("formato")) return "formato";
  return "otro";
}

function buildMetadataItems(viableRows, docs) {
  const docsByProceso = new Map();
  for (const d of docs || []) {
    const proceso = String(d.proceso || "").trim();
    if (!proceso) continue;
    if (!docsByProceso.has(proceso)) docsByProceso.set(proceso, []);
    docsByProceso.get(proceso).push(d);
  }

  return viableRows.map(viable => {
    const proceso = String(viable.secop_process_id || "").trim();
    const processDocs = docsByProceso.get(proceso) || [];
    const normalizedDocs = processDocs
      .filter(d => d && d.id_documento)
      .map(d => {
        const fileName = d.nombre_archivo || null;
        const description = d.descripci_n || null;
        const downloadUrl = getDownloadUrl(d.url_descarga_documento);
        return {
          viable_opportunity_id: viable.id,
          external_id: viable.external_id ?? null,
          secop_process_id: proceso,
          secop_document_id: String(d.id_documento),
          file_name: fileName,
          file_extension: d.extensi_n || null,
          file_size_bytes: toNumberOrNull(d.tamanno_archivo),
          description,
          upload_date: d.fecha_carga || null,
          entity_name: d.entidad || null,
          entity_nit: d.nit_entidad || null,
          source_download_url: downloadUrl,
          download_status: "queued",
          document_type: classifyDocument(fileName, description),
          raw_json: d
        };
      })
      .filter(d => d.source_download_url);

    const totalFileSizeBytes = normalizedDocs.reduce((acc, d) => acc + Number(d.file_size_bytes || 0), 0);
    return {
      viable_opportunity_id: viable.id,
      external_id: viable.external_id ?? null,
      secop_process_id: proceso,
      documents_found: normalizedDocs.length,
      documents_total_size_bytes: totalFileSizeBytes,
      documents: normalizedDocs
    };
  });
}

async function fetchFileWithHeaders(url, timeoutMs) {
  return await withTimeout(timeoutMs, async signal => {
    const response = await fetch(url, { method: "GET", redirect: "follow", headers: secopHeaders(), signal });
    const bodyTextOnError = async () => {
      try { return (await response.text()).slice(0, 300); } catch { return ""; }
    };
    if (!response.ok) {
      const body = await bodyTextOnError();
      const error = new Error(`HTTP ${response.status} ${body}`.trim());
      error.status = response.status;
      throw error;
    }
    const arr = await response.arrayBuffer();
    return {
      buffer: Buffer.from(arr),
      contentType: response.headers.get("content-type") || "application/octet-stream"
    };
  });
}

async function fetchFileWithPlaywright(url, timeoutMs, proxyUrl) {
  let browser;
  try {
    const launchOptions = { headless: true };
    if (proxyUrl) launchOptions.proxy = { server: proxyUrl };
    browser = await chromium.launch(launchOptions);
    const context = await browser.newContext({
      userAgent: secopHeaders()["User-Agent"],
      locale: "es-CO",
      extraHTTPHeaders: {
        "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
        "Referer": "https://community.secop.gov.co/"
      }
    });
    const page = await context.newPage();
    await page.goto("https://community.secop.gov.co/", { waitUntil: "domcontentloaded", timeout: Math.min(timeoutMs, 60000) }).catch(() => null);
    const response = await context.request.get(url, { timeout: timeoutMs, headers: secopHeaders() });
    if (!response.ok()) {
      const body = (await response.text().catch(() => "")).slice(0, 300);
      throw new Error(`Playwright download failed: HTTP ${response.status()} ${body}`.trim());
    }
    const buffer = await response.body();
    return {
      buffer,
      contentType: response.headers()["content-type"] || "application/octet-stream"
    };
  } finally {
    if (browser) await browser.close().catch(() => null);
  }
}

async function downloadSecopFile(url, options = {}) {
  const timeoutMs = intValue(options.timeoutMs, 180000, 10000, 600000);
  const usePlaywrightFallback = options.usePlaywrightFallback !== false;
  try {
    return await fetchFileWithHeaders(url, timeoutMs);
  } catch (error) {
    const status = error?.status;
    if (!usePlaywrightFallback || status !== 403) throw new Error(`Download failed: ${error.message}`);
    return await fetchFileWithPlaywright(url, timeoutMs, process.env.SECOP_PROXY_URL || "");
  }
}

async function uploadToSignedUrl(signedUrl, buffer, contentType, timeoutMs) {
  return await withTimeout(timeoutMs, async signal => {
    const response = await fetch(signedUrl, {
      method: "PUT",
      headers: { "Content-Type": contentType || "application/octet-stream" },
      body: buffer,
      signal
    });
    const text = await response.text().catch(() => "");
    if (!response.ok) throw new Error(`Signed upload failed: HTTP ${response.status} ${text.slice(0, 300)}`);
    return true;
  });
}

function buildStoragePath(doc) {
  const processId = sanitizeSegment(doc.secop_process_id || doc.external_id || "unknown_process");
  const docId = sanitizeSegment(doc.secop_document_id || doc.id);
  const fileName = sanitizeSegment(doc.file_name || `${docId}.bin`);
  return `${processId}/${docId}_${fileName}`;
}

async function processOneDocument(doc, options) {
  const maxBytes = intValue(options.max_file_mb, 30, 1, 500) * 1024 * 1024;
  const downloadTimeoutMs = intValue(process.env.DOWNLOAD_TIMEOUT_SECONDS, 180, 10, 600) * 1000;
  const uploadTimeoutMs = intValue(process.env.UPLOAD_TIMEOUT_SECONDS, 180, 10, 600) * 1000;
  const sourceUrl = doc.source_download_url;
  if (!sourceUrl) throw new Error("Missing source_download_url");

  try {
    const downloaded = await downloadSecopFile(sourceUrl, {
      timeoutMs: downloadTimeoutMs,
      usePlaywrightFallback: options.use_playwright_fallback !== false
    });

    if (downloaded.buffer.length > maxBytes) {
      throw new Error(`File too large: ${downloaded.buffer.length} bytes > ${maxBytes}`);
    }

    const contentType = downloaded.contentType || guessContentType(doc.file_name);
    const storagePath = buildStoragePath(doc);
    const signed = await callDocumentsApi("create_signed_upload_url", {
      document_id: doc.id,
      bucket: DEFAULT_BUCKET,
      storage_path: storagePath,
      content_type: contentType
    });

    const signedUrl = signed.signed_upload_url || signed.signedUrl;
    if (!signedUrl) throw new Error("Missing signed upload URL from Edge Function");

    await uploadToSignedUrl(signedUrl, downloaded.buffer, contentType, uploadTimeoutMs);

    await callDocumentsApi("complete_document_upload", {
      document_id: doc.id,
      storage_bucket: DEFAULT_BUCKET,
      storage_path: storagePath,
      file_size_bytes: downloaded.buffer.length,
      content_type: contentType
    });

    return { ok: true, document_id: doc.id, secop_document_id: doc.secop_document_id, storage_path: storagePath };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await callDocumentsApi("fail_document_upload", { document_id: doc.id, error_message: message }).catch(() => null);
    return { ok: false, document_id: doc.id, secop_document_id: doc.secop_document_id, error: message };
  }
}

async function processPendingDocuments(body = {}) {
  const limit = intValue(body.limit, process.env.DOWNLOAD_LIMIT || 20, 1, 500);
  const concurrency = intValue(body.concurrency ?? body.download_concurrency, process.env.DOWNLOAD_CONCURRENCY || 1, 1, 10);
  const onlyStatus = body.only_status || process.env.CLAIM_ONLY_STATUS || "queued";
  const claim = await callDocumentsApi("claim_documents_for_worker", { limit, only_status: onlyStatus });
  const documents = Array.isArray(claim.documents) ? claim.documents : [];

  const results = await runLimited(documents, concurrency, doc => processOneDocument(doc, body));
  const downloaded = results.filter(r => r?.ok).length;
  const failed = results.filter(r => r && r.ok === false).length;
  return {
    selected: documents.length,
    processed: results.length,
    downloaded,
    failed,
    results
  };
}

async function processPipeline(body = {}) {
  const opportunityLimit = intValue(body.opportunity_limit, process.env.OPPORTUNITY_LIMIT || 2500, 1, 2500);
  const batchSize = intValue(body.batch_size, process.env.BATCH_SIZE || 50, 1, 100);
  const metadataConcurrency = intValue(body.metadata_concurrency, process.env.METADATA_CONCURRENCY || 3, 1, 8);
  const source = body.source || "railway_documents_pipeline";

  const viableResp = await callDocumentsApi("list_viable_for_documents", {
    limit: opportunityLimit,
    older_than_hours: body.older_than_hours ?? 24,
    quality_statuses: body.quality_statuses,
    exclude_document_statuses: body.exclude_document_statuses
  });

  const viableRows = Array.isArray(viableResp.rows) ? viableResp.rows : [];
  const batches = chunkArray(viableRows, batchSize);

  const batchResults = await runLimited(batches, metadataConcurrency, async (batch, idx) => {
    const docs = await withTimeout(120000, signal => fetchSecopDocumentsBatch(batch, signal));
    const items = buildMetadataItems(batch, docs);
    if (body.dry_run === true) {
      return { ok: true, dry_run: true, batch_number: idx + 1, viables: batch.length, docs_found: docs.length, items: items.length };
    }
    const saved = await callDocumentsApi("save_documents_metadata_batch", {
      source,
      batch_number: idx + 1,
      items
    });
    return { ok: true, batch_number: idx + 1, viables: batch.length, docs_found: docs.length, saved };
  });

  const metadataDocsFound = batchResults.reduce((acc, r) => acc + Number(r?.docs_found || 0), 0);
  const metadataSaved = batchResults.reduce((acc, r) => acc + Number(r?.saved?.upserted || 0), 0);
  const metadataFailedBatches = batchResults.filter(r => r?.ok === false).length;

  let downloadResult = { selected: 0, processed: 0, downloaded: 0, failed: 0, results: [] };
  if (body.dry_run !== true) {
    downloadResult = await processPendingDocuments({
      ...body,
      limit: body.download_limit ?? process.env.DOWNLOAD_LIMIT ?? 100,
      concurrency: body.download_concurrency ?? process.env.DOWNLOAD_CONCURRENCY ?? 2,
      only_status: body.only_status || "queued"
    });
  }

  return {
    opportunities_selected: viableRows.length,
    metadata_batches: batches.length,
    metadata_failed_batches: metadataFailedBatches,
    documents_found: metadataDocsFound,
    documents_queued_or_upserted: metadataSaved,
    download: downloadResult,
    metadata_results: batchResults
  };
}

function requireInternal(req, res, next) {
  const provided = req.headers["x-internal-secret"] || "";
  if (!INTERNAL_API_SECRET || provided !== INTERNAL_API_SECRET) {
    return res.status(401).json({ ok: false, error: "Unauthorized: missing or invalid x-internal-secret" });
  }
  next();
}

app.get("/health", (req, res) => {
  res.json({ ok: true, service: "radar-documents-worker", version: "2.0.0-pipeline", has_documents_api_url: Boolean(DOCUMENTS_API_URL), has_internal_secret: Boolean(INTERNAL_API_SECRET) });
});

app.post("/secop-documents/process-pending", requireInternal, async (req, res) => {
  const started = Date.now();
  try {
    const result = await processPendingDocuments(req.body || {});
    res.json({ ok: true, service: "radar-documents-worker", version: "2.0.0-pipeline", mode: "process_pending", ...result, duration_ms: Date.now() - started, timestamp: new Date().toISOString() });
  } catch (error) {
    res.status(500).json({ ok: false, service: "radar-documents-worker", mode: "process_pending", error: error instanceof Error ? error.message : String(error), duration_ms: Date.now() - started, timestamp: new Date().toISOString() });
  }
});

app.post("/secop-documents/process-pipeline", requireInternal, async (req, res) => {
  const started = Date.now();
  try {
    const result = await processPipeline(req.body || {});
    res.json({ ok: true, service: "radar-documents-worker", version: "2.0.0-pipeline", mode: "metadata_and_download", ...result, duration_ms: Date.now() - started, timestamp: new Date().toISOString() });
  } catch (error) {
    res.status(500).json({ ok: false, service: "radar-documents-worker", version: "2.0.0-pipeline", mode: "metadata_and_download", error: error instanceof Error ? error.message : String(error), duration_ms: Date.now() - started, timestamp: new Date().toISOString() });
  }
});

app.listen(PORT, () => {
  console.log(`radar-documents-worker 2.0.0-pipeline listening on ${PORT}`);
});
