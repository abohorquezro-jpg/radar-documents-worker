import express from "express";

const app = express();
app.use(express.json({ limit: "5mb" }));

const {
  INTERNAL_API_SECRET,
  DOCUMENTS_API_URL,
  SUPABASE_STORAGE_BUCKET = "secop-documents",
  MAX_FILE_MB = "80",
  DEFAULT_LIMIT = "20",
  MAX_LIMIT = "100",
  CONCURRENCY = "3",
  DOWNLOAD_TIMEOUT_SECONDS = "180",
  UPLOAD_TIMEOUT_SECONDS = "180",
  CLAIM_ONLY_STATUS = "queued"
} = process.env;

const WORKER_VERSION = "1.0.1-production-secops-403-fix";

function nowIso() {
  return new Date().toISOString();
}

function toInt(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function json(res, status, body) {
  return res.status(status).json({
    ...body,
    timestamp: nowIso()
  });
}

function requireInternalSecret(req, res, next) {
  const provided = req.headers["x-internal-secret"];

  if (!INTERNAL_API_SECRET) {
    return json(res, 500, {
      ok: false,
      error: "INTERNAL_API_SECRET is not configured"
    });
  }

  if (provided !== INTERNAL_API_SECRET) {
    return json(res, 401, {
      ok: false,
      error: "Unauthorized"
    });
  }

  return next();
}

function withTimeout(seconds) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(1, seconds) * 1000);
  return { controller, timeout };
}

async function parseJsonResponse(response) {
  const text = await response.text();
  if (!text) return {};

  try {
    return JSON.parse(text);
  } catch {
    return { raw_response: text };
  }
}

async function readBodyPreview(response, maxChars = 300) {
  try {
    const text = await response.text();
    return text ? text.slice(0, maxChars) : "";
  } catch {
    return "";
  }
}

function secopBrowserHeaders() {
  return {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/pdf,application/octet-stream,image/*,*/*",
    "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
    "Referer": "https://community.secop.gov.co/",
    "Origin": "https://community.secop.gov.co",
    "Connection": "keep-alive"
  };
}

async function downloadSecopFile(url, signal) {
  return fetch(url, {
    method: "GET",
    redirect: "follow",
    headers: secopBrowserHeaders(),
    signal
  });
}

async function callDocumentsApi(payload) {
  if (!DOCUMENTS_API_URL) {
    throw new Error("DOCUMENTS_API_URL is not configured");
  }

  const { controller, timeout } = withTimeout(60);

  try {
    const response = await fetch(DOCUMENTS_API_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-internal-secret": INTERNAL_API_SECRET
      },
      body: JSON.stringify(payload),
      signal: controller.signal
    });

    const data = await parseJsonResponse(response);

    if (!response.ok || data.ok === false) {
      throw new Error(
        data?.error ||
          data?.message ||
          data?.raw_response ||
          `Documents API failed: HTTP ${response.status}`
      );
    }

    return data;
  } finally {
    clearTimeout(timeout);
  }
}

function sanitizeFileName(value) {
  const raw = String(value || "documento")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\w.\-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");

  return raw || "documento";
}

function getExtension(doc) {
  const ext = String(doc.file_extension || "")
    .trim()
    .replace(/^\./, "")
    .toLowerCase();

  if (ext) return ext;

  const match = String(doc.file_name || "").match(/\.([a-z0-9]+)$/i);
  return match ? match[1].toLowerCase() : "bin";
}

function buildStoragePath(doc) {
  const processId = sanitizeFileName(doc.secop_process_id || "unknown-process");
  const documentId = sanitizeFileName(doc.secop_document_id || doc.id);
  const ext = getExtension(doc);

  let fileName = sanitizeFileName(doc.file_name || `${documentId}.${ext}`);

  if (!fileName.toLowerCase().endsWith(`.${ext}`)) {
    fileName = `${fileName}.${ext}`;
  }

  return `${processId}/${documentId}-${fileName}`;
}

function normalizeDocumentsApiRows(data) {
  if (Array.isArray(data?.documents)) return data.documents;
  if (Array.isArray(data?.rows)) return data.rows;
  if (Array.isArray(data?.data)) return data.data;
  return [];
}

async function claimDocuments(limit, onlyStatus, source) {
  const response = await callDocumentsApi({
    action: "claim_documents_for_worker",
    limit,
    only_status: onlyStatus,
    worker_source: source,
    worker_version: WORKER_VERSION
  });

  return normalizeDocumentsApiRows(response);
}

async function createSignedUploadUrl({ documentId, storagePath, contentType, contentLength }) {
  const response = await callDocumentsApi({
    action: "create_signed_upload_url",
    document_id: documentId,
    storage_path: storagePath,
    content_type: contentType,
    content_length: contentLength || null,
    bucket: SUPABASE_STORAGE_BUCKET
  });

  const signedUploadUrl =
    response.signed_upload_url ||
    response.signedUploadUrl ||
    response.signedUrl ||
    response.url;

  if (!signedUploadUrl) {
    throw new Error("Documents API did not return signed_upload_url");
  }

  return {
    signedUploadUrl,
    token: response.token || null,
    uploadMethod: response.upload_method || response.method || "PUT"
  };
}

async function markComplete({ documentId, storagePath, fileSizeBytes, contentType }) {
  await callDocumentsApi({
    action: "complete_document_upload",
    document_id: documentId,
    storage_path: storagePath,
    storage_bucket: SUPABASE_STORAGE_BUCKET,
    file_size_bytes: fileSizeBytes,
    content_type: contentType,
    status: "downloaded",
    worker_version: WORKER_VERSION
  });
}

async function markFailed({ documentId, errorMessage }) {
  if (!documentId) return;

  await callDocumentsApi({
    action: "fail_document_upload",
    document_id: documentId,
    error_message: String(errorMessage || "Unknown error").slice(0, 2000),
    worker_version: WORKER_VERSION
  }).catch(() => {});
}

async function uploadBufferToSignedUrl({ signedUploadUrl, uploadMethod, token, buffer, contentType }) {
  const { controller, timeout } = withTimeout(toInt(UPLOAD_TIMEOUT_SECONDS, 180));

  try {
    const uploadResponse = await fetch(signedUploadUrl, {
      method: uploadMethod,
      headers: {
        "Content-Type": contentType,
        "Content-Length": String(buffer.length),
        ...(token ? { Authorization: `Bearer ${token}` } : {})
      },
      body: buffer,
      signal: controller.signal
    });

    if (!uploadResponse.ok) {
      const uploadText = await uploadResponse.text().catch(() => "");
      throw new Error(`Signed upload failed: HTTP ${uploadResponse.status} ${uploadText}`.trim());
    }
  } finally {
    clearTimeout(timeout);
  }
}

async function processOneDocument(doc, options) {
  const documentId = doc.id;
  const maxBytes = options.maxFileMb * 1024 * 1024;

  if (!documentId) {
    throw new Error("Document missing id");
  }

  if (!doc.source_download_url) {
    throw new Error("Document missing source_download_url");
  }

  const storagePath = buildStoragePath(doc);

  const { controller, timeout } = withTimeout(toInt(DOWNLOAD_TIMEOUT_SECONDS, 180));

  try {
    const downloadResponse = await downloadSecopFile(doc.source_download_url, controller.signal);

    if (!downloadResponse.ok) {
      const bodyPreview = await readBodyPreview(downloadResponse, 300);
      const suffix = bodyPreview ? ` Body: ${bodyPreview}` : "";
      throw new Error(`Download failed: HTTP ${downloadResponse.status}${suffix}`);
    }

    const contentType =
      downloadResponse.headers.get("content-type") || "application/octet-stream";

    const contentLength = Number(downloadResponse.headers.get("content-length") || "0");
    if (contentLength && contentLength > maxBytes) {
      throw new Error(`File exceeds MAX_FILE_MB=${options.maxFileMb}`);
    }

    const arrayBuffer = await downloadResponse.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    if (!buffer.length) {
      throw new Error("Downloaded file is empty");
    }

    if (buffer.length > maxBytes) {
      throw new Error(`File exceeds MAX_FILE_MB=${options.maxFileMb}`);
    }

    const { signedUploadUrl, token, uploadMethod } = await createSignedUploadUrl({
      documentId,
      storagePath,
      contentType,
      contentLength: buffer.length
    });

    await uploadBufferToSignedUrl({
      signedUploadUrl,
      uploadMethod,
      token,
      buffer,
      contentType
    });

    await markComplete({
      documentId,
      storagePath,
      fileSizeBytes: buffer.length,
      contentType
    });

    return {
      ok: true,
      document_id: documentId,
      secop_document_id: doc.secop_document_id,
      storage_path: storagePath,
      file_size_bytes: buffer.length,
      content_type: contentType
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function runWithConcurrency(items, concurrency, handler) {
  const results = [];
  let cursor = 0;

  async function worker() {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;

      try {
        const value = await handler(items[index], index);
        results[index] = { status: "fulfilled", value };
      } catch (error) {
        results[index] = {
          status: "rejected",
          reason: error instanceof Error ? error.message : String(error)
        };
      }
    }
  }

  const workers = Array.from(
    { length: Math.min(Math.max(1, concurrency), Math.max(1, items.length)) },
    () => worker()
  );

  await Promise.all(workers);
  return results;
}

app.get("/", (_req, res) => {
  return json(res, 200, {
    ok: true,
    service: "radar-documents-worker",
    version: WORKER_VERSION,
    mode: "railway-heavy-processing-edge-function-secure-control",
    routes: ["/health", "POST /secop-documents/process-pending"]
  });
});

app.get("/health", (_req, res) => {
  return json(res, 200, {
    ok: true,
    service: "radar-documents-worker",
    version: WORKER_VERSION,
    mode: "edge-api",
    has_documents_api_url: Boolean(DOCUMENTS_API_URL),
    has_internal_secret: Boolean(INTERNAL_API_SECRET),
    bucket: SUPABASE_STORAGE_BUCKET,
    default_limit: toInt(DEFAULT_LIMIT, 20),
    max_limit: toInt(MAX_LIMIT, 100),
    concurrency: toInt(CONCURRENCY, 3),
    max_file_mb: toInt(MAX_FILE_MB, 80)
  });
});

app.post("/secop-documents/process-pending", requireInternalSecret, async (req, res) => {
  const maxLimit = toInt(MAX_LIMIT, 100);
  const defaultLimit = toInt(DEFAULT_LIMIT, 20);
  const requestedLimit = toInt(req.body?.limit, defaultLimit);
  const limit = Math.min(Math.max(1, requestedLimit), maxLimit);

  const concurrency = Math.min(
    Math.max(1, toInt(req.body?.concurrency, toInt(CONCURRENCY, 3))),
    10
  );

  const onlyStatus = String(req.body?.only_status || CLAIM_ONLY_STATUS || "queued");
  const source = String(req.body?.source || "manual");
  const maxFileMb = toInt(req.body?.max_file_mb, toInt(MAX_FILE_MB, 80));

  const summary = {
    ok: true,
    service: "radar-documents-worker",
    version: WORKER_VERSION,
    source,
    mode: "railway_downloads_edge_function_signed_upload",
    limit,
    concurrency,
    max_file_mb: maxFileMb,
    selected: 0,
    processed: 0,
    downloaded: 0,
    failed: 0,
    errors: [],
    results: []
  };

  try {
    const documents = await claimDocuments(limit, onlyStatus, source);
    summary.selected = documents.length;

    const results = await runWithConcurrency(documents, concurrency, async (doc) => {
      try {
        const result = await processOneDocument(doc, { maxFileMb });
        summary.downloaded += 1;
        summary.processed += 1;
        return result;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);

        await markFailed({
          documentId: doc?.id,
          errorMessage: message
        });

        summary.failed += 1;
        summary.processed += 1;

        const errorItem = {
          document_id: doc?.id || null,
          secop_document_id: doc?.secop_document_id || null,
          error: message
        };

        summary.errors.push(errorItem);
        return {
          ok: false,
          ...errorItem
        };
      }
    });

    summary.results = results.map((item) =>
      item.status === "fulfilled" ? item.value : { ok: false, error: item.reason }
    );

    return json(res, 200, summary);
  } catch (error) {
    return json(res, 500, {
      ...summary,
      ok: false,
      stage: "process_pending_documents",
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

const port = Number(process.env.PORT || 3000);

app.listen(port, "0.0.0.0", () => {
  console.log(`radar-documents-worker ${WORKER_VERSION} listening on 0.0.0.0:${port}`);
});
