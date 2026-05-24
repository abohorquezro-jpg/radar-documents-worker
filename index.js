import express from "express";
import { chromium } from "playwright";

const app = express();
app.use(express.json({ limit: "20mb" }));

const PORT = Number(process.env.PORT || 3000);

// Edge Function actual de documentos.
const DOCUMENTS_API_URL =
  process.env.DOCUMENTS_API_URL ||
  "https://infxodoiupqivhgzsgza.supabase.co/functions/v1/secop-documents-api";

// Secret interno usado por n8n -> Railway y Railway -> secop-documents-api.
const INTERNAL_API_SECRET = process.env.INTERNAL_API_SECRET || "";

const DEFAULT_BUCKET = process.env.SUPABASE_STORAGE_BUCKET || "secop-documents";

// Dataset de documentos SECOP II.
const SECOP_DATASET_URL =
  process.env.SECOP_DOCUMENTS_DATASET_URL ||
  "https://www.datos.gov.co/resource/dmgg-8hin.json";

// Dataset de oportunidades SECOP II.
const SECOP_OPPORTUNITIES_URL =
  process.env.SECOP_OPPORTUNITIES_URL ||
  "https://www.datos.gov.co/resource/p6dx-8zbt.json";

// Edge Function de ingest de oportunidades.
const INGEST_API_URL =
  process.env.INGEST_API_URL ||
  "https://infxodoiupqivhgzsgza.supabase.co/functions/v1/ingest-secop-opportunities";

// Tu función ingest normalmente valida x-n8n-secret.
// Si la migraste a x-internal-secret, cambia INGEST_SECRET_HEADER en Railway.
const INGEST_SECRET_HEADER = process.env.INGEST_SECRET_HEADER || "x-n8n-secret";
const INGEST_SECRET =
  process.env.N8N_INGEST_SECRET ||
  process.env.INGEST_SECRET ||
  INTERNAL_API_SECRET;

// Edge Function para encolar matching después del ingest.
const MATCHING_JOBS_API_URL =
  process.env.MATCHING_JOBS_API_URL ||
  "https://infxodoiupqivhgzsgza.supabase.co/functions/v1/enqueue-daily-matching-jobs";

const MATCHING_INTERNAL_SECRET =
  process.env.MATCHING_INTERNAL_SECRET ||
  INTERNAL_API_SECRET;

const SERVICE_VERSION = "3.0.0-full-daily-pipeline";

if (!INTERNAL_API_SECRET) {
  console.warn("[WARN] Missing INTERNAL_API_SECRET. Requests with x-internal-secret will fail.");
}
if (!INGEST_SECRET) {
  console.warn("[WARN] Missing INGEST_SECRET/N8N_INGEST_SECRET. Ingest may fail.");
}

const pipelineRuns = new Map();

function intValue(value, fallback, min = 1, max = 10000) {
  const n = Number(value ?? fallback);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(Math.trunc(n), min), max);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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
  try {
    return await fn(controller.signal);
  } finally {
    clearTimeout(timer);
  }
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

  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }

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
      try {
        results[i] = await worker(items[i], i);
      } catch (error) {
        results[i] = {
          ok: false,
          error: error instanceof Error ? error.message : String(error)
        };
      }
    }
  });

  await Promise.all(runners);
  return results;
}

function escapeSoql(value) {
  return String(value || "").trim().replace(/'/g, "''");
}

function first(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined && String(value).trim() !== "") {
      return String(value).trim();
    }
  }
  return null;
}

function toNumberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(String(value).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : null;
}

function normalizeProcessUrl(value) {
  if (!value) return null;

  let url = typeof value === "object" && value.url ? value.url : value;
  url = String(url).trim();

  if (!url || url.includes("DEMO-000")) return null;
  if (url.startsWith("http://") || url.startsWith("https://")) return url;
  if (url.startsWith("/Public/")) return `https://community.secop.gov.co${url}`;

  return null;
}

function normalizeOpportunityRow(r) {
  const external_id = first(r.id_del_proceso, r.referencia_del_proceso);
  const secop_process_id = first(r.id_del_proceso, r.referencia_del_proceso);

  if (!external_id) return null;

  return {
    external_id,
    secop_process_id,
    portfolio_id: first(r.id_del_portafolio),
    title: first(r.nombre_del_procedimiento),
    description: first(r.descripci_n_del_procedimiento),
    entity_name: first(r.entidad),
    entity_nit: first(r.nit_entidad),
    department: first(r.departamento_entidad),
    city: first(r.ciudad_entidad),
    phase: first(r.fase),
    publication_date: r.fecha_de_publicacion_del || null,
    closing_date: r.fecha_de_recepcion_de || null,
    base_price: toNumberOrNull(r.precio_base),
    contracting_mode: first(r.modalidad_de_contratacion),
    mode_justification: first(r.justificaci_n_modalidad_de),
    procedure_status: first(r.estado_del_procedimiento),
    opening_status: first(r.estado_de_apertura_del_proceso),
    category_code: first(r.codigo_principal_de_categoria),
    contract_type: first(r.tipo_de_contrato),
    contract_subtype: first(r.subtipo_de_contrato),
    process_url: normalizeProcessUrl(r.urlproceso)
  };
}

function isoDateOnly(date) {
  return date.toISOString().slice(0, 10);
}

async function fetchSecopOpportunitiesPage({ limit, offset, windowDays, signal }) {
  const now = new Date();
  const fromDate = new Date(now);
  fromDate.setDate(fromDate.getDate() - windowDays);

  const from = `${isoDateOnly(fromDate)}T00:00:00`;
  const to = `${isoDateOnly(new Date(now.getTime() + 24 * 60 * 60 * 1000))}T00:00:00`;

  const query = [
    "SELECT `entidad`, `nit_entidad`, `departamento_entidad`, `ciudad_entidad`,",
    "`id_del_proceso`, `referencia_del_proceso`, `id_del_portafolio`,",
    "`nombre_del_procedimiento`, `descripci_n_del_procedimiento`, `fase`,",
    "`fecha_de_publicacion_del`, `fecha_de_recepcion_de`, `precio_base`,",
    "`modalidad_de_contratacion`, `justificaci_n_modalidad_de`,",
    "`estado_del_procedimiento`, `estado_de_apertura_del_proceso`,",
    "`codigo_principal_de_categoria`, `tipo_de_contrato`, `subtipo_de_contrato`, `urlproceso`",
    `WHERE \`fecha_de_publicacion_del\` >= '${from}'::floating_timestamp`,
    `AND \`fecha_de_publicacion_del\` < '${to}'::floating_timestamp`,
    "AND caseless_one_of(`estado_de_apertura_del_proceso`, 'Abierto')",
    "AND caseless_one_of(`estado_del_procedimiento`, 'Publicado')",
    "AND `id_del_proceso` IS NOT NULL",
    "AND `entidad` IS NOT NULL",
    "AND `nombre_del_procedimiento` IS NOT NULL",
    "ORDER BY `fecha_de_publicacion_del` DESC",
    `LIMIT ${limit}`,
    `OFFSET ${offset}`
  ].join(" ");

  const url = `${SECOP_OPPORTUNITIES_URL}?${new URLSearchParams({ "$query": query }).toString()}`;

  const response = await fetch(url, {
    method: "GET",
    headers: { "Accept": "application/json" },
    signal
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(`SECOP opportunities fetch failed: HTTP ${response.status} offset=${offset} ${text.slice(0, 300)}`);
  }

  try {
    return text ? JSON.parse(text) : [];
  } catch {
    throw new Error(`SECOP opportunities returned invalid JSON offset=${offset}: ${text.slice(0, 300)}`);
  }
}

async function fetchAndNormalizeOpportunities(body = {}) {
  const opportunityLimit = intValue(body.opportunity_limit, process.env.OPPORTUNITY_LIMIT || 2500, 1, 10000);

  const opportunityBatchSize = intValue(
    body.opportunity_batch_size ?? body.ingest_batch_size ?? body.fetch_batch_size ?? body.batch_size,
    process.env.OPPORTUNITY_BATCH_SIZE || 500,
    1,
    1000
  );

  const windowDays = intValue(body.window_days, process.env.WINDOW_DAYS || 7, 1, 365);

  const pageDelayMs = intValue(
    body.secop_page_delay_ms ?? body.sec_page_delay_ms,
    process.env.SECOP_PAGE_DELAY_MS || 800,
    0,
    30000
  );

  const pages = Math.ceil(opportunityLimit / opportunityBatchSize);
  const allRows = [];
  const errors = [];

  for (let i = 0; i < pages; i++) {
    const offset = i * opportunityBatchSize;
    const limit = Math.min(opportunityBatchSize, opportunityLimit - offset);

    try {
      const rows = await withTimeout(45000, signal => fetchSecopOpportunitiesPage({
        limit,
        offset,
        windowDays,
        signal
      }));

      console.log(`[opportunities] page=${i + 1}/${pages} offset=${offset} rows=${rows.length}`);
      allRows.push(...rows);

      if (pageDelayMs > 0 && i < pages - 1) await sleep(pageDelayMs);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[opportunities] page error page=${i + 1}/${pages}: ${message}`);
      errors.push({ page: i + 1, offset, error: message });
    }
  }

  const opportunities = allRows.map(normalizeOpportunityRow).filter(Boolean);

  return {
    rows_fetched: allRows.length,
    normalized: opportunities.length,
    opportunity_limit: opportunityLimit,
    opportunity_batch_size: opportunityBatchSize,
    window_days: windowDays,
    pages_requested: pages,
    page_errors: errors,
    opportunities
  };
}

async function ingestOpportunities(opportunities, body = {}) {
  const ingestChunkSize = intValue(body.ingest_chunk_size, process.env.INGEST_CHUNK_SIZE || 500, 1, 1000);
  const chunks = chunkArray(opportunities, ingestChunkSize);
  const results = [];

  let upserted = 0;
  let failed_chunks = 0;

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];

    const response = await withTimeout(90000, async signal => fetch(INGEST_API_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        [INGEST_SECRET_HEADER]: INGEST_SECRET
      },
      body: JSON.stringify({ opportunities: chunk }),
      signal
    }));

    const text = await response.text();
    let data;

    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = { raw: text };
    }

    const ok = response.ok && data.ok !== false;

    if (!ok) failed_chunks += 1;
    upserted += Number(data.upserted || 0);

    const result = {
      chunk_number: i + 1,
      sent: chunk.length,
      http_status: response.status,
      ok,
      upserted: Number(data.upserted || 0),
      error: ok ? null : (data.error || data.message || text.slice(0, 300)),
      body: data
    };

    console.log(`[ingest] chunk=${i + 1}/${chunks.length} sent=${chunk.length} status=${response.status} ok=${ok} upserted=${result.upserted}`);
    results.push(result);
  }

  return {
    batches: chunks.length,
    successful_batches: results.filter(r => r.ok).length,
    failed_batches: failed_chunks,
    sent: opportunities.length,
    upserted,
    results
  };
}

async function enqueueMatchingJobs(ingestSummary, body = {}) {
  if (body.enqueue_matching === false) {
    return { skipped: true, reason: "enqueue_matching=false" };
  }

  const payload = {
    source: body.matching_source || "railway_full_daily_after_ingest",
    mode: body.matching_mode || "new_only",
    limit_value: intValue(body.matching_limit_value, process.env.MATCHING_LIMIT_VALUE || 1000, 1, 10000),
    only_companies_with_profile: body.only_companies_with_profile ?? true,
    avoid_active_duplicates: body.avoid_active_duplicates ?? true,
    ingest_summary: ingestSummary,
    requested_at: new Date().toISOString()
  };

  const response = await withTimeout(60000, async signal => fetch(MATCHING_JOBS_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-internal-secret": MATCHING_INTERNAL_SECRET
    },
    body: JSON.stringify(payload),
    signal
  }));

  const text = await response.text();
  let data;

  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }

  return {
    ok: response.ok && data.ok !== false,
    http_status: response.status,
    payload,
    body: data
  };
}

async function fetchSecopDocumentsBatch(viableRows, signal) {
  const ids = viableRows.map(v => escapeSoql(v.secop_process_id)).filter(Boolean);
  if (!ids.length) return [];

  const inList = ids.map(id => `'${id}'`).join(",");
  const query = `SELECT id_documento,proceso,nombre_archivo,tamanno_archivo,extensi_n,descripci_n,fecha_carga,entidad,nit_entidad,url_descarga_documento WHERE proceso in(${inList}) LIMIT 50000`;

  const params = new URLSearchParams({ "$query": query });
  const url = `${SECOP_DATASET_URL}?${params.toString()}`;

  const response = await fetch(url, {
    method: "GET",
    headers: { "Accept": "application/json" },
    signal
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(`datos.gov.co documents batch failed: HTTP ${response.status} ${text.slice(0, 300)}`);
  }

  try {
    return text ? JSON.parse(text) : [];
  } catch {
    throw new Error(`datos.gov.co documents batch returned invalid JSON: ${text.slice(0, 300)}`);
  }
}

function getDownloadUrl(value) {
  if (!value) return null;
  if (typeof value === "object") return value.url || value.uri || null;
  const text = String(value).trim();
  return text || null;
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
    const response = await fetch(url, {
      method: "GET",
      redirect: "follow",
      headers: secopHeaders(),
      signal
    });

    const bodyTextOnError = async () => {
      try {
        return (await response.text()).slice(0, 300);
      } catch {
        return "";
      }
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

    await page.goto("https://community.secop.gov.co/", {
      waitUntil: "domcontentloaded",
      timeout: Math.min(timeoutMs, 60000)
    }).catch(() => null);

    const response = await context.request.get(url, {
      timeout: timeoutMs,
      headers: secopHeaders()
    });

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

    if (!usePlaywrightFallback || status !== 403) {
      throw new Error(`Download failed: ${error.message}`);
    }

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

    if (!response.ok) {
      throw new Error(`Signed upload failed: HTTP ${response.status} ${text.slice(0, 300)}`);
    }

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

    if (!signedUrl) {
      throw new Error("Missing signed upload URL from Edge Function");
    }

    await uploadToSignedUrl(signedUrl, downloaded.buffer, contentType, uploadTimeoutMs);

    await callDocumentsApi("complete_document_upload", {
      document_id: doc.id,
      storage_bucket: DEFAULT_BUCKET,
      storage_path: storagePath,
      file_size_bytes: downloaded.buffer.length,
      content_type: contentType
    });

    return {
      ok: true,
      document_id: doc.id,
      secop_document_id: doc.secop_document_id,
      storage_path: storagePath
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);

    await callDocumentsApi("fail_document_upload", {
      document_id: doc.id,
      error_message: message
    }).catch(() => null);

    return {
      ok: false,
      document_id: doc.id,
      secop_document_id: doc.secop_document_id,
      error: message
    };
  }
}

async function processPendingDocuments(body = {}) {
  const limit = intValue(body.limit, process.env.DOWNLOAD_LIMIT || 20, 1, 500);
  const concurrency = intValue(body.concurrency ?? body.download_concurrency, process.env.DOWNLOAD_CONCURRENCY || 1, 1, 10);
  const onlyStatus = body.only_status || process.env.CLAIM_ONLY_STATUS || "queued";

  const claim = await callDocumentsApi("claim_documents_for_worker", {
    limit,
    only_status: onlyStatus
  });

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

// Esta es tu lógica original de documentos. Se conserva para no romper el flujo actual.
async function processPipeline(body = {}) {
  const opportunityLimit = intValue(body.opportunity_limit, process.env.OPPORTUNITY_LIMIT || 2500, 1, 2500);
  const batchSize = intValue(body.documents_batch_size ?? body.batch_size, process.env.BATCH_SIZE || 50, 1, 100);
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
      return {
        ok: true,
        dry_run: true,
        batch_number: idx + 1,
        viables: batch.length,
        docs_found: docs.length,
        items: items.length
      };
    }

    const saved = await callDocumentsApi("save_documents_metadata_batch", {
      source,
      batch_number: idx + 1,
      items
    });

    return {
      ok: true,
      batch_number: idx + 1,
      viables: batch.length,
      docs_found: docs.length,
      saved
    };
  });

  const metadataDocsFound = batchResults.reduce((acc, r) => acc + Number(r?.docs_found || 0), 0);
  const metadataSaved = batchResults.reduce((acc, r) => acc + Number(r?.saved?.upserted || 0), 0);
  const metadataFailedBatches = batchResults.filter(r => r?.ok === false).length;

  let downloadResult = {
    selected: 0,
    processed: 0,
    downloaded: 0,
    failed: 0,
    results: []
  };

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

async function runFullDailyPipeline(body = {}) {
  const startedAt = new Date().toISOString();

  console.log("[full-daily] started", {
    opportunity_limit: body.opportunity_limit,
    window_days: body.window_days,
    mode: body.mode
  });

  const fetched = await fetchAndNormalizeOpportunities(body);

  const ingestSummary = {
    ok: fetched.page_errors.length === 0,
    stage: "railway_fetch_normalize",
    timestamp: new Date().toISOString(),
    rows_fetched: fetched.rows_fetched,
    normalized: fetched.normalized,
    opportunity_limit: fetched.opportunity_limit,
    opportunity_batch_size: fetched.opportunity_batch_size,
    window_days: fetched.window_days,
    pages_requested: fetched.pages_requested,
    page_errors: fetched.page_errors
  };

  let ingest = {
    batches: 0,
    successful_batches: 0,
    failed_batches: 0,
    sent: 0,
    upserted: 0,
    results: []
  };

  if (fetched.opportunities.length > 0 && body.skip_ingest !== true) {
    ingest = await ingestOpportunities(fetched.opportunities, body);
  }

  const finalIngestSummary = {
    ...ingestSummary,
    ok: fetched.page_errors.length === 0 && ingest.failed_batches === 0,
    ingest
  };

  const matching = await enqueueMatchingJobs(finalIngestSummary, body).catch(error => ({
    ok: false,
    error: error instanceof Error ? error.message : String(error)
  }));

  const documentsBody = {
    ...body,
    source: body.documents_source || "railway_full_daily_documents_pipeline",
    mode: "metadata_and_download",
    batch_size: body.documents_batch_size ?? process.env.DOCUMENTS_BATCH_SIZE ?? 50,
    opportunity_limit: body.documents_opportunity_limit ?? body.opportunity_limit ?? 2500,
    older_than_hours: body.older_than_hours ?? 0,
    quality_statuses: body.quality_statuses ?? ["actionable", "open_without_closing_date"],
    exclude_document_statuses: body.exclude_document_statuses ?? [
      "downloaded",
      "no_documents",
      "missing_process_id"
    ],
    dry_run: body.dry_run === true,
    use_playwright_fallback: body.use_playwright_fallback !== false
  };

  const documents = body.skip_documents === true
    ? { skipped: true, reason: "skip_documents=true" }
    : await processPipeline(documentsBody);

  const finishedAt = new Date().toISOString();

  return {
    ok: true,
    service: "radar-documents-worker",
    version: SERVICE_VERSION,
    mode: "full_daily",
    started_at: startedAt,
    finished_at: finishedAt,
    fetch_normalize: {
      rows_fetched: fetched.rows_fetched,
      normalized: fetched.normalized,
      opportunity_limit: fetched.opportunity_limit,
      opportunity_batch_size: fetched.opportunity_batch_size,
      window_days: fetched.window_days,
      pages_requested: fetched.pages_requested,
      page_errors: fetched.page_errors
    },
    ingest,
    matching,
    documents
  };
}

function isFullDailyRequest(body = {}) {
  return body.mode === "full_daily" ||
    body.mode === "async_full_pipeline" ||
    body.run_full_pipeline === true ||
    body.run_opportunity_ingest === true;
}

function startBackgroundPipeline(body = {}) {
  const runId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const startedAt = new Date().toISOString();

  pipelineRuns.set(runId, {
    run_id: runId,
    status: "running",
    started_at: startedAt,
    updated_at: startedAt,
    request: body
  });

  setImmediate(async () => {
    try {
      const result = await runFullDailyPipeline(body);

      pipelineRuns.set(runId, {
        run_id: runId,
        status: "completed",
        started_at: startedAt,
        updated_at: new Date().toISOString(),
        result
      });

      console.log(`[full-daily] completed run_id=${runId}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);

      pipelineRuns.set(runId, {
        run_id: runId,
        status: "failed",
        started_at: startedAt,
        updated_at: new Date().toISOString(),
        error: message,
        stack: error?.stack
      });

      console.error(`[full-daily] failed run_id=${runId}`, message, error?.stack);
    }
  });

  return runId;
}

function requireInternal(req, res, next) {
  const provided = req.headers["x-internal-secret"] || "";

  if (!INTERNAL_API_SECRET || provided !== INTERNAL_API_SECRET) {
    return res.status(401).json({
      ok: false,
      error: "Unauthorized: missing or invalid x-internal-secret"
    });
  }

  next();
}

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "radar-documents-worker",
    version: SERVICE_VERSION,
    has_documents_api_url: Boolean(DOCUMENTS_API_URL),
    has_internal_secret: Boolean(INTERNAL_API_SECRET),
    has_ingest_api_url: Boolean(INGEST_API_URL),
    has_ingest_secret: Boolean(INGEST_SECRET),
    has_matching_jobs_api_url: Boolean(MATCHING_JOBS_API_URL),
    routes: [
      "GET /health",
      "GET /secop-documents/pipeline-status/:runId",
      "POST /secop-documents/process-pending",
      "POST /secop-documents/process-pipeline",
      "POST /secop-documents/full-daily-pipeline",
      "POST /documents/backfill",
      "POST /documents/process-pending",
      "POST /documents/diagnostics"
    ]
  });
});

app.get("/secop-documents/pipeline-status/:runId", requireInternal, (req, res) => {
  const run = pipelineRuns.get(req.params.runId);

  if (!run) {
    return res.status(404).json({
      ok: false,
      error: "run_id not found"
    });
  }

  res.json({
    ok: true,
    ...run
  });
});

app.post("/secop-documents/process-pending", requireInternal, async (req, res) => {
  const started = Date.now();

  try {
    const result = await processPendingDocuments(req.body || {});

    res.json({
      ok: true,
      service: "radar-documents-worker",
      version: SERVICE_VERSION,
      mode: "process_pending",
      ...result,
      duration_ms: Date.now() - started,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      service: "radar-documents-worker",
      mode: "process_pending",
      error: error instanceof Error ? error.message : String(error),
      duration_ms: Date.now() - started,
      timestamp: new Date().toISOString()
    });
  }
});

app.post("/secop-documents/process-pipeline", requireInternal, async (req, res) => {
  const body = req.body || {};

  if (isFullDailyRequest(body)) {
    const runId = startBackgroundPipeline(body);

    return res.status(202).json({
      ok: true,
      service: "radar-documents-worker",
      version: SERVICE_VERSION,
      mode: "full_daily_async",
      message: "Pipeline iniciado: fetch SECOP + normalize + ingest + matching + documentos",
      run_id: runId,
      status_url: `/secop-documents/pipeline-status/${runId}`,
      started_at: new Date().toISOString(),
      config: {
        opportunity_limit: body.opportunity_limit ?? 2500,
        opportunity_batch_size: body.opportunity_batch_size ?? body.ingest_batch_size ?? body.fetch_batch_size ?? body.batch_size ?? 500,
        documents_batch_size: body.documents_batch_size ?? 50,
        window_days: body.window_days ?? 7,
        download_limit: body.download_limit ?? 100,
        metadata_concurrency: body.metadata_concurrency ?? 3,
        download_concurrency: body.download_concurrency ?? 1
      }
    });
  }

  const started = Date.now();

  try {
    const result = await processPipeline(body);

    res.json({
      ok: true,
      service: "radar-documents-worker",
      version: SERVICE_VERSION,
      mode: "metadata_and_download",
      ...result,
      duration_ms: Date.now() - started,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      service: "radar-documents-worker",
      version: SERVICE_VERSION,
      mode: "metadata_and_download",
      error: error instanceof Error ? error.message : String(error),
      duration_ms: Date.now() - started,
      timestamp: new Date().toISOString()
    });
  }
});

app.post("/secop-documents/full-daily-pipeline", requireInternal, async (req, res) => {
  const body = { ...(req.body || {}), mode: "full_daily" };
  const runId = startBackgroundPipeline(body);

  res.status(202).json({
    ok: true,
    service: "radar-documents-worker",
    version: SERVICE_VERSION,
    mode: "full_daily_async",
    message: "Pipeline iniciado: fetch SECOP + normalize + ingest + matching + documentos",
    run_id: runId,
    status_url: `/secop-documents/pipeline-status/${runId}`,
    started_at: new Date().toISOString()
  });
});

// ============================================================================
// n8n compatibility aliases
// Estas rutas corrigen el error:
// Cannot POST /documents/backfill
// ============================================================================

app.post("/documents/backfill", requireInternal, async (req, res) => {
  const started = Date.now();
  const body = req.body || {};

  try {
    const limit = intValue(
      body.limit ?? body.opportunity_limit,
      process.env.DOCUMENTS_BACKFILL_LIMIT || 20,
      1,
      2500
    );

    const batchSize = intValue(
      body.documents_batch_size ?? body.batch_size,
      Math.min(limit, 50),
      1,
      100
    );

    const metadataConcurrency = intValue(
      body.metadata_concurrency,
      process.env.METADATA_CONCURRENCY || 2,
      1,
      8
    );

    const source = body.source || "n8n_secop_documents_backfill";
    const dryRun = body.dry_run === true;

    const excludeDocumentStatuses =
      Array.isArray(body.exclude_document_statuses)
        ? body.exclude_document_statuses
        : body.reprocess_existing === true
          ? ["missing_process_id"]
          : ["downloaded", "no_documents", "missing_process_id"];

    console.log("[documents/backfill] started", {
      source,
      limit,
      batch_size: batchSize,
      metadata_concurrency: metadataConcurrency,
      dry_run: dryRun,
      exclude_document_statuses: excludeDocumentStatuses
    });

    const viableResp = await callDocumentsApi("list_viable_for_documents", {
      limit,
      older_than_hours: body.older_than_hours ?? 0,
      ignore_checked_at: body.ignore_checked_at === true,
      include_no_documents: body.include_no_documents === true,
      quality_statuses: body.quality_statuses ?? [
        "actionable",
        "open_without_closing_date"
      ],
      exclude_document_statuses: excludeDocumentStatuses
    });

    const viableRows = Array.isArray(viableResp.rows) ? viableResp.rows : [];
    const batches = chunkArray(viableRows, batchSize);

    console.log("[documents/backfill] viable rows selected", {
      selected: viableRows.length,
      batches: batches.length
    });

    const batchResults = await runLimited(
      batches,
      metadataConcurrency,
      async (batch, idx) => {
        const docs = await withTimeout(120000, signal =>
          fetchSecopDocumentsBatch(batch, signal)
        );

        const items = buildMetadataItems(batch, docs);

        const noDocuments = items.filter(
          item => Number(item.documents_found || 0) === 0
        ).length;

        if (dryRun) {
          return {
            ok: true,
            dry_run: true,
            batch_number: idx + 1,
            viables: batch.length,
            docs_found: docs.length,
            items: items.length,
            no_documents: noDocuments,
            preview: items.slice(0, 3)
          };
        }

        const saved = await callDocumentsApi("save_documents_metadata_batch", {
          source,
          batch_number: idx + 1,
          items
        });

        return {
          ok: true,
          batch_number: idx + 1,
          viables: batch.length,
          docs_found: docs.length,
          items: items.length,
          no_documents: noDocuments,
          saved
        };
      }
    );

    const processed = batchResults.reduce(
      (acc, r) => acc + Number(r?.viables || 0),
      0
    );

    const documentsFound = batchResults.reduce(
      (acc, r) => acc + Number(r?.docs_found || 0),
      0
    );

    const documentsInserted = batchResults.reduce(
      (acc, r) => acc + Number(r?.saved?.upserted || 0),
      0
    );

    const noDocuments = batchResults.reduce(
      (acc, r) => acc + Number(r?.no_documents || 0),
      0
    );

    const failedBatches = batchResults.filter(r => r?.ok === false).length;

    const response = {
      ok: failedBatches === 0,
      service: "radar-documents-worker",
      version: SERVICE_VERSION,
      action: "documents_backfill",
      mode: "metadata_only",
      dry_run: dryRun,
      claimed: viableRows.length,
      processed,
      metadata_batches: batches.length,
      metadata_failed_batches: failedBatches,
      documents_found: documentsFound,
      documents_inserted: documentsInserted,
      no_documents: noDocuments,
      duration_ms: Date.now() - started,
      timestamp: new Date().toISOString(),
      results: batchResults
    };

    console.log("[documents/backfill] complete", {
      claimed: response.claimed,
      processed: response.processed,
      documents_found: response.documents_found,
      documents_inserted: response.documents_inserted,
      no_documents: response.no_documents,
      failed_batches: response.metadata_failed_batches,
      duration_ms: response.duration_ms
    });

    return res.status(failedBatches > 0 ? 207 : 200).json(response);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);

    console.error("[documents/backfill] failed", {
      error: message,
      duration_ms: Date.now() - started
    });

    return res.status(500).json({
      ok: false,
      service: "radar-documents-worker",
      version: SERVICE_VERSION,
      action: "documents_backfill",
      error: message,
      duration_ms: Date.now() - started,
      timestamp: new Date().toISOString()
    });
  }
});

app.post("/documents/process-pending", requireInternal, async (req, res) => {
  const started = Date.now();

  try {
    const result = await processPendingDocuments(req.body || {});

    return res.json({
      ok: true,
      service: "radar-documents-worker",
      version: SERVICE_VERSION,
      action: "documents_process_pending",
      mode: "download_and_upload",
      ...result,
      duration_ms: Date.now() - started,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);

    console.error("[documents/process-pending] failed", {
      error: message,
      duration_ms: Date.now() - started
    });

    return res.status(500).json({
      ok: false,
      service: "radar-documents-worker",
      version: SERVICE_VERSION,
      action: "documents_process_pending",
      error: message,
      duration_ms: Date.now() - started,
      timestamp: new Date().toISOString()
    });
  }
});

app.post("/documents/diagnostics", requireInternal, async (req, res) => {
  try {
    const result = await callDocumentsApi("diagnostics", req.body || {});

    return res.json({
      ok: true,
      service: "radar-documents-worker",
      version: SERVICE_VERSION,
      action: "documents_diagnostics",
      diagnostics: result,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      service: "radar-documents-worker",
      version: SERVICE_VERSION,
      action: "documents_diagnostics",
      error: error instanceof Error ? error.message : String(error),
      timestamp: new Date().toISOString()
    });
  }
});

app.listen(PORT, () => {
  console.log(`radar-documents-worker ${SERVICE_VERSION} listening on ${PORT}`);
});
