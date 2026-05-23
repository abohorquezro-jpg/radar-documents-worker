# Radar Documents Worker Pipeline

Railway worker para SECOP II documentos.

## Rutas

- `GET /health`
- `POST /secop-documents/process-pending`: procesa documentos ya en `queued`.
- `POST /secop-documents/process-pipeline`: lista oportunidades viables, consulta documentos en datos.gov.co por lotes, guarda metadata y descarga/sube archivos.

## Variables requeridas en Railway

```env
DOCUMENTS_API_URL=https://infxodoiupqivhgzsgza.supabase.co/functions/v1/secop-documents-api
INTERNAL_API_SECRET=...
SUPABASE_STORAGE_BUCKET=secop-documents
SECOP_DOWNLOAD_STRATEGY=fetch_then_browser
```

## Payload recomendado

```json
{
  "source": "n8n_daily_after_ingest",
  "mode": "metadata_and_download",
  "opportunity_limit": 2500,
  "batch_size": 50,
  "download_limit": 100,
  "metadata_concurrency": 3,
  "download_concurrency": 2,
  "max_file_mb": 30,
  "use_playwright_fallback": true
}
```
