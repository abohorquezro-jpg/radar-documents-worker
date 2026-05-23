# Radar Documents Worker — Railway Production

Worker para procesar documentos SECOP pesados desde Railway.

## Arquitectura

```text
n8n
  -> Railway documents worker
      -> Supabase Edge Function secop-documents-api
          -> Supabase DB / Storage con service role interno
```

Railway hace el trabajo pesado:

- Reclama documentos pendientes.
- Descarga PDFs/DOCX desde SECOP.
- Sube a Supabase Storage con signed upload URL.
- Reporta success/fail a la Edge Function.

La Edge Function hace el trabajo sensible:

- Usa service role internamente.
- Reclama jobs de forma segura.
- Crea signed upload URLs.
- Actualiza `secop_opportunity_documents`.
- Actualiza contadores de `secop_viable_opportunities`.

## Railway variables

Configurar en Railway → Service → Variables:

```env
INTERNAL_API_SECRET=el_mismo_secret_de_n8n_y_la_edge_function
DOCUMENTS_API_URL=https://infxodoiupqivhgzsgza.supabase.co/functions/v1/secop-documents-api
SUPABASE_STORAGE_BUCKET=secop-documents

DEFAULT_LIMIT=20
MAX_LIMIT=100
CONCURRENCY=3
MAX_FILE_MB=80
DOWNLOAD_TIMEOUT_SECONDS=180
UPLOAD_TIMEOUT_SECONDS=180
CLAIM_ONLY_STATUS=queued
```

No necesitas en Railway:

```env
SUPABASE_SERVICE_ROLE_KEY
EXTERNAL_SUPABASE_SERVICE_ROLE_KEY
```

## Endpoints

### GET /health

Valida que el worker está vivo.

### POST /secop-documents/process-pending

Headers:

```text
Content-Type: application/json
x-internal-secret: TU_INTERNAL_SECRET_REAL
```

Body recomendado desde n8n:

```json
{
  "source": "n8n_after_save_documents_metadata",
  "limit": 20,
  "only_status": "queued",
  "concurrency": 3,
  "max_file_mb": 80
}
```

## Edge Function requerida

`secop-documents-api` debe soportar estas acciones:

1. `claim_documents_for_worker`
2. `create_signed_upload_url`
3. `complete_document_upload`
4. `fail_document_upload`

## Prompt para Lovable

```text
Actualizar la Edge Function secop-documents-api para que Railway procese documentos pesados sin exponer service role.

Seguridad:
- verify_jwt = false.
- Validar header x-internal-secret contra Deno.env.get("INTERNAL_API_SECRET").
- Usar internamente SUPABASE_SERVICE_ROLE_KEY o EXTERNAL_SUPABASE_SERVICE_ROLE_KEY.
- No devolver ni loguear secrets.

Acciones:

1) claim_documents_for_worker
Body:
{
  "action": "claim_documents_for_worker",
  "limit": 20,
  "only_status": "queued"
}

Debe reclamar de forma atómica documentos de public.secop_opportunity_documents:
download_status = only_status o 'queued'
source_download_url no null
order by created_at asc
limit máximo 100
marcar como processing
retornar documents con:
id, viable_opportunity_id, secop_process_id, secop_document_id, file_name, file_extension, source_download_url

Idealmente usar una RPC SQL con FOR UPDATE SKIP LOCKED para evitar duplicados.

2) create_signed_upload_url
Body:
{
  "action": "create_signed_upload_url",
  "document_id": "...",
  "storage_path": "...",
  "content_type": "application/pdf",
  "content_length": 12345,
  "bucket": "secop-documents"
}

Debe crear signed upload URL para bucket secop-documents y retornar:
{
  "ok": true,
  "signed_upload_url": "...",
  "storage_path": "...",
  "upload_method": "PUT"
}

3) complete_document_upload
Body:
{
  "action": "complete_document_upload",
  "document_id": "...",
  "storage_path": "...",
  "storage_bucket": "secop-documents",
  "file_size_bytes": 12345,
  "content_type": "application/pdf",
  "status": "downloaded"
}

Debe actualizar public.secop_opportunity_documents:
download_status='downloaded',
storage_bucket,
storage_path,
file_size_bytes,
content_type,
error_message=null,
updated_at=now()

Y actualizar contadores/status de public.secop_viable_opportunities.

4) fail_document_upload
Body:
{
  "action": "fail_document_upload",
  "document_id": "...",
  "error_message": "..."
}

Debe actualizar:
download_status='failed',
error_message,
updated_at=now()

Y actualizar contadores/status de public.secop_viable_opportunities.
```

## SQL recomendado para claim atómico

```sql
create or replace function public.claim_secop_documents_for_worker(p_limit int default 20)
returns table (
  id uuid,
  viable_opportunity_id uuid,
  secop_process_id text,
  secop_document_id text,
  file_name text,
  file_extension text,
  source_download_url text
)
language sql
security definer
as $$
  with claimed as (
    select d.id
    from public.secop_opportunity_documents d
    where d.download_status = 'queued'
      and d.source_download_url is not null
    order by d.created_at asc
    limit least(greatest(p_limit, 1), 100)
    for update skip locked
  ),
  updated as (
    update public.secop_opportunity_documents d
    set download_status = 'processing',
        updated_at = now()
    from claimed
    where d.id = claimed.id
    returning
      d.id,
      d.viable_opportunity_id,
      d.secop_process_id,
      d.secop_document_id,
      d.file_name,
      d.file_extension,
      d.source_download_url
  )
  select * from updated;
$$;
```

## Prueba local

```bash
npm install
npm run dev
```

## Prueba Railway

```bash
curl https://TU-DOMINIO.up.railway.app/health

curl -X POST https://TU-DOMINIO.up.railway.app/secop-documents/process-pending \
  -H "Content-Type: application/json" \
  -H "x-internal-secret: TU_INTERNAL_SECRET_REAL" \
  -d '{"source":"manual","limit":5,"only_status":"queued","concurrency":2}'
```
