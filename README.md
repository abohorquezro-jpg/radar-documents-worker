# Radar Documents Worker

Worker HTTP para descargar documentos SECOP en estado `queued`, subirlos a Supabase Storage y actualizar estados en Supabase.

## Endpoints

- `GET /health`
- `POST /secop-documents/process-pending`

## Variables requeridas en Railway

```env
INTERNAL_API_SECRET=el_mismo_secret_que_usas_en_n8n
SUPABASE_URL=https://infxodoiupqivhgzsgza.supabase.co
SUPABASE_SERVICE_ROLE_KEY=tu_service_role_key
SUPABASE_STORAGE_BUCKET=secop-documents
```

## Body para n8n

```json
{
  "source": "n8n_after_save_documents_metadata",
  "limit": 50,
  "only_status": "queued"
}
```

## Headers para n8n

```text
Content-Type: application/json
x-internal-secret: TU_INTERNAL_SECRET_REAL
```

## Pruebas

```bash
curl https://TU-DOMINIO.up.railway.app/health

curl -X POST https://TU-DOMINIO.up.railway.app/secop-documents/process-pending \
  -H "Content-Type: application/json" \
  -H "x-internal-secret: TU_INTERNAL_SECRET_REAL" \
  -d '{"source":"manual","limit":5,"only_status":"queued"}'
```
