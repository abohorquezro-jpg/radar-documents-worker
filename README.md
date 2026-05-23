# Radar Documents Worker — Fixed Deploy Structure

Esta versión evita el error:

```text
Cannot find module '/app/src/index.js'
```

porque el entrypoint principal ahora está en la raíz:

```text
index.js
```

También se incluye una copia en:

```text
src/index.js
```

## Estructura correcta del repo

```text
package.json
index.js
railway.json
README.md
.env.example
.gitignore
src/index.js
```

## Railway variables

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

## Test

```bash
curl https://TU-DOMINIO.up.railway.app/health
```


## Fix incluido en esta versión

- Descarga de archivos SECOP con headers tipo navegador para mitigar HTTP 403.
- Guarda en `error_message` el status HTTP y una vista previa del body cuando la descarga falla.
- Mantiene el flujo existente: claim en Edge Function, signed upload y complete/fail por API interna.
