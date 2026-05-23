# Radar Documents Worker Pipeline - Fixed Entrypoint

Este ZIP incluye `index.js` en la raíz y también `src/index.js` para evitar el error:

`Cannot find module '/app/src/index.js'`

Railway debe usar Dockerfile y `npm start`.

Rutas:
- GET /health
- POST /secop-documents/process-pending
- POST /secop-documents/process-pipeline
