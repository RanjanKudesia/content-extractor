# content-extractor

FastAPI microservice for extracting structured content from uploaded documents.

## Features

- Supports: docx, pdf, md, html, pptx, txt
- Single extraction endpoint with automatic extension-based pipeline routing
- JSON extraction for all supported formats
- Optional media extraction toggle across all supported formats
- Optional media storage mode: S3-backed or inline base64
- Uploads original file and extracted media to S3-compatible object storage
- Persists extracted JSON payload in S3 and stores only references in MongoDB
- Returns upload metadata plus extracted content version identifiers in the API response

## Run

```bash
cd content-extractor
pip install -r requirements.txt
# create .env from .env.example and fill values
uvicorn main:app --host 127.0.0.1 --port 8004
```

## Endpoints

- `GET /health`
- `POST /extract-content`
- `GET /content`

## API Usage

### Unified extraction endpoint

The API automatically selects the extraction pipeline by uploaded file extension.

Supported extensions:
- `docx`, `dox`
- `pdf`
- `md`
- `txt`
- `html`, `htm`
- `pptx`, `ppt`

Output format rules:
- Extraction output is always JSON.
- Media behavior is controlled by `extract_media` and `store_media`.

Storage behavior:
- Original uploaded file is stored in S3-compatible object storage.
- Extracted payload is always JSON.
- `extract_media=true` includes media objects in the extracted payload.
- `extract_media=false` skips media extraction entirely.
- `store_media=true` uploads extracted media to S3 and writes `s3_key` into media objects inside the extracted payload.
- `store_media=false` keeps extracted media inline as base64 inside the payload.
- The full extracted payload is stored in S3 and MongoDB only keeps references and metadata.
- The service does not persist extracted payloads, extracted media, or PDF conversion artifacts on local disk.
- API response includes `upload_id`, `uploaded_file_s3_key`, and `content_versions`.

### Example: JSON extraction

```bash
curl -X POST http://127.0.0.1:8004/extract-content \
  -F "file=@/absolute/path/sample.docx" \
  -F "user_id=test-user" \
  -F "extract_media=true" \
  -F "store_media=true"
```

### Example response fields (excerpt)

```json
{
  "upload_id": "67f02c2ef1a2b4d9c4f0a123",
  "uploaded_file_s3_key": "content-extractor/uploads/abc123/abc123.docx",
  "extension": "docx",
  "extract_media": true,
  "store_media": true,
  "content_versions": [
    {
      "content_id": "67f02c2ef1a2b4d9c4f0a456",
      "version": 0
    }
  ]
}
```

### Get extracted content

`GET /content` supports two response modes via the query parameter `output_format`:

- `output_format=json` (default): returns inline extracted JSON payload in the `data` field.
- `output_format=file`: returns a presigned URL in `file_download_url` pointing to the stored extracted JSON file in S3.

Example: inline JSON payload

```bash
curl "http://127.0.0.1:8004/content?content_id=<content_id>&version=0&output_format=json"
```

Example: presigned file URL

```bash
curl "http://127.0.0.1:8004/content?content_id=<content_id>&version=0&output_format=file"
```

## Required Environment Variables

MongoDB:
- `MONGODB_URI` (must be full URI, e.g. `mongodb://...` or `mongodb+srv://...`)
- `MONGODB_DATABASE` (default: `content_extractor`)
- `MONGODB_COLLECTION` (default: `extractions`)

S3-compatible storage:
- `S3_ENDPOINT_URL`
- `S3_ACCESS_KEY_ID`
- `S3_SECRET_ACCESS_KEY`
- `S3_REGION` (default: `us-east-1`)
- `S3_BUCKET_NAME`
- `S3_KEY_PREFIX` (default: `content-extractor`)
- `S3_ADDRESSING_STYLE` (default: `path`)
- `S3_SIGNATURE_VERSION` (default: `s3v4`)
- `S3_SESSION_TOKEN` (optional)

Logging:
- `LOG_LEVEL` (default: `INFO`)

## Troubleshooting

- `AccessDenied` on S3 upload usually means one of these is wrong: bucket name, endpoint URL, access key pair, or bucket write permissions.
- Verify your bucket policy allows `PutObject` for your access key.
- If using Railway or another S3-compatible provider, keep `S3_ADDRESSING_STYLE=path` unless your provider requires virtual host style.

## Notes

- The endpoint validates file extension and routes to the matching pipeline.
- For PDF files, the service uses the default pdf-to-docx extraction flow.
- `store_media=false` is useful when downstream consumers need the extracted payload to remain self-contained.
