# content-extractor

FastAPI microservice for extracting structured content from uploaded documents.

## Features

- Supports: docx, pdf, md, html, pptx, txt
- Single extraction endpoint with automatic extension-based pipeline routing
- JSON extraction for all supported formats
- XML extraction only for docx and pptx
- Uploads original file and extracted media to S3-compatible object storage
- Persists full extracted payload (JSON/XML structure) in MongoDB
- Returns MongoDB record id in API response

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
- `output_format=json` works for every supported extension.
- `output_format=xml` is allowed only for docx and pptx uploads.
- If `xml` is requested for other file types, the API returns HTTP 400 with a clear error message.

Storage behavior:
- Original uploaded file is stored in S3-compatible object storage.
- Extracted media are uploaded to S3 and `s3_key` is written into media objects inside extracted payload.
- The full extracted payload is stored in MongoDB.
- The service does not persist extracted payloads, extracted media, or PDF conversion artifacts on local disk.
- API response includes `db_record_id` and `uploaded_file_s3_key`.

### Example: JSON extraction

```bash
curl -X POST http://127.0.0.1:8004/extract-content \
  -F "file=@/absolute/path/sample.docx" \
  -F "output_format=json"
```

### Example response fields (excerpt)

```json
{
  "db_record_id": "67f02c2ef1a2b4d9c4f0a123",
  "uploaded_file_s3_key": "content-extractor/uploads/abc123/abc123.docx",
  "output_file_path": "virtual://extracted/abc123.json",
  "extension": "docx",
  "output_format": "json"
}
```

### Example: XML extraction (DOCX/PPTX only)

```bash
curl -X POST http://127.0.0.1:8004/extract-content \
  -F "file=@/absolute/path/sample.pptx" \
  -F "output_format=xml"
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
- For most use cases, `output_format=json` is the recommended default.
