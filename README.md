## Outlook-PST-Data-Generator

PST Extraction • Metadata Aggregation • Synthetic Email Generation • Flexible PST Re‑Packaging

---

## 1. Overview

Outlook-PST-Data-Generator is a three-component toolkit to:
1. **Extract** structured metadata from Outlook PST archives
2. **Merge & augment** extracted metadata with optional synthetic generation (datagen)
3. **Re-package** metadata back into various PST layouts (convert)

Each module can run standalone or be chained together. Docker orchestration is fully supported.

**Pipeline Flow:**
PST Files (`extract/data`) → `extract.py` parses & exports CSV → `metadata/<account>/emails_*.csv` → `datagen.py` merges & augments → `convert` (.NET) emits PST bundles (single / per-account / per-folder / per-message).

---

## 2. Table of Contents
1. Overview  
2. Directory Structure  
3. Quick Start  
4. Extract Service (Python / pypff)  
5. Datagen Service (Python)  
6. Convert Service (.NET 7/8 + Aspose.Email)  
7. Docker Usage  
8. Environment Variables  
9. End-to-End Example  
10. Development Notes  
11. Possible Extensions  
12. License Note  

---

## 3. Architecture Overview

Each stage can run independently; output directories are shared to create a processing chain.

---

## 4. Directory Structure
```
extract_service/
  docker-compose.yml
  README.md
  extract/          # PST analysis & metadata extraction (Python)
    data/           # Input .pst files
    metadata/       # Output (emails_*.csv + logs + attachments)
  datagen/          # Metadata merging + synthetic data generation
    output/         # merged_emails_*.csv + stats_*.json
  convert/          # .NET conversion (csv -> various PST packages)
    output/         # Generated PST files
```

---

## 5. Quick Start

To see the example workflow without installing local dependencies:

```bash
# 1) Extract: generate metadata (in extract directory)
cd extract
python extract.py

# 2) Datagen: merge metadata
cd ../datagen
python datagen.py -m ../extract/metadata -o ./output --synthesize 200

# 3) Convert: generate different PSTs (single PST example)
cd ../convert
dotnet run -- \
  --metadata ../extract/metadata \
  --out ./output \
  --single-pst
```

With Docker Compose (customize baseline scenario as needed):
```bash
docker compose up --build
```

---

## 6. Extract Service
Location: `extract/extract.py`

**Purpose:** Finds all `.pst` files under `extract/data`, and for each one:
* Traverses folder tree
* Extracts emails (core attributes + first 1000 characters of body)
* Saves attachments under `metadata/attachments/<email_id>/`
* Generates `emails_<timestamp>.csv` and JSON summary + log

**Usage:**
```bash
cd extract
python extract.py
```

**Output Example** (`metadata/<account>/emails_YYYYMMDD_HHMMSS.csv`):
Columns: `id,folder,subject,sender_name,sender_email,delivery_time,size,attachments_count`

**Notes:**
* Requires `pypff`. Installation: `pip install pypff` or `conda install -c conda-forge pypff`
* If you get errors on macOS, pay attention to libyal dependencies.

---

## 7. Datagen Service
Location: `datagen/datagen.py`

**Tasks:**
1. Finds the most recent `emails_*.csv` file in each account folder under `metadata/`.
2. Merges them to create `merged_emails_<timestamp>.csv`.
3. Optionally adds synthetic records (`--synthesize N`).
4. Writes summary statistics to `stats_<timestamp>.json`.
5. Optionally generates new demo accounts & CSV (`--make-accounts`).

**Arguments:**
```bash
python datagen.py \
  -m ../extract/metadata \
  -o ./output \
  --synthesize 200 \
  --make-accounts 3 \
  --emails-per-account 50 \
  --inbox-only
```

**Important Options:**
* `--synthesize <n>` : Number of synthetic records
* `--make-accounts <n>` : Create demo accounts
* `--emails-per-account` : Email count per new account
* `--inbox-only` : Use single folder (Inbox) only
* `-v` : Detailed logging

**Faker Usage:** `pip install faker` (falls back to simple mode if not available).

---

## 8. Convert Service (.NET)
Location: `convert/Program.cs`

**Input:** `metadata/<account>/emails_*.csv` (latest for each account)

**Output:** PST files (depending on selected mode) under `convert/output`.

**Usage** (with .NET SDK installed):
```bash
cd convert
dotnet run -- --metadata ../extract/metadata --out ./output --single-pst
```

**Supported PST Generation Modes** (argument combinations):
* `--single-pst` : All accounts in one PST (accounts as subfolders)
* `--per-message` (default) : Separate PST for each email
* `--per-folder` : One PST per (account + folder)
* `--per-account` : One PST per account (preserves internal folder structure)

**Other Arguments:**
* `--metadata, -m <dir>` : Metadata root directory (default: `/data/metadata` inside container)
* `--out, -o <dir>` : Output directory
* `--limit <N>` : Maximum emails (global limit)
* `--skip-attachments` : (Currently adds dummy header to body, future enhancement)
* `--format pst` : Currently only `pst`

**Aspose License (optional):**
If `ASPOSE_EMAIL_LICENSE_PATH` environment variable points to a valid license file, it will be loaded; otherwise runs in Evaluation mode or creates fallback stub.

**Example:**
```bash
ASPOSE_EMAIL_LICENSE_PATH=/licenses/Aspose.Email.lic \
dotnet run -- --metadata ../extract/metadata --out ./output --per-account --limit 500
```

---

## 9. Docker Usage

`docker-compose.yml` (summary): Configure to run each service in separate images (customize file for your scenario). Typical approach:
1. Extract container: Takes `extract/data` folder as volume, writes output to `extract/metadata`.
2. Datagen container: Reads `extract/metadata`, produces `datagen/output`.
3. Convert container: Reads `extract/metadata`, generates `convert/output` PST files.

**Manual build & run example (single service):**
```bash
# Extract
cd extract
docker build -t extract-svc .
docker run --rm -v "$PWD/data:/app/data" -v "$PWD/metadata:/app/metadata" extract-svc

# Datagen
cd ../datagen
docker build -t datagen-svc .
docker run --rm -v "$(realpath ../extract/metadata):/data/metadata" -v "$PWD/output:/app/output" datagen-svc \
  python datagen.py -m /data/metadata -o ./output --synthesize 100

# Convert
cd ../convert
docker build -t convert-svc .
docker run --rm -e ASPOSE_EMAIL_LICENSE_PATH=/license/Aspose.Email.lic \
  -v "$(realpath ../extract/metadata):/data/metadata" \
  -v "$PWD/output:/data/converted" convert-svc \
  --metadata /data/metadata --out /data/converted --single-pst
```

---

## 10. Environment Variables
| Variable | Description | Example |
|----------|-------------|---------|
| `ASPOSE_EMAIL_LICENSE_PATH` | Path to Aspose.Email license file (inside container) | `/license/Aspose.Email.lic` |
| `METADATA_DIR` (datagen) | Default metadata path for datagen | `../extract/metadata` |

---

## 11. End-to-End Example Flow
```bash
# 1) Place PST files in data/ folder
ls extract/data/*.pst

# 2) Extract
python extract/extract.py

# 3) Merge + Synthetic
python datagen/datagen.py -m extract/metadata -o datagen/output --synthesize 150

# 4) Generate PST (account-based)
dotnet run --project convert/Convert.csproj -- \
  --metadata extract/metadata --out convert/output --per-account
```

---

## 12. Development Tips
* **Performance:** Threading or queuing can be added for large PST files.
* **Memory:** `extract` currently keeps full lists in RAM; stream-based writing is possible.
* **Additional Processing:** MIME analysis, body normalization, full-text indexing (Whoosh/Elastic) can be integrated.
* **Convert:** Currently simple body; add enrichment from JSON metadata to store original body/plain/html.

---

## 13. Possible Extensions
| Area | Idea |
|------|------|
| Extract | Chunked CSV writing, multi-process |
| Datagen | Read folder distributions from config file |
| Convert | EML / MBOX output modes, ZIP packaging |
| Monitoring | Prometheus metrics |
| Interface | Simple web dashboard (FastAPI + Vue) |

---

## 14. License Note
Code may be under MIT / (to be determined) license; add license file. Aspose.Email may require commercial license – license responsibility is yours.

---

## 15. Quick Reference
```bash
# Datagen merge only
python datagen/datagen.py -m extract/metadata -o datagen/output

# Single PST
dotnet run --project convert/Convert.csproj -- --metadata extract/metadata --out convert/output --single-pst

# One PST per email
dotnet run --project convert/Convert.csproj -- --metadata extract/metadata --out convert/output --per-message --limit 50
```

