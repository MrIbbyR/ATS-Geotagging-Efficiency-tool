# Recruiter Workflow Optimisation

Automation script to optimise recruiter workflows for SmartRecruiters prospect screening.

This is a time-saving script to assist with geotagging of high volume prospects with location specific tech as well as the automation of key recruiter behaviours to tag inbound talent. Key features solve business problems such as missing location data, automation of key recruiter behaviours for efficient screening, and minimising click-ops to review talent.

⸻

## Key Features

- **Location extraction** – NER, regex, DOM, and OCR fallback for prospects with missing location data
- **Keyword search (Ctrl+F style)** – Flexible keyword input: prompt, CLI, or file; auto-expands abbreviations (R&D → research and development)
- **Semantic tagging** – When `semantic_tagger.py` is available
- **GDPR compliant workflow**

⸻

## Prerequisites

- Python 3.8+
- Chrome browser
- SmartRecruiters account

⸻

## Installation

### 1. Install dependencies

```bash
pip install -r requirements-macos.txt
```

### 2. Download spaCy model (for NER location extraction)

```bash
python -m spacy download en_core_web_sm
```

### 3. Set up Playwright

```bash
playwright install chromium
playwright install-deps
```

### 4. Optional: OCR for scanned PDFs

```bash
pip install paddleocr paddlepaddle
```

### 5. Optional: Semantic tagging (embedding-based)

```bash
pip install torch sentence-transformers numpy
```

⸻

## Quick Start

### 1. Launch Chrome with debugging

```bash
# macOS
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222 --user-data-dir="/tmp/sr-prospect" &

# Or
google-chrome-stable --remote-debugging-port=9222 --user-data-dir=/tmp/sr-prospect
```

### 2. Log in to SmartRecruiters

Open Chrome, log in, and navigate to the job’s **Applicants** tab.

### 3. Run the script

```bash
source .venv/bin/activate
python req.py
```

By default: email-only (no local file). You’ll be prompted for keywords, or press Enter for defaults.

⸻

## Configuration

### Email (default: email-only)

Create `email_credentials.env` (copy from `email_credentials.env.example`):

```
EMAIL_PROVIDER=gmail
EMAIL_USER=your-email@gmail.com
EMAIL_PASSWORD=your-app-password
EMAIL_TO=your-email@gmail.com
```

- **Gmail**: Use an [App Password](https://myaccount.google.com/apppasswords) (2FA required)
- **Outlook**: Basic Auth often disabled; Gmail recommended

### Keywords

| Method | Example |
|--------|---------|
| **Interactive** | Run `python req.py` → enter keywords when prompted |
| **CLI** | `python req.py --keywords "Python, ML, Chennai"` |
| **File** | `python req.py --keywords-file keywords_ml.txt` |
| **Env** | `KEYWORDS_FILE=keywords_ml.txt` in `email_credentials.env` |

Abbreviations auto-expand (e.g. R&D → research and development). Add custom expansions in `keyword_expansions.txt`.

### CLI options

```bash
python req.py                    # Email only, prompt for keywords
python req.py --save-local       # Also save report locally
python req.py --keywords "..."   # Inline keywords
python req.py --keywords-file X  # Load from file
```

⸻

## Tech Stack

### Core browser automation

- **asyncio** – async operations and concurrency
- **Playwright** – browser automation
- **Chrome DevTools Protocol (CDP)** – connect to existing Chrome instance
- **JavaScript injection** – scrolling and data extraction in page context

### Data processing

- **PyMuPDF (fitz)** – PDF text extraction from resumes
- **openpyxl** – Excel generation with formatting, tables, hyperlinks
- **PaddleOCR** (optional) – OCR for scanned/low-text PDFs
- **Regular expressions** – text parsing and keyword matching

### Location intelligence

- **geonamescache** – cities, countries, geographic data
- **pycountry** – ISO codes, subdivisions
- **phonenumbers** – phone parsing and geolocation
- **spaCy NER** – named-entity recognition for locations

### Semantic tagging (optional)

- **sentence-transformers** – embedding model (`all-MiniLM-L6-v2`)
- **PyTorch** – model inference (CPU or MPS on Apple Silicon)
- **NumPy** – similarity scoring

### Email

- **smtplib** – SMTP (Gmail, Outlook)
- **TLS** – encryption in transit

### Web performance

- Request interception – block trackers, images, fonts
- Custom HTTP headers
- CSS selectors and XPath for reliability

⸻

## Output

- **Excel report** – title, company, location, salary, cntrl f keyword matches, semantic tags
- **Email** – Report sent to `EMAIL_TO` (default: email-only, no local file)
- **Filename** – `{RoleName}_{RequisitionNumber}.xlsx` when job details are detected

⸻

## GDPR notes

- **Encryption in transit** – TLS for SMTP
- **Encryption at rest** – Gmail/Outlook encrypt stored mail
- **Report files** – `*.xlsx` in `.gitignore`; do not commit candidate data

⸻

## File structure

```
ATS-market-reporter/
├── req.py                      # Main script
├── semantic_tagger.py          # Optional embedding-based tagging
├── ner_location.py             # NER location extraction
├── email_credentials.env      # Your credentials (not committed)
├── email_credentials.env.example
├── keywords_ml.txt            # Example keyword sets
├── keywords_security.txt
├── keyword_expansions.txt.example
└── requirements-macos.txt
```
