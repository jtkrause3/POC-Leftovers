import io
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import fitz
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest, DocumentAnalysisFeature, DocumentContentFormat
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from openai import AzureOpenAI


load_dotenv()

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger("acord125-api")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_UPLOAD_SIZE_MB", "25")) * 1024 * 1024

SUPPORTED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff"}
QUERY_FIELDS = [
    "NamedInsured",
    "MailingAddress",
    "TaxIdentifier",
    "ProducerIdentifier",
    "SubProducerIdentifier",
    "ProducerCustomerIdentifier",
    "AgentNumber",
    "FederalEmployerIdentificationNumber",
]
QUERY_FIELD_TARGETS = {
    "NamedInsured": "Name",
    "MailingAddress": "Address",
    "TaxIdentifier": "FEIN",
    "FederalEmployerIdentificationNumber": "FEIN",
    "ProducerIdentifier": "Agent Number",
    "SubProducerIdentifier": "Agent Number",
    "ProducerCustomerIdentifier": "Agent Number",
    "AgentNumber": "Agent Number",
}
FINAL_KEYS = ["Name", "Address", "FEIN", "Agent Number"]


class ConfigError(RuntimeError):
    pass


def file_ext(filename: str) -> str:
    return os.path.splitext(filename or "")[1].lower()


def normalize_whitespace(value: Optional[str]) -> str:
    if not value:
        return ""
    text = value.replace("\r", "\n")
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines).strip()


def normalize_single_line(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", normalize_whitespace(value)).strip()


def clean_identifier(value: Optional[str]) -> str:
    text = normalize_single_line(value)
    text = text.replace(":selected:", "").replace(":unselected:", "").strip(" -")
    return text


def normalize_fein(value: Optional[str]) -> Optional[str]:
    text = clean_identifier(value)
    if not text:
        return ""

    match = re.search(r"\b\d{2}-\d{7}\b", text)
    if match:
        return match.group(0)

    match = re.search(r"\b\d{3}-\d{2}-\d{4}\b", text)
    if match:
        return match.group(0)

    return None


def parse_json_object(text: str) -> Dict[str, Any]:
    if not text:
        return {}

    cleaned = text.strip()
    cleaned = re.sub(r"^```json\s*", "", cleaned)
    cleaned = re.sub(r"^```\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        return {}

    return parsed if isinstance(parsed, dict) else {}


def init_final_payload() -> Dict[str, Optional[str]]:
    return {key: "" for key in FINAL_KEYS}


def ensure_api_key() -> Optional[Tuple[Dict[str, str], int]]:
    expected = os.getenv("API_SHARED_SECRET", "").strip()
    if not expected:
        logger.error("API_SHARED_SECRET is missing.")
        return jsonify({"error": "API is not configured."}), 500

    provided = request.headers.get("x-api-key", "").strip()
    if not provided:
        authorization = request.headers.get("Authorization", "")
        if authorization.lower().startswith("bearer "):
            provided = authorization[7:].strip()

    if provided != expected:
        return jsonify({"error": "Unauthorized."}), 401

    return None


def get_doc_client() -> DocumentIntelligenceClient:
    endpoint = os.getenv("AZURE_DOC_INTEL_ENDPOINT", "").strip()
    key = os.getenv("AZURE_DOC_INTEL_KEY", "").strip()
    if not endpoint or not key:
        raise ConfigError("Azure Document Intelligence is not configured.")

    return DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))


def get_openai_client() -> Tuple[AzureOpenAI, str]:
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "").strip()
    key = os.getenv("AZURE_OPENAI_KEY", "").strip()
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT", "").strip()
    api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-21").strip()

    if not endpoint or not key or not deployment:
        raise ConfigError("Azure OpenAI is not configured.")

    client = AzureOpenAI(api_key=key, azure_endpoint=endpoint, api_version=api_version)
    return client, deployment


def validate_uploads(items: List[Tuple[str, bytes]]) -> None:
    if not items:
        raise ValueError("No file provided.")

    for filename, data in items:
        ext = file_ext(filename)
        if ext not in SUPPORTED_EXTENSIONS:
            raise ValueError(f"Unsupported file type: {ext or 'unknown'}")
        if not data:
            raise ValueError(f"Uploaded file is empty: {filename}")


def extract_request_files() -> List[Tuple[str, bytes]]:
    items: List[Tuple[str, bytes]] = []

    single = request.files.get("file")
    if single and single.filename:
        items.append((single.filename, single.read()))

    for entry in request.files.getlist("files"):
        if entry and entry.filename:
            items.append((entry.filename, entry.read()))

    validate_uploads(items)
    return items


def merge_uploads(items: List[Tuple[str, bytes]]) -> Tuple[bytes, str]:
    if len(items) == 1:
        return items[0][1], items[0][0]

    merged = fitz.open()
    for filename, data in items:
        ext = file_ext(filename)
        if ext == ".pdf":
            source = fitz.open(stream=data, filetype="pdf")
        else:
            image_type = ext.lstrip(".").replace("jpg", "jpeg").replace("tif", "tiff")
            image_doc = fitz.open(stream=data, filetype=image_type)
            pdf_bytes = image_doc.convert_to_pdf()
            image_doc.close()
            source = fitz.open(stream=pdf_bytes, filetype="pdf")

        merged.insert_pdf(source)
        source.close()

    merged_bytes = merged.tobytes(deflate=True, garbage=4)
    merged.close()
    return merged_bytes, "combined-upload.pdf"


def first_matching_value(field_values: Dict[str, str], fragments: List[str]) -> str:
    for field_name, value in field_values.items():
        if all(fragment in field_name for fragment in fragments):
            return value
    return ""


def build_address(parts: List[str]) -> str:
    normalized_parts = [normalize_single_line(part) for part in parts if normalize_single_line(part)]
    if not normalized_parts:
        return ""

    if len(normalized_parts) >= 5:
        city = normalized_parts[-3]
        state = normalized_parts[-2]
        postal = normalized_parts[-1]
        remaining = normalized_parts[:-3]
        tail = " ".join([piece for piece in [city + ",", state, postal] if piece]).strip()
        return ", ".join(remaining + ([tail] if tail else []))

    if len(normalized_parts) >= 3 and re.match(r"^[A-Z]{2}\s+\d{5}(?:-\d{4})?$", normalized_parts[-1]):
        street_parts = normalized_parts[:-2]
        city = normalized_parts[-2]
        state_and_postal = normalized_parts[-1]
        tail = f"{city}, {state_and_postal}".strip()
        return ", ".join(street_parts + ([tail] if tail else []))

    return ", ".join(normalized_parts)


def extract_pdf_form_candidates(document_bytes: bytes, filename: str) -> Dict[str, List[Dict[str, Any]]]:
    candidates: Dict[str, List[Dict[str, Any]]] = {key: [] for key in FINAL_KEYS}
    if file_ext(filename) != ".pdf":
        return candidates

    try:
        doc = fitz.open(stream=document_bytes, filetype="pdf")
    except Exception:
        return candidates

    try:
        field_values: Dict[str, str] = {}
        for page_index in range(doc.page_count):
            page = doc.load_page(page_index)
            for widget in list(page.widgets() or []):
                value = clean_identifier(widget.field_value)
                if value:
                    field_values[widget.field_name] = value

        for slot in ["A", "B", "C"]:
            suffix = f"_{slot}["
            name = first_matching_value(field_values, [".NamedInsured_FullName", suffix])
            line_one = first_matching_value(field_values, [".NamedInsured_MailingAddress_LineOne", suffix])
            line_two = first_matching_value(field_values, [".NamedInsured_MailingAddress_LineTwo", suffix])
            city = first_matching_value(field_values, [".NamedInsured_MailingAddress_CityName", suffix])
            state = first_matching_value(field_values, [".NamedInsured_MailingAddress_StateOrProvinceCode", suffix])
            postal = first_matching_value(field_values, [".NamedInsured_MailingAddress_PostalCode", suffix])
            tax_identifier = first_matching_value(field_values, [".NamedInsured_TaxIdentifier", suffix])

            if name:
                candidates["Name"].append({"source": f"pdf_form.NamedInsured_FullName_{slot}", "value": name})

            address = build_address([line_one, line_two, city, state, postal])
            if address:
                candidates["Address"].append({"source": f"pdf_form.NamedInsured_MailingAddress_{slot}", "value": address})

            if tax_identifier:
                candidates["FEIN"].append(
                    {"source": f"pdf_form.NamedInsured_TaxIdentifier_{slot}", "value": normalize_fein(tax_identifier)}
                )

        for fragments, label in [
            ([".Insurer_ProducerIdentifier_A["], "ProducerIdentifier"),
            ([".Insurer_SubProducerIdentifier_A["], "SubProducerIdentifier"),
            ([".Producer_CustomerIdentifier_A["], "ProducerCustomerIdentifier"),
        ]:
            value = first_matching_value(field_values, fragments)
            if value:
                candidates["Agent Number"].append({"source": f"pdf_form.{label}", "value": value})

        for field_name, value in field_values.items():
            if "NationalProducerNumber" in field_name and value:
                candidates["Agent Number"].append({"source": "pdf_form.NationalProducerNumber", "value": value})
    finally:
        doc.close()

    return candidates


def analyze_document(document_bytes: bytes, pages: Optional[str]) -> Any:
    client = get_doc_client()
    poller = client.begin_analyze_document(
        "prebuilt-layout",
        io.BytesIO(document_bytes),
        pages=pages or None,
        locale="en-US",
        features=[
            DocumentAnalysisFeature.OCR_HIGH_RESOLUTION,
            DocumentAnalysisFeature.KEY_VALUE_PAIRS,
            DocumentAnalysisFeature.QUERY_FIELDS,
        ],
        query_fields=QUERY_FIELDS,
        output_content_format=DocumentContentFormat.MARKDOWN,
    )
    return poller.result()


def field_to_text(field: Any) -> str:
    if field is None:
        return ""

    for attr in ["value_string", "content"]:
        value = getattr(field, attr, None)
        if value:
            return normalize_whitespace(value)
    return ""


def append_candidate(bucket: Dict[str, List[Dict[str, Any]]], target: str, value: str, source: str, confidence: Optional[float] = None) -> None:
    cleaned = value if target == "Address" else clean_identifier(value)
    cleaned = normalize_fein(cleaned) if target == "FEIN" else cleaned
    if not cleaned:
        return

    bucket[target].append({"value": cleaned, "source": source, "confidence": confidence})


def classify_key_value_label(label: str) -> Optional[str]:
    normalized = re.sub(r"[^a-z0-9]+", " ", label.lower()).strip()
    if "first named insured" in normalized and "mailing address" in normalized:
        return "combined_name_address"
    if "mailing address" in normalized and "named insured" in normalized:
        return "Address"
    if "fein" in normalized or "soc sec" in normalized or "tax identifier" in normalized:
        return "FEIN"
    if "agency customer id" in normalized or "national producer number" in normalized:
        return "Agent Number"
    if "producer identifier" in normalized or "agent number" in normalized:
        return "Agent Number"
    if "first named insured" in normalized or ("named insured" in normalized and normalized.startswith("name")):
        return "Name"
    return None


def snippet_from_content(content: str, token: str, width: int) -> str:
    if not content:
        return ""

    index = content.upper().find(token.upper())
    if index < 0:
        return ""

    start = max(0, index - 250)
    end = min(len(content), index + width)
    return normalize_whitespace(content[start:end])


def extract_markdown_agent_candidates(section: str) -> List[Dict[str, Any]]:
    if not section:
        return []

    patterns = [
        (r"\bCODE:\s*([A-Za-z0-9-]+)\s*(?:SUBCODE:|AGENCY CUSTOMER ID:|LINES OF BUSINESS|$)", "markdown.CODE"),
        (r"\bSUBCODE:\s*([A-Za-z0-9-]+)\s*(?:AGENCY CUSTOMER ID:|LINES OF BUSINESS|$)", "markdown.SUBCODE"),
        (r"\bAGENCY CUSTOMER ID:\s*([A-Za-z0-9-]+)", "markdown.AGENCY_CUSTOMER_ID"),
        (r"\bNATIONAL PRODUCER NUMBER\b[:\s]*([A-Za-z0-9-]+)", "markdown.NATIONAL_PRODUCER_NUMBER"),
    ]

    values: List[Dict[str, Any]] = []
    for pattern, source in patterns:
        match = re.search(pattern, section, flags=re.IGNORECASE | re.DOTALL)
        if match:
            values.append({"value": clean_identifier(match.group(1)), "source": source, "confidence": None})
    return [item for item in values if item["value"]]


def extract_di_candidates(result: Any) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]], Dict[str, str]]:
    direct_candidates: Dict[str, List[Dict[str, Any]]] = {key: [] for key in FINAL_KEYS}
    combined_name_address: List[Dict[str, Any]] = []

    for document in result.documents or []:
        for field_name, field in (document.fields or {}).items():
            target = QUERY_FIELD_TARGETS.get(field_name)
            value = field_to_text(field)
            if target and value:
                append_candidate(direct_candidates, target, value, f"query_field.{field_name}", getattr(field, "confidence", None))

    for pair in result.key_value_pairs or []:
        key_text = normalize_single_line(getattr(getattr(pair, "key", None), "content", None))
        value_text = normalize_whitespace(getattr(getattr(pair, "value", None), "content", None))
        if not key_text or not value_text:
            continue

        target = classify_key_value_label(key_text)
        if target == "combined_name_address":
            combined_name_address.append({"key": key_text, "value": value_text, "confidence": getattr(pair, "confidence", None)})
        elif target:
            append_candidate(direct_candidates, target, value_text, f"key_value.{key_text}", getattr(pair, "confidence", None))

    content = result.content or ""
    snippets = {
        "applicant_section": snippet_from_content(content, "APPLICANT INFORMATION", 2600),
        "agency_section": snippet_from_content(content, "AGENCY CUSTOMER ID", 1800),
        "producer_signature_section": snippet_from_content(content, "PRODUCER'S SIGNATURE", 1400),
        "fein_section": snippet_from_content(content, "FEIN OR SOC SEC", 1600),
    }

    markdown_agent_candidates = extract_markdown_agent_candidates(snippets["agency_section"])
    if markdown_agent_candidates:
        direct_candidates["Agent Number"] = markdown_agent_candidates + direct_candidates["Agent Number"]

    return direct_candidates, combined_name_address, snippets


def split_combined_name_address(value: str) -> Tuple[str, str]:
    lines = [normalize_single_line(line) for line in normalize_whitespace(value).splitlines() if normalize_single_line(line)]
    if len(lines) >= 2:
        first_line = lines[0]
        address_match = re.search(r"\b\d{1,6}\s+[A-Za-z0-9].*", first_line)
        if address_match:
            name = first_line[: address_match.start()].strip(" ,-;")
            address_parts = [first_line[address_match.start() :].strip(" ,-;")] + lines[1:]
            return name, build_address(address_parts)
        return lines[0], build_address(lines[1:])

    compact = normalize_single_line(value)
    address_match = re.search(r"\b\d{1,6}\s+[A-Za-z0-9].*", compact)
    if address_match:
        name = compact[: address_match.start()].strip(" ,-;")
        address = compact[address_match.start() :].strip(" ,-;")
        return name, address

    return compact, ""


def dedupe_candidates(candidates: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    deduped: Dict[str, List[Dict[str, Any]]] = {key: [] for key in FINAL_KEYS}
    seen: Dict[str, set] = {key: set() for key in FINAL_KEYS}

    for key in FINAL_KEYS:
        for candidate in candidates.get(key, []):
            value = candidate.get("value", "")
            fingerprint = value.lower()
            if not value or fingerprint in seen[key]:
                continue
            seen[key].add(fingerprint)
            deduped[key].append(candidate)

    return deduped


def deterministic_extract(direct_candidates: Dict[str, List[Dict[str, Any]]], combined_name_address: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    result = init_final_payload()
    for key in FINAL_KEYS:
        if direct_candidates.get(key):
            result[key] = direct_candidates[key][0]["value"]

    if combined_name_address and (not result["Name"] or not result["Address"]):
        name_guess, address_guess = split_combined_name_address(combined_name_address[0]["value"])
        if not result["Name"]:
            result["Name"] = name_guess
        if not result["Address"]:
            result["Address"] = address_guess

    result["Name"] = clean_identifier(result["Name"])
    result["Address"] = normalize_single_line(result["Address"])
    result["FEIN"] = normalize_fein(result["FEIN"])
    result["Agent Number"] = clean_identifier(result["Agent Number"])
    return result


def adjudicate_with_openai(evidence: Dict[str, Any]) -> Dict[str, Optional[str]]:
    client, deployment = get_openai_client()
    system_prompt = (
        "You extract four exact values from an ACORD 125 Commercial Insurance Application. "
        "Use only the supplied evidence. Prefer fillable PDF field values when present, then Document Intelligence query-field results, "
        "then key-value pairs, then markdown snippets. Prefer first named insured applicant values, never producer or agency mailing address "
        "for the applicant address. Agent Number means the producer or agent-related identifier on the form. Prefer ProducerIdentifier or agent "
        "code first, then SubProducerIdentifier or subcode, then ProducerCustomerIdentifier or Agency Customer ID, then National Producer Number. "
        "If a field is not supportable, return an empty string. Return JSON only with exactly these keys: Name, Address, FEIN, Agent Number."
    )
    user_prompt = json.dumps(evidence, ensure_ascii=False)

    response = client.chat.completions.create(
        model=deployment,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    parsed = parse_json_object(response.choices[0].message.content or "")

    payload = init_final_payload()
    for key in FINAL_KEYS:
        raw_value = parsed.get(key, "")
        payload[key] = normalize_single_line(raw_value) if key == "Address" else clean_identifier(raw_value)
    payload["FEIN"] = normalize_fein(payload["FEIN"])
    return payload


def build_final_payload(document_bytes: bytes, filename: str, pages: Optional[str]) -> Tuple[Dict[str, Optional[str]], Dict[str, Any]]:
    pdf_form_candidates = extract_pdf_form_candidates(document_bytes, filename)
    di_result = analyze_document(document_bytes, pages)
    di_candidates, combined_name_address, snippets = extract_di_candidates(di_result)

    merged_candidates: Dict[str, List[Dict[str, Any]]] = {key: [] for key in FINAL_KEYS}
    for key in FINAL_KEYS:
        merged_candidates[key].extend(pdf_form_candidates.get(key, []))
        merged_candidates[key].extend(di_candidates.get(key, []))

    merged_candidates = dedupe_candidates(merged_candidates)
    fallback = deterministic_extract(merged_candidates, combined_name_address)

    evidence = {
        "pdf_form_candidates": pdf_form_candidates,
        "document_intelligence_candidates": di_candidates,
        "combined_name_address_candidates": combined_name_address,
        "markdown_snippets": snippets,
        "fallback": fallback,
    }

    final_payload = fallback
    if os.getenv("AZURE_OPENAI_ENDPOINT") and os.getenv("AZURE_OPENAI_KEY") and os.getenv("AZURE_OPENAI_DEPLOYMENT"):
        try:
            final_payload = adjudicate_with_openai(evidence)
        except Exception as exc:
            logger.warning("Azure OpenAI adjudication failed. Falling back to deterministic extraction: %s", exc)

    return final_payload, evidence


@app.errorhandler(413)
def request_too_large(_: Exception):
    limit_mb = int(os.getenv("MAX_UPLOAD_SIZE_MB", "25"))
    return jsonify({"error": f"Upload exceeds {limit_mb} MB limit."}), 413


@app.route("/")
def index():
    return jsonify(
        {
            "service": "acord125-extractor",
            "status": "ok",
            "extract_endpoint": "/api/extract",
            "auth": "Provide x-api-key or Authorization: Bearer <secret>",
        }
    )


@app.route("/health")
def health():
    return jsonify(
        {
            "ok": True,
            "document_intelligence_configured": bool(os.getenv("AZURE_DOC_INTEL_ENDPOINT") and os.getenv("AZURE_DOC_INTEL_KEY")),
            "azure_openai_configured": bool(os.getenv("AZURE_OPENAI_ENDPOINT") and os.getenv("AZURE_OPENAI_KEY") and os.getenv("AZURE_OPENAI_DEPLOYMENT")),
            "api_secret_configured": bool(os.getenv("API_SHARED_SECRET")),
            "max_upload_size_mb": int(os.getenv("MAX_UPLOAD_SIZE_MB", "25")),
        }
    )


@app.route("/api/extract", methods=["POST"])
@app.route("/extract", methods=["POST"])
def extract():
    unauthorized = ensure_api_key()
    if unauthorized:
        return unauthorized

    try:
        items = extract_request_files()
        pages = normalize_single_line(request.form.get("pages", "")) or None
        document_bytes, filename = merge_uploads(items)
        payload, evidence = build_final_payload(document_bytes, filename, pages)

        if request.args.get("debug", "false").lower() == "true":
            return jsonify({**payload, "_debug": evidence})
        return jsonify(payload)
    except ConfigError as exc:
        logger.error("Configuration error: %s", exc)
        return jsonify({"error": str(exc)}), 500
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.exception("Extraction failed")
        return jsonify({"error": f"Extraction failed: {exc}"}), 500


if __name__ == "__main__":
    host = os.getenv("FLASK_HOST", "127.0.0.1")
    port = int(os.getenv("FLASK_PORT", "5000"))
    logger.info("Starting Accord 125 extractor at http://%s:%s", host, port)
    app.run(host=host, port=port, debug=False)