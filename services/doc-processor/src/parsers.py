import io
import json
import logging

logger = logging.getLogger(__name__)

# Optional dependencies — imported at module level so tests can patch them easily.
try:
    import pdfplumber
except ImportError:
    pdfplumber = None  # type: ignore

try:
    import docx
except ImportError:
    docx = None  # type: ignore

try:
    import html2text
    from bs4 import BeautifulSoup
except ImportError:
    html2text = None  # type: ignore
    BeautifulSoup = None  # type: ignore

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore


class ParseError(Exception):
    pass


def parse(content: bytes, content_type: str) -> str:
    ct = content_type.lower().split(";")[0].strip()

    if ct == "application/pdf":
        return _parse_pdf(content)
    elif ct in (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/msword",
    ):
        return _parse_docx(content)
    elif ct in ("text/html", "application/xhtml+xml"):
        return _parse_html(content)
    elif ct == "text/csv":
        return _parse_csv(content)
    elif ct == "application/json":
        return _parse_json(content)
    else:
        return content.decode("utf-8", errors="replace")


def _parse_pdf(content: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            parts = [page.extract_text() for page in pdf.pages]
        return "\n".join(p for p in parts if p)
    except Exception as exc:
        raise ParseError(f"pdfplumber: {exc}") from exc


def _parse_docx(content: bytes) -> str:
    try:
        doc = docx.Document(io.BytesIO(content))
        return "\n".join(p.text for p in doc.paragraphs if p.text.strip())
    except Exception as exc:
        raise ParseError(f"python-docx: {exc}") from exc


def _parse_html(content: bytes) -> str:
    try:
        soup = BeautifulSoup(content, "html.parser")
        converter = html2text.HTML2Text()
        converter.ignore_links = False
        return converter.handle(str(soup))
    except Exception as exc:
        raise ParseError(f"html2text: {exc}") from exc


def _parse_csv(content: bytes) -> str:
    try:
        df = pd.read_csv(io.BytesIO(content))
        return df.to_string(index=False)
    except Exception as exc:
        raise ParseError(f"pandas: {exc}") from exc


def _parse_json(content: bytes) -> str:
    try:
        data = json.loads(content.decode("utf-8"))
        return _flatten_json(data)
    except Exception as exc:
        raise ParseError(f"json: {exc}") from exc


def _flatten_json(obj, prefix: str = "") -> str:
    lines = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            full_key = f"{prefix}.{key}" if prefix else key
            lines.append(_flatten_json(value, full_key))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            full_key = f"{prefix}[{i}]"
            lines.append(_flatten_json(item, full_key))
    else:
        lines.append(f"{prefix}: {obj}")
    return "\n".join(lines)
