import json
import os
import sys
import time
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "services", "doc-processor", "src"
    ),
)

from chunker import Chunk, FixedSizeChunker
from config import Config
from events import DocumentChunkEvent, DLQEnvelope, RawDocumentEvent


# ──────────────────── test helpers ────────────────────

def _make_raw_event(**overrides) -> RawDocumentEvent:
    defaults = dict(
        source_type="s3",
        source_id="bucket/doc.pdf",
        content_ref="s3://bucket/doc.pdf",
        content_type="application/pdf",
        tenant_id="tenant-1",
        metadata={},
    )
    defaults.update(overrides)
    return RawDocumentEvent(**defaults)


def _make_kafka_msg(event: RawDocumentEvent, topic="raw-documents", partition=0, offset=10):
    msg = MagicMock()
    msg.value.return_value = event.to_json().encode()
    msg.error.return_value = None
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.timestamp.return_value = (1, int(time.time() * 1000))
    return msg


def _make_processor_cfg() -> Config:
    cfg = Config()
    cfg.kafka_input_topic = "raw-documents"
    cfg.kafka_output_topic = "document-chunks"
    cfg.kafka_dlq_topic = "dlq-raw-documents"
    cfg.kafka_produce_timeout_ms = 5000
    cfg.chunk_size_tokens = 512
    cfg.chunk_overlap_tokens = 64
    return cfg


def _make_processor(content_fetcher=None):
    from processor import DocumentProcessor

    consumer = MagicMock()
    producer = MagicMock()
    dlq_producer = MagicMock()
    db_conn = MagicMock()
    cursor = MagicMock()
    db_conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    db_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    if content_fetcher is None:
        content_fetcher = MagicMock(return_value=b"sample text content")

    proc = DocumentProcessor(
        consumer=consumer,
        producer=producer,
        dlq_producer=dlq_producer,
        content_fetcher=content_fetcher,
        db_conn=db_conn,
        cfg=_make_processor_cfg(),
    )
    return proc, consumer, producer, dlq_producer, db_conn


def _make_mock_chunk(doc_id="doc-abc", index=0, text="chunk text") -> Chunk:
    c = MagicMock(spec=Chunk)
    c.doc_id = doc_id
    c.chunk_id = f"{doc_id}:{index}"
    c.index = index
    c.text = text
    return c


# ──────────────────── PDF parser ────────────────────

class TestPDFParser:
    def test_pdf_parser_extracts_text(self):
        from parsers import parse

        mock_page = MagicMock()
        mock_page.extract_text.return_value = "Hello from PDF"
        mock_pdf_ctx = MagicMock()
        mock_pdf_ctx.__enter__ = MagicMock(return_value=mock_pdf_ctx)
        mock_pdf_ctx.__exit__ = MagicMock(return_value=False)
        mock_pdf_ctx.pages = [mock_page]

        with patch("parsers.pdfplumber") as mock_pdfplumber:
            mock_pdfplumber.open.return_value = mock_pdf_ctx
            result = parse(b"%PDF fake", "application/pdf")

        assert "Hello from PDF" in result

    def test_pdf_parser_skips_empty_pages(self):
        from parsers import parse

        page_with_text = MagicMock()
        page_with_text.extract_text.return_value = "Real text"
        page_no_text = MagicMock()
        page_no_text.extract_text.return_value = None
        mock_pdf_ctx = MagicMock()
        mock_pdf_ctx.__enter__ = MagicMock(return_value=mock_pdf_ctx)
        mock_pdf_ctx.__exit__ = MagicMock(return_value=False)
        mock_pdf_ctx.pages = [page_no_text, page_with_text]

        with patch("parsers.pdfplumber") as mock_pdfplumber:
            mock_pdfplumber.open.return_value = mock_pdf_ctx
            result = parse(b"%PDF fake", "application/pdf")

        assert result == "Real text"

    def test_pdf_parser_raises_parse_error_on_exception(self):
        from parsers import ParseError, parse

        with patch("parsers.pdfplumber") as mock_pdfplumber:
            mock_pdfplumber.open.side_effect = Exception("encrypted PDF")
            with pytest.raises(ParseError, match="pdfplumber"):
                parse(b"bad pdf", "application/pdf")


# ──────────────────── DOCX parser ────────────────────

class TestDOCXParser:
    def test_docx_parser_extracts_paragraphs(self):
        from parsers import parse

        p1, p2 = MagicMock(), MagicMock()
        p1.text = "Paragraph one"
        p2.text = "Paragraph two"
        mock_doc = MagicMock()
        mock_doc.paragraphs = [p1, p2]

        with patch("parsers.docx") as mock_docx:
            mock_docx.Document.return_value = mock_doc
            result = parse(
                b"fake docx bytes",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )

        assert "Paragraph one" in result
        assert "Paragraph two" in result

    def test_docx_parser_skips_empty_paragraphs(self):
        from parsers import parse

        p1, p2 = MagicMock(), MagicMock()
        p1.text = "Content"
        p2.text = ""
        mock_doc = MagicMock()
        mock_doc.paragraphs = [p1, p2]

        with patch("parsers.docx") as mock_docx:
            mock_docx.Document.return_value = mock_doc
            result = parse(b"fake docx bytes", "application/msword")

        assert result == "Content"

    def test_docx_parser_raises_parse_error(self):
        from parsers import ParseError, parse

        with patch("parsers.docx") as mock_docx:
            mock_docx.Document.side_effect = Exception("bad docx")
            with pytest.raises(ParseError, match="python-docx"):
                parse(
                    b"bad",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )


# ──────────────────── HTML parser ────────────────────

class TestHTMLParser:
    def test_html_parser_returns_text(self):
        from parsers import parse

        mock_converter = MagicMock()
        mock_converter.handle.return_value = "Hello World"
        mock_soup = MagicMock()
        mock_soup.__str__ = MagicMock(return_value="<html>Hello World</html>")

        with patch("parsers.html2text") as mock_h2t, patch("parsers.BeautifulSoup") as mock_bs4:
            mock_bs4.return_value = mock_soup
            mock_h2t.HTML2Text.return_value = mock_converter
            result = parse(b"<html><body>Hello World</body></html>", "text/html")

        assert "Hello World" in result

    def test_html_parser_handles_xhtml(self):
        from parsers import parse

        mock_converter = MagicMock()
        mock_converter.handle.return_value = "XHTML text"
        mock_soup = MagicMock()
        mock_soup.__str__ = MagicMock(return_value="<html/>")

        with patch("parsers.html2text") as mock_h2t, patch("parsers.BeautifulSoup") as mock_bs4:
            mock_bs4.return_value = mock_soup
            mock_h2t.HTML2Text.return_value = mock_converter
            result = parse(b"<html/>", "application/xhtml+xml")

        assert "XHTML text" in result

    def test_html_parser_raises_parse_error(self):
        from parsers import ParseError, parse

        with patch("parsers.BeautifulSoup") as mock_bs4:
            mock_bs4.side_effect = Exception("bs4 fail")
            with pytest.raises(ParseError, match="html2text"):
                parse(b"<html>", "text/html")


# ──────────────────── CSV parser ────────────────────

class TestCSVParser:
    def test_csv_parser_produces_tabular_text(self):
        from parsers import parse

        mock_df = MagicMock()
        mock_df.to_string.return_value = "name  age\nAlice  30\nBob  25"

        with patch("parsers.pd") as mock_pd:
            mock_pd.read_csv.return_value = mock_df
            result = parse(b"name,age\nAlice,30\nBob,25", "text/csv")

        assert "name" in result
        mock_pd.read_csv.assert_called_once()

    def test_csv_parser_raises_parse_error(self):
        from parsers import ParseError, parse

        with patch("parsers.pd") as mock_pd:
            mock_pd.read_csv.side_effect = Exception("bad csv")
            with pytest.raises(ParseError, match="pandas"):
                parse(b"bad", "text/csv")


# ──────────────────── JSON parser ────────────────────

class TestJSONParser:
    def test_json_parser_flattens_dict(self):
        from parsers import parse

        data = json.dumps({"title": "Test", "body": "Content"}).encode()
        result = parse(data, "application/json")
        assert "title: Test" in result
        assert "body: Content" in result

    def test_json_parser_flattens_nested(self):
        from parsers import parse

        data = json.dumps({"a": {"b": "deep"}}).encode()
        result = parse(data, "application/json")
        assert "a.b: deep" in result

    def test_json_parser_handles_list(self):
        from parsers import parse

        data = json.dumps([{"item": "one"}, {"item": "two"}]).encode()
        result = parse(data, "application/json")
        assert "item: one" in result

    def test_json_parser_raises_parse_error_on_invalid(self):
        from parsers import ParseError, parse

        with pytest.raises(ParseError, match="json"):
            parse(b"not json {{{", "application/json")


# ──────────────────── plain text parser ────────────────────

class TestPlainTextParser:
    def test_plain_text_returns_decoded_string(self):
        from parsers import parse

        text = "Hello, this is plain text."
        result = parse(text.encode("utf-8"), "text/plain")
        assert result == text

    def test_unknown_content_type_falls_back_to_utf8(self):
        from parsers import parse

        text = "fallback text"
        result = parse(text.encode("utf-8"), "application/octet-stream")
        assert result == text

    def test_content_type_with_charset_stripped(self):
        from parsers import parse

        text = "charset aware"
        result = parse(text.encode("utf-8"), "text/plain; charset=utf-8")
        assert result == text


# ──────────────────── chunker ────────────────────

class TestFixedSizeChunker:
    def _make_chunker(self, chunk_size=10, overlap=2):
        chunker = FixedSizeChunker(chunk_size=chunk_size, overlap=overlap)
        mock_enc = MagicMock()
        # Each character = one token for easy reasoning.
        mock_enc.encode.side_effect = lambda t: list(range(len(t)))
        mock_enc.decode.side_effect = lambda tokens: "X" * len(tokens)
        chunker._enc = mock_enc
        return chunker, mock_enc

    def test_produces_chunks_for_long_text(self):
        chunker, _ = self._make_chunker(chunk_size=5, overlap=1)
        chunks = chunker.chunk("a" * 20, "doc-abc")
        assert len(chunks) >= 1

    def test_chunk_text_length_bounded_by_chunk_size(self):
        chunker, _ = self._make_chunker(chunk_size=5, overlap=1)
        chunks = chunker.chunk("a" * 20, "doc-abc")
        for c in chunks:
            assert len(c.text) <= 5

    def test_indices_are_sequential(self):
        chunker, _ = self._make_chunker(chunk_size=5, overlap=1)
        chunks = chunker.chunk("a" * 20, "doc-abc")
        for i, c in enumerate(chunks):
            assert c.index == i

    def test_doc_id_and_chunk_id_set_correctly(self):
        chunker, _ = self._make_chunker(chunk_size=5, overlap=1)
        chunks = chunker.chunk("a" * 10, "doc-xyz")
        assert all(c.doc_id == "doc-xyz" for c in chunks)
        assert all(c.chunk_id == f"doc-xyz:{c.index}" for c in chunks)

    def test_returns_empty_for_empty_text(self):
        chunker, mock_enc = self._make_chunker()
        mock_enc.encode.side_effect = lambda t: []
        chunks = chunker.chunk("", "doc-abc")
        assert chunks == []

    def test_single_chunk_for_short_text(self):
        chunker, _ = self._make_chunker(chunk_size=100, overlap=10)
        chunks = chunker.chunk("hello", "doc-abc")
        assert len(chunks) == 1

    def test_overlap_produces_more_chunks_than_no_overlap(self):
        chunker_overlap, _ = self._make_chunker(chunk_size=5, overlap=2)
        chunker_no_overlap, _ = self._make_chunker(chunk_size=5, overlap=0)
        text = "a" * 20
        assert len(chunker_overlap.chunk(text, "d")) >= len(chunker_no_overlap.chunk(text, "d"))


# ──────────────────── processor: offset commit behaviour ────────────────────

class TestOffsetNotCommittedOnChunkPublishFailure:
    """Critical: when all chunk publish retries fail, offset must NOT be committed."""

    def test_offset_not_committed_when_chunk_publish_fails(self):
        event = _make_raw_event(content_type="text/plain")
        msg = _make_kafka_msg(event)

        proc, consumer, producer, dlq_producer, db = _make_processor(
            content_fetcher=MagicMock(return_value=b"hello world")
        )

        # All produce() calls raise
        producer.produce.side_effect = Exception("Kafka unavailable")

        with patch.object(
            proc._chunker,
            "chunk",
            return_value=[_make_mock_chunk()],
        ), patch("processor.time.sleep"):
            proc._process_message(msg)

        consumer.commit.assert_not_called()

    def test_dlq_receives_message_when_chunk_publish_fails(self):
        event = _make_raw_event(content_type="text/plain")
        msg = _make_kafka_msg(event)

        proc, consumer, producer, dlq_producer, db = _make_processor(
            content_fetcher=MagicMock(return_value=b"hello world")
        )
        producer.produce.side_effect = Exception("Kafka unavailable")

        with patch.object(
            proc._chunker,
            "chunk",
            return_value=[_make_mock_chunk()],
        ), patch("processor.time.sleep"):
            proc._process_message(msg)

        dlq_producer.produce.assert_called_once()
        call_kwargs = dlq_producer.produce.call_args[1]
        payload = json.loads(call_kwargs["value"].decode())
        assert payload["failure_reason"] == "chunk_publish_failed"


class TestOffsetCommittedAfterParseFailure:
    """Parse error is non-recoverable: route to DLQ, update status, commit offset."""

    def test_offset_committed_after_parse_failure(self):
        event = _make_raw_event(content_type="application/pdf")
        msg = _make_kafka_msg(event)

        proc, consumer, producer, dlq_producer, db = _make_processor(
            content_fetcher=MagicMock(return_value=b"%PDF bad")
        )

        from parsers import ParseError

        with patch("processor.parse", side_effect=ParseError("corrupt PDF")):
            proc._process_message(msg)

        consumer.commit.assert_called_once_with(message=msg)
        dlq_producer.produce.assert_called_once()

    def test_source_file_status_updated_to_error_on_parse_failure(self):
        event = _make_raw_event(content_type="application/pdf")
        msg = _make_kafka_msg(event)

        proc, consumer, producer, dlq_producer, db = _make_processor(
            content_fetcher=MagicMock(return_value=b"%PDF bad")
        )

        from parsers import ParseError

        with patch("processor.parse", side_effect=ParseError("corrupt")), \
             patch("processor.update_source_file_status") as mock_status:
            proc._process_message(msg)

        mock_status.assert_called_once_with(db, event.source_id, "error")


class TestDLQEnvelopeFields:
    def test_dlq_envelope_contains_correct_failure_metadata(self):
        event = _make_raw_event(content_type="application/pdf")
        msg = _make_kafka_msg(event, topic="raw-documents", partition=2, offset=42)

        proc, consumer, producer, dlq_producer, db = _make_processor(
            content_fetcher=MagicMock(return_value=b"bad pdf")
        )

        from parsers import ParseError

        with patch("processor.parse", side_effect=ParseError("encrypted PDF")):
            proc._process_message(msg)

        call_kwargs = dlq_producer.produce.call_args[1]
        payload = json.loads(call_kwargs["value"].decode())
        assert payload["failure_reason"] == "parse_error"
        assert payload["original_partition"] == 2
        assert payload["original_offset"] == 42
        assert payload["original_topic"] == "raw-documents"
        assert "failure_detail" in payload
        assert "dlq_id" in payload


class TestDLQRoutingOnFetchFailure:
    def test_dlq_called_and_offset_committed_on_fetch_failure(self):
        from fetcher import FetchError

        event = _make_raw_event()
        msg = _make_kafka_msg(event)

        proc, consumer, producer, dlq_producer, db = _make_processor(
            content_fetcher=MagicMock(side_effect=FetchError("S3 503"))
        )

        proc._process_message(msg)

        dlq_producer.produce.assert_called_once()
        consumer.commit.assert_called_once_with(message=msg)

    def test_dlq_failure_reason_is_fetch_error(self):
        from fetcher import FetchError

        event = _make_raw_event()
        msg = _make_kafka_msg(event)

        proc, consumer, producer, dlq_producer, db = _make_processor(
            content_fetcher=MagicMock(side_effect=FetchError("S3 503"))
        )

        proc._process_message(msg)

        call_kwargs = dlq_producer.produce.call_args[1]
        payload = json.loads(call_kwargs["value"].decode())
        assert payload["failure_reason"] == "fetch_error"


# ──────────────────── processor: happy path ────────────────────

class TestSuccessPath:
    def test_offset_committed_after_successful_processing(self):
        event = _make_raw_event(content_type="text/plain")
        msg = _make_kafka_msg(event)

        proc, consumer, producer, dlq_producer, db = _make_processor(
            content_fetcher=MagicMock(return_value=b"Some text content")
        )

        with patch.object(proc._chunker, "chunk", return_value=[_make_mock_chunk()]):
            proc._process_message(msg)

        consumer.commit.assert_called_once_with(message=msg)
        producer.produce.assert_called_once()
        dlq_producer.produce.assert_not_called()

    def test_chunk_event_published_with_correct_fields(self):
        event = _make_raw_event(content_type="text/plain")
        msg = _make_kafka_msg(event)

        proc, consumer, producer, dlq_producer, db = _make_processor(
            content_fetcher=MagicMock(return_value=b"Some text")
        )

        mock_chunk = _make_mock_chunk(doc_id="doc-abc", index=0, text="Some text")
        with patch.object(proc._chunker, "chunk", return_value=[mock_chunk]):
            proc._process_message(msg)

        call_kwargs = producer.produce.call_args[1]
        chunk_data = json.loads(call_kwargs["value"].decode())
        assert chunk_data["doc_id"] == "doc-abc"
        assert chunk_data["chunk_id"] == "doc-abc:0"
        assert chunk_data["chunk_index"] == 0
        assert chunk_data["total_chunks"] == 1
        assert chunk_data["source_id"] == "bucket/doc.pdf"
        assert chunk_data["tenant_id"] == "tenant-1"

    def test_no_dlq_routing_on_success(self):
        event = _make_raw_event(content_type="text/plain")
        msg = _make_kafka_msg(event)

        proc, consumer, producer, dlq_producer, db = _make_processor(
            content_fetcher=MagicMock(return_value=b"text")
        )

        with patch.object(proc._chunker, "chunk", return_value=[_make_mock_chunk()]):
            proc._process_message(msg)

        dlq_producer.produce.assert_not_called()


class TestEmptyChunks:
    def test_offset_committed_when_no_chunks_produced(self):
        event = _make_raw_event(content_type="text/plain")
        msg = _make_kafka_msg(event)

        proc, consumer, producer, dlq_producer, db = _make_processor(
            content_fetcher=MagicMock(return_value=b"")
        )

        with patch.object(proc._chunker, "chunk", return_value=[]):
            proc._process_message(msg)

        consumer.commit.assert_called_once_with(message=msg)
        producer.produce.assert_not_called()
        dlq_producer.produce.assert_not_called()


# ──────────────────── events schema ────────────────────

class TestDocumentChunkEventSchema:
    def test_chunk_event_serialises_and_deserialises(self):
        evt = DocumentChunkEvent(
            doc_id="doc-1",
            chunk_id="doc-1:0",
            chunk_index=0,
            total_chunks=3,
            text="hello world",
            source_type="s3",
            source_id="bucket/file.txt",
            content_type="text/plain",
            tenant_id="t1",
        )
        serialised = evt.to_json()
        parsed = json.loads(serialised)
        assert parsed["doc_id"] == "doc-1"
        assert parsed["chunk_index"] == 0
        assert parsed["total_chunks"] == 3
        assert isinstance(parsed["event_id"], str) and len(parsed["event_id"]) == 36

        roundtrip = DocumentChunkEvent.from_json(serialised)
        assert roundtrip.chunk_id == "doc-1:0"
        assert roundtrip.tenant_id == "t1"


class TestDLQEnvelopeSchema:
    def test_dlq_envelope_serialises_correctly(self):
        env = DLQEnvelope(
            original_topic="raw-documents",
            original_partition=1,
            original_offset=100,
            original_timestamp=1717401600,
            failure_reason="parse_error",
            failure_detail="encrypted PDF",
            original_payload={"event_id": "abc"},
        )
        serialised = env.to_json()
        parsed = json.loads(serialised)
        assert parsed["failure_reason"] == "parse_error"
        assert parsed["original_partition"] == 1
        assert parsed["original_offset"] == 100
        assert parsed["failure_count"] == 1
        assert isinstance(parsed["dlq_id"], str)


class TestDocProcessorOTelSpans:
    @pytest.fixture
    def span_exporter(self):
        """Provide an InMemorySpanExporter and patch the processor module's _tracer."""
        import processor as proc_mod
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        test_tracer = provider.get_tracer("test")
        original_tracer = proc_mod._tracer
        proc_mod._tracer = test_tracer
        yield exporter
        proc_mod._tracer = original_tracer
        exporter.clear()

    def test_process_message_emits_kafka_consume_span(self, span_exporter):
        """_process_message emits a kafka.consume span with topic/partition/offset attributes."""
        from processor import DocumentProcessor

        cfg = _make_processor_cfg()
        event = _make_raw_event(content_type="text/plain")
        msg = _make_kafka_msg(event, topic="raw-documents", partition=2, offset=99)

        proc = DocumentProcessor(
            consumer=MagicMock(),
            producer=MagicMock(),
            dlq_producer=MagicMock(),
            content_fetcher=lambda ref: b"Plain text content for span test",
            db_conn=MagicMock(),
            cfg=cfg,
        )
        proc._process_message(msg)

        span_names = [s.name for s in span_exporter.get_finished_spans()]
        assert "kafka.consume" in span_names
        span = next(s for s in span_exporter.get_finished_spans() if s.name == "kafka.consume")
        assert span.attributes.get("messaging.system") == "kafka"
        assert span.attributes.get("messaging.source") == "raw-documents"
        assert span.attributes.get("messaging.operation") == "receive"
        assert span.attributes.get("messaging.kafka.partition") == 2
        assert span.attributes.get("messaging.kafka.offset") == 99

    def test_produce_with_retry_emits_kafka_produce_span(self, span_exporter):
        """_produce_with_retry emits a kafka.produce span with destination and key attributes."""
        from processor import DocumentProcessor

        cfg = _make_processor_cfg()
        producer = MagicMock()
        proc = DocumentProcessor(
            consumer=MagicMock(),
            producer=producer,
            dlq_producer=MagicMock(),
            content_fetcher=lambda ref: b"",
            cfg=cfg,
        )
        proc._produce_with_retry(producer, "document-chunks", "chunk-abc", '{"text": "hi"}')

        span_names = [s.name for s in span_exporter.get_finished_spans()]
        assert "kafka.produce" in span_names
        span = next(s for s in span_exporter.get_finished_spans() if s.name == "kafka.produce")
        assert span.attributes.get("messaging.system") == "kafka"
        assert span.attributes.get("messaging.destination") == "document-chunks"
        assert span.attributes.get("messaging.operation") == "publish"
        assert span.attributes.get("messaging.kafka.message_key") == "chunk-abc"
