"""OpenTelemetry setup. Spans are always created; they are only *printed* when OTEL_CONSOLE=true.

To ship traces to Jaeger/Grafana/Honeycomb add an OTLP exporter below.
Never put raw user text in span attributes (use lengths / hashes instead).
"""
import logging

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor

from config.settings import settings

logger = logging.getLogger("telemetry")


def _format_span(span) -> str:
    ms = (span.end_time - span.start_time) / 1_000_000
    lines = [f"[span] {span.name}  {ms:.1f} ms  {span.status.status_code.name}"]
    for k, v in sorted((span.attributes or {}).items()):
        lines.append(f"         {k}: {v}")
    return "\n".join(lines) + "\n"


_provider = TracerProvider(resource=Resource.create({"service.name": "insider-trading-agent"}))
if settings.otel_console:
    _provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter(formatter=_format_span)))

# from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
# from opentelemetry.sdk.trace.export import BatchSpanProcessor
# _provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317")))

try:
    trace.set_tracer_provider(_provider)
except Exception:  # provider already set (e.g. Streamlit re-import)
    logger.debug("Tracer provider already configured")

tracer = trace.get_tracer("insider-trading-agent")
