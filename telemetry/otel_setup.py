from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.sdk.resources import Resource
import json
import logging

logger = logging.getLogger("telemetry")


class CleanSpanFormatter:
    """Custom formatter that prints spans in a readable, compact format."""

    def format_span(self, span):
        """Format a single ReadableSpan into a readable string."""
        duration_ns = span.end_time - span.start_time
        duration_ms = duration_ns / 1_000_000

        attrs = dict(span.attributes) if span.attributes else {}

        lines = [
            "",
            "─" * 60,
            f"📊 SPAN: {span.name}",
            f"   Duration : {duration_ms:.1f} ms",
            f"   Status   : {span.status.status_code.name}",
        ]

        # Group attributes by prefix for readability
        if attrs:
            lines.append("   Attributes:")
            for key, value in sorted(attrs.items()):
                lines.append(f"      {key}: {value}")

        lines.append("─" * 60)
        return "\n".join(lines)


class FormattedConsoleExporter(ConsoleSpanExporter):
    """Console exporter that uses our clean formatter."""

    def __init__(self):
        self._formatter = CleanSpanFormatter()
        super().__init__(
            formatter=lambda span: self._formatter.format_span(span)
        )


# ── Setup ────────────────────────────────────────────────────
resource = Resource.create({"service.name": "insider-trading-agent"})
provider = TracerProvider(resource=resource)

# Console exporter — prints every completed span to the terminal
console_exporter = FormattedConsoleExporter()
provider.add_span_processor(SimpleSpanProcessor(console_exporter))

# Tip: To send to Jaeger / Grafana / Honeycomb, add an OTLP exporter here:
# from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
# otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317")
# provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

trace.set_tracer_provider(provider)
tracer = trace.get_tracer("insider-trading-agent")
