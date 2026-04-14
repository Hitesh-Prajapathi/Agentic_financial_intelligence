from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource

resource = Resource.create({"service.name": "insider-trading-agent"})
provider = TracerProvider(resource=resource)

# No console exporter — keeps terminal clean.
# Add OTLP exporter here for production monitoring.
trace.set_tracer_provider(provider)

tracer = trace.get_tracer("insider-trading-agent")
