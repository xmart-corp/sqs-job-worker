import json


class SqsMessageAttributes:
    """Encode and decode correlation data carried in SQS message attributes."""

    CORRELATION_FIELDS_ATTRIBUTE = "correlation_fields"
    MAX_MESSAGE_ATTRIBUTES = 10

    @classmethod
    def parse(cls, message: dict) -> tuple[dict, dict]:
        """Return correlation fields and opaque trace headers from an SQS message."""
        correlation_fields: dict = {}
        trace_headers: dict = {}
        for key, attribute in (message.get("MessageAttributes") or {}).items():
            value = (attribute or {}).get("StringValue")
            if not value:
                continue
            if key == cls.CORRELATION_FIELDS_ATTRIBUTE:
                try:
                    decoded = json.loads(value)
                except ValueError:
                    decoded = {}
                correlation_fields = decoded if isinstance(decoded, dict) else {}
            else:
                trace_headers[key] = value
        return correlation_fields, trace_headers

    @classmethod
    def build(cls, *, trace_headers: dict, correlation_fields: dict, caller_attributes: dict | None = None) -> tuple[dict, list[str]]:
        """Build SQS attributes and return the names of propagation attributes dropped to fit the SQS limit."""
        correlation_fields = {key: value for key, value in correlation_fields.items() if value is not None}

        propagation_attributes = {}
        if correlation_fields:
            propagation_attributes[cls.CORRELATION_FIELDS_ATTRIBUTE] = {"DataType": "String", "StringValue": json.dumps(correlation_fields)}
        for key, value in trace_headers.items():
            if value and key != cls.CORRELATION_FIELDS_ATTRIBUTE:
                propagation_attributes[key] = {"DataType": "String", "StringValue": value}

        caller_attributes = caller_attributes or {}
        attributes = propagation_attributes | caller_attributes
        dropped = []
        for name in reversed(list(propagation_attributes)):
            if len(attributes) <= cls.MAX_MESSAGE_ATTRIBUTES:
                break
            if name in caller_attributes:
                continue
            del attributes[name]
            dropped.append(name)
        return attributes, dropped
