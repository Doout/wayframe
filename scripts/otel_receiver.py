"""Bounded local OTLP test receiver, decoding exports with the official protobuf schema."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


class Receiver:
    def __init__(self, retain=100_000):
        self.exports = []
        self.errors = []
        self.retain = retain
        self.timings = {}
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if self.path != "/v1/traces" or not 0 < length <= 4 * 1024 * 1024:
                        raise ValueError("invalid export envelope")
                    body = self.rfile.read(length)
                    if self.headers.get("Content-Type") == "application/x-protobuf":
                        export = ExportTraceServiceRequest.FromString(body)
                        normalized = []
                        for resource in export.resource_spans:
                            for scope in resource.scope_spans:
                                for span in scope.spans:
                                    normalized.append(
                                        {
                                            "name": span.name,
                                            "trace_id": span.trace_id.hex(),
                                            "span_id": span.span_id.hex(),
                                            "parent_id": span.parent_span_id.hex(),
                                            "start_ns": span.start_time_unix_nano,
                                            "end_ns": span.end_time_unix_nano,
                                            "attributes": {
                                                a.key: getattr(a.value, a.value.WhichOneof("value"))
                                                for a in span.attributes
                                            },
                                            "status": span.status.code,
                                        }
                                    )
                    elif self.headers.get("Content-Type") == "application/json":
                        normalized = []
                        for resource in json.loads(body)["resourceSpans"]:
                            for scope in resource["scopeSpans"]:
                                for span in scope["spans"]:
                                    normalized.append(
                                        {
                                            "name": span["name"],
                                            "trace_id": span["traceId"].lower(),
                                            "span_id": span["spanId"].lower(),
                                            "parent_id": span.get("parentSpanId", "").lower(),
                                            "start_ns": int(span["startTimeUnixNano"]),
                                            "end_ns": int(span["endTimeUnixNano"]),
                                            "attributes": {
                                                a["key"]: next(iter(a["value"].values()))
                                                for a in span.get("attributes", [])
                                            },
                                            "status": span.get("status", {}).get("code", 0),
                                        }
                                    )
                    else:
                        raise ValueError("unsupported OTLP content type")
                    if not normalized:
                        raise ValueError("no exported spans")
                    with owner.lock:
                        for span in normalized:
                            name = span["name"]
                            if name == "policy.step":
                                name += ":" + span["attributes"].get("block.id", "unknown")
                            ns = span["end_ns"] - span["start_ns"]
                            timing = owner.timings.setdefault(
                                name, {"count": 0, "total_ns": 0, "min_ns": ns, "max_ns": ns}
                            )
                            timing["count"] += 1
                            timing["total_ns"] += ns
                            timing["min_ns"] = min(timing["min_ns"], ns)
                            timing["max_ns"] = max(timing["max_ns"], ns)
                        remaining = max(0, owner.retain - len(owner.exports))
                        owner.exports.extend(normalized[:remaining])
                    self.send_response(200)
                    self.send_header("Content-Type", self.headers["Content-Type"])
                    self.end_headers()
                    self.wfile.write(
                        b"{}" if self.headers["Content-Type"] == "application/json" else b""
                    )
                except (ValueError, KeyError, TypeError) as error:
                    with owner.lock:
                        owner.errors.append(str(error))
                    self.send_error(400)

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"

    def spans(self):
        with self.lock:
            return list(self.exports)

    def summary(self):
        with self.lock:
            return {
                name: {**timing, "mean_us": timing["total_ns"] / timing["count"] / 1000}
                for name, timing in self.timings.items()
            }

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
