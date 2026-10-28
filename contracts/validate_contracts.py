# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = [
#   "jsonschema[format]==4.25.1",
#   "openapi-spec-validator==0.7.2",
#   "PyYAML==6.0.2",
# ]
# ///
"""Fail-fast validation for the platform's versioned HTTP and event contracts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator

import yaml
from jsonschema import Draft202012Validator, FormatChecker
from openapi_spec_validator import validate_spec
from referencing import Registry, Resource


ROOT = Path(__file__).resolve().parent


def load(path: Path) -> Any:
    with path.open(encoding="utf-8") as source:
        return json.load(source) if path.suffix == ".json" else yaml.safe_load(source)


def refs(value: Any) -> Iterator[str]:
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "$ref":
                yield child
            yield from refs(child)
    elif isinstance(value, list):
        for child in value:
            yield from refs(child)


def resolve_ref(document: Any, document_path: Path, ref: str) -> Any:
    file_part, _, fragment = ref.partition("#")
    target_path = (document_path.parent / file_part).resolve() if file_part else document_path
    target = load(target_path) if file_part else document
    if fragment:
        for part in fragment.removeprefix("/").split("/"):
            target = target[part.replace("~1", "/").replace("~0", "~")]
    return target


def validate_openapi() -> None:
    path = ROOT / "openapi" / "platform-api-v1.yaml"
    document = load(path)
    validate_spec(document)
    assert document["openapi"].startswith("3.1."), "OpenAPI must remain on 3.1.x"

    operations = [
        operation
        for path_item in document["paths"].values()
        for method, operation in path_item.items()
        if method in {"get", "post", "put", "patch", "delete", "options", "head", "trace"}
    ]
    operation_ids = [operation["operationId"] for operation in operations]
    assert len(operation_ids) == len(set(operation_ids)), "OpenAPI operationId values must be unique"
    assert all("x-implementation-status" in operation for operation in operations), (
        "Every operation must distinguish implemented behavior from proposed behavior"
    )


def validate_asyncapi() -> None:
    path = ROOT / "asyncapi" / "platform-events-v1.yaml"
    document = load(path)
    assert document["asyncapi"].startswith("3.0."), "AsyncAPI must remain on 3.0.x"
    assert document.get("channels") and document.get("operations") and document.get("components")

    for ref in refs(document):
        resolve_ref(document, path, ref)

    for channel in document["channels"].values():
        assert channel["address"].endswith(".v1"), "Kafka topic contracts must be explicitly versioned"
    for message in document["components"]["messages"].values():
        assert message["name"].endswith(".v1"), "Event names must be explicitly versioned"
        assert message["bindings"]["kafka"]["key"]["description"] == "tenantId:orderId"
        schema = resolve_ref(document, path, message["payload"]["$ref"])
        event_type = schema["allOf"][1]["properties"]["eventType"]["const"]
        assert event_type == message["name"], "AsyncAPI message name must match its JSON Schema eventType"


def validate_event_examples() -> None:
    schema_paths = sorted((ROOT / "events").glob("*.schema.json"))
    schemas = [load(path) for path in schema_paths]
    resources = [(schema["$id"], Resource.from_contents(schema)) for schema in schemas]
    registry = Registry().with_resources(resources)

    for schema in schemas:
        Draft202012Validator.check_schema(schema)

    for example_path in sorted((ROOT / "examples").glob("*.json")):
        schema_name = f"{example_path.stem}.schema.json"
        schema = load(ROOT / "events" / schema_name)
        validator = Draft202012Validator(
            schema,
            registry=registry,
            format_checker=FormatChecker(),
        )
        errors = sorted(validator.iter_errors(load(example_path)), key=lambda error: list(error.path))
        assert not errors, f"{example_path.name}: " + "; ".join(error.message for error in errors)


def main() -> None:
    validate_openapi()
    validate_asyncapi()
    validate_event_examples()
    print("Contracts valid: OpenAPI 3.1, AsyncAPI 3.0 references, and JSON Schema examples")


if __name__ == "__main__":
    main()
