"""Exercise each public route's actual header annotation through FastAPI."""

import inspect
from typing import get_type_hints

import pytest
from automl_api.api.routes import contracts, datasets, monitoring, operations, training, validation
from automl_api.main import app
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient


def key_routes():
    return [
        route
        for module in (contracts, datasets, monitoring, operations, training, validation)
        for route in module.router.routes
        if isinstance(route, APIRoute)
        and any(field.alias == "Idempotency-Key" for field in route.dependant.header_params)
    ]


@pytest.mark.parametrize("route", key_routes(), ids=lambda route: route.path)
def test_header_contract_before_handler_execution(route):
    parameter = inspect.signature(route.endpoint).parameters["idempotency_key"]
    annotation = get_type_hints(route.endpoint, include_extras=True)["idempotency_key"]
    seen = []

    def probe(idempotency_key):
        seen.append(idempotency_key)
        return {"key": idempotency_key}

    probe.__annotations__["idempotency_key"] = annotation
    if parameter.default is not inspect.Parameter.empty:
        probe.__defaults__ = (parameter.default,)
    test_app = FastAPI()
    test_app.post("/probe")(probe)
    with TestClient(test_app) as client:
        for key in ("", " ", "\t", "a" * 256):
            result = client.post("/probe", headers={"Idempotency-Key": key})
            assert result.status_code == 422, (route.path, repr(key), result.text)
            assert any(
                error["loc"] == ["header", "Idempotency-Key"] for error in result.json()["detail"]
            )
        assert seen == []
        for key in ("a", "a" * 255, " key with spaces "):
            result = client.post("/probe", headers={"Idempotency-Key": key})
            assert result.status_code == 200
            assert result.json() == {"key": key}
        omitted = client.post("/probe")
        if parameter.default is None:
            assert omitted.status_code == 200
            assert omitted.json() == {"key": None}
        else:
            assert omitted.status_code == 422


def test_header_inventory_covers_all_command_route_families():
    modules = {route.endpoint.__module__.rsplit(".", 1)[-1] for route in key_routes()}
    assert modules == {
        "contracts",
        "datasets",
        "monitoring",
        "operations",
        "training",
        "validation",
    }
    assert len(key_routes()) == 21

    documented = {
        (method.upper(), path)
        for path, methods in app.openapi()["paths"].items()
        for method, operation in methods.items()
        if isinstance(operation, dict)
        and any(
            p.get("name") == "Idempotency-Key" and p.get("in") == "header"
            for p in operation.get("parameters", [])
        )
    }
    assert documented == {
        (method, "/api/v1" + route.path) for route in key_routes() for method in route.methods
    }
