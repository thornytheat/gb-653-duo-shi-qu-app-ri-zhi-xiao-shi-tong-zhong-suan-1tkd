from fastapi import APIRouter, Body, Request

from app.errors import DomainError
from .service import execute

router = APIRouter(tags=["M1-日志采信"])


def _intake(request: Request):
    return request.app.state.intake


def _fail(code: str):
    from app.reasons import reason
    raise DomainError(code, reason(code))


@router.get("/logs")
def page(request: Request):
    legacy = execute()
    snapshot = _intake(request).snapshot()
    data = {**legacy, "intake": snapshot}
    return request.app.state.templates.TemplateResponse(
        request, "m1_logs.html", {"title": "日志采信", "data": data})


@router.post("/api/logs/import")
def api(payload: dict = Body(default={})):
    return execute(payload or None)


@router.get("/api/logs/intake")
def intake_snapshot(request: Request, source: str | None = None):
    return _intake(request).snapshot(source)


def _as_int(payload: dict, key: str) -> int:
    try:
        return int(payload[key])
    except (KeyError, TypeError, ValueError):
        _fail("INVALID_ARGUMENT")


@router.post("/api/logs/intake")
def intake_dispatch(request: Request, payload: dict = Body(default={})):
    service = _intake(request)
    action = payload.get("action")
    try:
        if action == "open":
            return service.open_source(payload["source"], payload.get("batch_id"))
        if action == "upload":
            return service.upload(
                payload["source"], _as_int(payload, "generation"),
                _as_int(payload, "seq"), payload.get("text", ""),
                payload.get("expected_offset"))
        if action == "rotate":
            return service.rotate(
                payload["source"], _as_int(payload, "generation"),
                payload.get("batch_id"))
        if action == "end":
            return service.end_generation(
                payload["source"], _as_int(payload, "generation"))
    except KeyError:
        _fail("INVALID_ARGUMENT")
    except DomainError:
        raise  # 领域异常已带稳定原因码，直接交给全局处理器
    except ValueError as exc:
        _fail(str(exc))
    _fail("UNKNOWN_ACTION")
