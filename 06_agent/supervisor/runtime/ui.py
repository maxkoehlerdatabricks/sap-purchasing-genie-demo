"""Serve the supervisor's chat page at / (ui/index.html).

A deliberately simple page for business users: pick a mode (Supervisor or Normal), ask,
read the answer. It calls this app's own /api/invocations in background mode and polls the
result, so long supervisor answers are not cut off by a request timeout.
"""

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

UI_DIR = Path(__file__).resolve().parent.parent / "ui"


def install_ui(app: FastAPI) -> None:
    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(UI_DIR / "index.html", headers={"Cache-Control": "no-store"})

    app.mount("/ui", StaticFiles(directory=UI_DIR), name="ui")
