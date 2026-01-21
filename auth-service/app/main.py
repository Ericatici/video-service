import sys
from pathlib import Path

# Add the parent directory to sys.path so we can import shared
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from .routes import router
from shared.database import Base, engine

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Auth Service - Video Converter")

# Add Prometheus metrics
Instrumentator().instrument(app).expose(app)

app.include_router(router, prefix="/auth", tags=["auth"])

@app.get("/health")
def health():
    return {"status": "ok", "service": "auth-service"}
