import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from .routes import router
from shared.database import Base, engine

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Video Service - Video Converter")

# Add Prometheus metrics
Instrumentator().instrument(app).expose(app)

app.include_router(router, prefix="/videos", tags=["videos"])

@app.get("/health")
def health():
    return {"status": "ok", "service": "video-service"}
