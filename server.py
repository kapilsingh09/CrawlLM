import sys
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from logger_config import setup_logger
from exception import CustomException

logger = setup_logger("fastapi_server")

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    logger.info("GET / endpoint called")
    return {"message": "Welcome to FastAPI Server"}


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI server on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
