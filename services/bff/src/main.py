import uvicorn

from logging_config import setup_logging

setup_logging("bff")

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
