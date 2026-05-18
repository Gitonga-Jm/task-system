import os
import sys

print("Python version:", sys.version)
print("Starting minimal test...")

from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"status": "alive", "message": "Test successful"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('PORT', 8000))
    print(f"Starting on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
