"""QuantX Deployer — Development server launcher.

Excludes bots/ and logs/ from file watching so deployed bot scripts
don't trigger server restarts.
"""

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "api.main:app",
        host="127.0.0.1",
        port=8080,
        reload=True,
        reload_excludes=["bots/*", "logs/*", "*.db", "*.log", "*.pyc"],
        reload_dirs=["api", "static"],
    )
