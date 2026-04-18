#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

cleanup() {
    echo ""
    echo "Stopping..."
    kill 0 2>/dev/null
    wait 2>/dev/null
    echo "Done."
}
trap cleanup EXIT INT TERM

# Backend
echo "Starting backend..."
(
    cd "$PROJECT_DIR/backend"
    source venv/bin/activate
    python run.py 2>&1 | sed -u "s/^/[backend]  /"
) &

# Frontend
echo "Starting frontend..."
(
    cd "$PROJECT_DIR/frontend"
    npm run dev 2>&1 | sed -u "s/^/[frontend] /"
) &

echo ""
echo "Backend:  http://127.0.0.1:8000"
echo "Frontend: http://localhost:5173"
echo "Press Ctrl+C to stop both."
echo ""

wait
