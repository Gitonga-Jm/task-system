#!/bin/bash

echo "Starting Task Queue System..."

# Start API server
source venv/bin/activate
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload &
API_PID=$!

sleep 2

# Start 3 workers
export WORKER_ID=worker1
python3 worker.py &
WORKER1_PID=$!

export WORKER_ID=worker2
python3 worker.py &
WORKER2_PID=$!

export WORKER_ID=worker3
python3 worker.py &
WORKER3_PID=$!

echo ""
echo "=========================================="
echo "System Started!"
echo "API: http://localhost:8000"
echo "Dashboard: http://localhost:8000/dashboard"
echo "Workers: 3 running"
echo ""
echo "Stop with: pkill -f 'uvicorn|worker.py'"
echo "=========================================="

wait
