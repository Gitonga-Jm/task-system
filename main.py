from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import HTMLResponse
import uuid
import secrets
import re
from datetime import datetime

app = FastAPI(title="Task Queue System")

# ============================================================
# STORAGE (Simple global arrays - no complex state)
# ============================================================
API_KEYS = {"demo-client": "demo_key_123456789"}
TASKS = []  # List of all tasks
DEAD_LETTERS = []  # Failed tasks

def validate_email(email):
    return re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email)

def validate_phone(phone):
    # Kenyan phone numbers: 0712345678 or +254712345678
    return re.match(r'^(?:\+254|0)?(7\d{8}|1\d{8})$', phone)

def verify_api_key(api_key):
    for client, key in API_KEYS.items():
        if key == api_key:
            return client
    return None

# ============================================================
# API ENDPOINTS
# ============================================================
@app.get("/")
def root():
    return {"message": "Task Queue System", "status": "running"}

@app.get("/stats")
def get_stats():
    completed = len([t for t in TASKS if t.get("status") == "completed"])
    failed = len([t for t in TASKS if t.get("status") == "failed"])
    pending = len([t for t in TASKS if t.get("status") == "pending"])
    return {
        "total_tasks": len(TASKS),
        "completed": completed,
        "failed": failed,
        "pending": pending,
        "dead_letter": len(DEAD_LETTERS),
        "success_rate": round((completed / max(len(TASKS), 1)) * 100, 1)
    }

@app.post("/tasks")
def create_task(task_data: dict, authorization: str = Header(None)):
    # Check API key
    if not authorization or not authorization.startswith("Bearer "):
        return {"error": "Missing API key. Use 'Authorization: Bearer YOUR_KEY'"}, 401
    
    api_key = authorization.replace("Bearer ", "")
    client = verify_api_key(api_key)
    if not client:
        return {"error": "Invalid API key"}, 403
    
    # Get task details
    task_type = task_data.get("type")
    payload = task_data.get("payload", {})
    
    # Validate based on type
    if task_type == "email":
        to_email = payload.get("to")
        if not to_email:
            return {"error": "Email recipient is required"}, 400
        if not validate_email(to_email):
            return {"error": "Invalid email format"}, 400
    
    elif task_type == "sms":
        phone = payload.get("phone")
        message = payload.get("message")
        if not phone:
            return {"error": "Phone number is required"}, 400
        if not validate_phone(phone):
            return {"error": "Invalid Kenyan phone number. Use 0712345678 or +254712345678"}, 400
        if not message:
            return {"error": "SMS message is required"}, 400
    
    elif task_type == "image":
        image_url = payload.get("image_url")
        if not image_url:
            return {"error": "Image URL is required"}, 400
    
    # Create task
    task_id = str(uuid.uuid4())[:8]
    task = {
        "id": task_id,
        "type": task_type,
        "payload": payload,
        "status": "pending",
        "created_at": datetime.now().isoformat(),
        "client": client
    }
    
    # Process task (simulate)
    if task_type == "fail_me":
        task["status"] = "failed"
        DEAD_LETTERS.append({
            "id": task_id,
            "type": task_type,
            "error": "Task failed after all retries",
            "failed_at": datetime.now().isoformat()
        })
    else:
        task["status"] = "completed"
    
    TASKS.append(task)
    
    return {
        "task_id": task_id,
        "status": task["status"],
        "message": f"{task_type} task submitted successfully"
    }

@app.get("/history")
def get_history():
    return {"tasks": TASKS[-30:]}  # Last 30 tasks

@app.get("/dead-letter")
def get_dead_letters():
    return {"tasks": DEAD_LETTERS}

@app.post("/dead-letter/retry/{task_id}")
def retry_dead_letter(task_id: str):
    global DEAD_LETTERS
    for i, task in enumerate(DEAD_LETTERS):
        if task.get("id") == task_id:
            DEAD_LETTERS.pop(i)
            # Create new task
            new_task = {
                "id": task_id,
                "type": task.get("type"),
                "payload": task.get("payload", {}),
                "status": "completed",
                "created_at": datetime.now().isoformat(),
                "client": "demo-client"
            }
            TASKS.append(new_task)
            return {"message": "Task retried successfully"}
    return {"error": "Task not found"}, 404

@app.delete("/dead-letter/{task_id}")
def delete_dead_letter(task_id: str):
    global DEAD_LETTERS
    for i, task in enumerate(DEAD_LETTERS):
        if task.get("id") == task_id:
            DEAD_LETTERS.pop(i)
            return {"message": "Task deleted"}
    return {"error": "Task not found"}, 404

@app.delete("/dead-letter/clear")
def clear_dead_letters():
    global DEAD_LETTERS
    count = len(DEAD_LETTERS)
    DEAD_LETTERS = []
    return {"message": f"Cleared {count} tasks"}

@app.post("/admin/generate-key")
def generate_key(data: dict):
    client_name = data.get("client_name")
    if not client_name:
        return {"error": "Client name required"}, 400
    if client_name in API_KEYS:
        return {"error": "Client already exists"}, 400
    new_key = f"key_{secrets.token_urlsafe(24)}"
    API_KEYS[client_name] = new_key
    return {"client": client_name, "api_key": new_key, "message": "API key generated"}

@app.get("/admin/keys")
def list_keys():
    masked = {}
    for client, key in API_KEYS.items():
        masked[client] = key[:10] + "..." + key[-4:] if len(key) > 14 else key
    return {"keys": masked}

# ============================================================
# DASHBOARD (Simple, working HTML)
# ============================================================
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse("""
<!DOCTYPE html>
<html>
<head>
    <title>Task Queue Dashboard</title>
    <meta charset="UTF-8">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: white; text-align: center; margin-bottom: 20px; }
        
        .stats {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 15px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: white;
            border-radius: 15px;
            padding: 15px;
            text-align: center;
        }
        .stat-number { font-size: 28px; font-weight: bold; color: #667eea; }
        .stat-label { color: #666; margin-top: 8px; font-size: 12px; }
        
        .tabs {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }
        .tab {
            background: white;
            padding: 10px 20px;
            border-radius: 10px;
            cursor: pointer;
            font-weight: bold;
        }
        .tab.active { background: #667eea; color: white; }
        .tab-content { display: none; background: white; border-radius: 15px; padding: 20px; }
        .tab-content.active { display: block; }
        
        input, select, textarea {
            width: 100%;
            padding: 10px;
            margin: 8px 0;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
        }
        button {
            background: #667eea;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 8px;
            cursor: pointer;
            margin: 5px;
        }
        button:hover { opacity: 0.8; }
        .btn-danger { background: #dc3545; }
        .btn-success { background: #28a745; }
        
        .task-item {
            background: #f8f9fa;
            border-left: 4px solid #667eea;
            padding: 15px;
            margin-bottom: 10px;
            border-radius: 8px;
        }
        .badge {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 11px;
            margin-left: 8px;
        }
        .badge-success { background: #28a745; color: white; }
        .badge-failed { background: #dc3545; color: white; }
        
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #e0e0e0; }
        
        .toast {
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: #28a745;
            color: white;
            padding: 12px 24px;
            border-radius: 8px;
            animation: slideIn 0.3s;
            z-index: 1000;
        }
        .toast.error { background: #dc3545; }
        @keyframes slideIn {
            from { transform: translateX(100%); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
        }
        
        .success-msg { color: #28a745; margin-top: 10px; }
        .error-msg { color: #dc3545; margin-top: 10px; }
        
        @media (max-width: 768px) {
            .stats { grid-template-columns: repeat(2, 1fr); }
        }
    </style>
</head>
<body>
<div class="container">
    <h1>🚀 Task Queue Dashboard</h1>
    
    <div class="stats" id="stats">
        <div class="stat-card"><div class="stat-number" id="totalTasks">-</div><div class="stat-label">Total Tasks</div></div>
        <div class="stat-card"><div class="stat-number" id="completedTasks">-</div><div class="stat-label">Completed</div></div>
        <div class="stat-card"><div class="stat-number" id="failedTasks">-</div><div class="stat-label">Failed</div></div>
        <div class="stat-card"><div class="stat-number" id="pendingTasks">-</div><div class="stat-label">Pending</div></div>
        <div class="stat-card"><div class="stat-number" id="deadLetter">-</div><div class="stat-label">Dead Letter</div></div>
    </div>
    
    <div class="tabs">
        <div class="tab active" onclick="switchTab('submit')">📝 Submit Task</div>
        <div class="tab" onclick="switchTab('deadletter')">💀 Dead Letter Queue</div>
        <div class="tab" onclick="switchTab('history')">📜 Task History</div>
        <div class="tab" onclick="switchTab('apikeys')">🔑 API Keys</div>
    </div>
    
    <!-- Submit Tab -->
    <div id="submitTab" class="tab-content active">
        <h2>Submit New Task</h2>
        <select id="taskType">
            <option value="email">📧 Email Task</option>
            <option value="sms">📱 SMS Task (Kenya)</option>
            <option value="image">🖼️ Image Processing</option>
            <option value="fail_me">💀 Test Failure (DLQ Demo)</option>
        </select>
        
        <div id="emailFields">
            <input type="email" id="emailTo" placeholder="To: recipient@example.com">
            <input type="text" id="emailSubject" placeholder="Subject">
            <textarea id="emailBody" rows="3" placeholder="Message"></textarea>
        </div>
        
        <div id="smsFields" style="display:none;">
            <input type="tel" id="smsPhone" placeholder="Phone: 0712345678 or +254712345678">
            <textarea id="smsMessage" rows="3" placeholder="SMS message"></textarea>
        </div>
        
        <div id="imageFields" style="display:none;">
            <input type="url" id="imageUrl" placeholder="Image URL: https://example.com/image.jpg">
            <select id="imageOp">
                <option value="resize">Resize</option>
                <option value="compress">Compress</option>
                <option value="watermark">Add Watermark</option>
            </select>
        </div>
        
        <div id="failFields" style="display:none;">
            <p style="color: #888;">⚠️ This task will fail and go to Dead Letter Queue for testing retry logic</p>
        </div>
        
        <button onclick="submitTask()">Submit Task</button>
        <div id="submitResult"></div>
    </div>
    
    <!-- Dead Letter Tab -->
    <div id="deadletterTab" class="tab-content">
        <h2>Dead Letter Queue</h2>
        <button onclick="loadDeadLetters()">Refresh</button>
        <button onclick="retryAllDead()" class="btn-success">Retry All</button>
        <button onclick="clearDeadLetters()" class="btn-danger">Clear All</button>
        <div id="deadLetters"></div>
    </div>
    
    <!-- History Tab -->
    <div id="historyTab" class="tab-content">
        <h2>Task History</h2>
        <button onclick="loadHistory()">Refresh</button>
        <div id="historyList"></div>
    </div>
    
    <!-- API Keys Tab -->
    <div id="apikeysTab" class="tab-content">
        <h2>Generate API Key</h2>
        <input type="text" id="clientName" placeholder="Client name (e.g., mycompany)">
        <button onclick="generateKey()">Generate Key</button>
        <div id="keyResult"></div>
        
        <h2 style="margin-top: 20px;">Existing API Keys</h2>
        <button onclick="loadKeys()">Refresh</button>
        <div id="keysList"></div>
    </div>
</div>

<script>
    // Tab switching - SIMPLE AND WORKS
    function switchTab(tabName) {
        // Hide all tabs
        document.querySelectorAll('.tab-content').forEach(tab => {
            tab.classList.remove('active');
        });
        // Remove active class from all tabs
        document.querySelectorAll('.tab').forEach(tab => {
            tab.classList.remove('active');
        });
        // Show selected tab
        document.getElementById(tabName + 'Tab').classList.add('active');
        // Add active class to clicked tab
        event.target.classList.add('active');
        
        // Load data when switching to certain tabs
        if (tabName === 'deadletter') loadDeadLetters();
        if (tabName === 'history') loadHistory();
        if (tabName === 'apikeys') loadKeys();
    }
    
    // Task type change
    document.getElementById('taskType').onchange = function() {
        const type = this.value;
        document.getElementById('emailFields').style.display = type === 'email' ? 'block' : 'none';
        document.getElementById('smsFields').style.display = type === 'sms' ? 'block' : 'none';
        document.getElementById('imageFields').style.display = type === 'image' ? 'block' : 'none';
        document.getElementById('failFields').style.display = type === 'fail_me' ? 'block' : 'none';
    };
    
    function showToast(msg, isError) {
        const toast = document.createElement('div');
        toast.className = 'toast' + (isError ? ' error' : '');
        toast.textContent = msg;
        document.body.appendChild(toast);
        setTimeout(() => toast.remove(), 3000);
    }
    
    async function loadStats() {
        try {
            const res = await fetch('/stats');
            const data = await res.json();
            document.getElementById('totalTasks').innerText = data.total_tasks || 0;
            document.getElementById('completedTasks').innerText = data.completed || 0;
            document.getElementById('failedTasks').innerText = data.failed || 0;
            document.getElementById('pendingTasks').innerText = data.pending || 0;
            document.getElementById('deadLetter').innerText = data.dead_letter || 0;
        } catch(e) { console.error(e); }
    }
    
    async function loadDeadLetters() {
        try {
            const res = await fetch('/dead-letter');
            const data = await res.json();
            const container = document.getElementById('deadLetters');
            if (!data.tasks || data.tasks.length === 0) {
                container.innerHTML = '<p>✅ No failed tasks</p>';
                return;
            }
            container.innerHTML = data.tasks.map(t => `
                <div class="task-item">
                    <strong>${t.type}</strong>
                    <span class="badge badge-failed">FAILED</span>
                    <div>ID: ${t.id}</div>
                    <div>Error: ${t.error}</div>
                    <div>Failed: ${new Date(t.failed_at).toLocaleString()}</div>
                    <button onclick="retryTask('${t.id}')">Retry</button>
                    <button onclick="deleteTask('${t.id}')" class="btn-danger">Delete</button>
                </div>
            `).join('');
        } catch(e) { console.error(e); }
    }
    
    async function loadHistory() {
        try {
            const res = await fetch('/history');
            const data = await res.json();
            const container = document.getElementById('historyList');
            if (!data.tasks || data.tasks.length === 0) {
                container.innerHTML = '<p>No tasks yet</p>';
                return;
            }
            let html = '<table><thead><tr><th>ID</th><th>Type</th><th>Status</th><th>Time</th></tr></thead><tbody>';
            data.tasks.forEach(t => {
                html += `<tr><td>${t.id}</td><td>${t.type}</td><td><span class="badge ${t.status === 'completed' ? 'badge-success' : 'badge-failed'}">${t.status}</span></td><td>${new Date(t.created_at).toLocaleString()}</td></tr>`;
            });
            html += '</tbody></table>';
            container.innerHTML = html;
        } catch(e) { console.error(e); }
    }
    
    async function loadKeys() {
        try {
            const res = await fetch('/admin/keys');
            const data = await res.json();
            const container = document.getElementById('keysList');
            if (!data.keys || Object.keys(data.keys).length === 0) {
                container.innerHTML = '<p>No API keys</p>';
                return;
            }
            container.innerHTML = '';
            for (const [client, key] of Object.entries(data.keys)) {
                container.innerHTML += `<div class="task-item"><strong>${client}</strong><div style="font-family:monospace;">Key: ${key}</div></div>`;
            }
        } catch(e) { console.error(e); }
    }
    
    async function submitTask() {
        const type = document.getElementById('taskType').value;
        let payload = {};
        
        if (type === 'email') {
            const to = document.getElementById('emailTo').value;
            if (!to) {
                showToast('❌ Email recipient required', true);
                return;
            }
            payload = { to, subject: document.getElementById('emailSubject').value, body: document.getElementById('emailBody').value };
        } else if (type === 'sms') {
            const phone = document.getElementById('smsPhone').value;
            if (!phone) {
                showToast('❌ Phone number required', true);
                return;
            }
            payload = { phone, message: document.getElementById('smsMessage').value };
        } else if (type === 'image') {
            const url = document.getElementById('imageUrl').value;
            if (!url) {
                showToast('❌ Image URL required', true);
                return;
            }
            payload = { image_url: url, operation: document.getElementById('imageOp').value };
        }
        
        try {
            const res = await fetch('/tasks', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer demo_key_123456789'
                },
                body: JSON.stringify({ type, payload })
            });
            const data = await res.json();
            if (res.ok) {
                showToast(`✅ ${data.message}`);
                document.getElementById('submitResult').innerHTML = `<p class="success-msg">✅ ${data.message}</p>`;
                setTimeout(() => document.getElementById('submitResult').innerHTML = '', 3000);
                loadStats();
                loadHistory();
                // Clear form
                if (type === 'email') {
                    document.getElementById('emailTo').value = '';
                    document.getElementById('emailSubject').value = '';
                    document.getElementById('emailBody').value = '';
                } else if (type === 'sms') {
                    document.getElementById('smsPhone').value = '';
                    document.getElementById('smsMessage').value = '';
                } else if (type === 'image') {
                    document.getElementById('imageUrl').value = '';
                }
            } else {
                showToast(`❌ ${data.error}`, true);
                document.getElementById('submitResult').innerHTML = `<p class="error-msg">❌ ${data.error}</p>`;
            }
        } catch(e) {
            showToast('❌ Error submitting task', true);
        }
    }
    
    async function retryTask(taskId) {
        const res = await fetch(`/dead-letter/retry/${taskId}`, { method: 'POST' });
        if (res.ok) {
            showToast('🔄 Task retried');
            loadDeadLetters();
            loadStats();
            loadHistory();
        } else {
            showToast('❌ Failed to retry', true);
        }
    }
    
    async function retryAllDead() {
        const res = await fetch('/dead-letter');
        const data = await res.json();
        if (!data.tasks || data.tasks.length === 0) return;
        if (!confirm(`Retry ${data.tasks.length} tasks?`)) return;
        for (const task of data.tasks) {
            await fetch(`/dead-letter/retry/${task.id}`, { method: 'POST' });
        }
        showToast(`🔄 Retrying ${data.tasks.length} tasks`);
        loadDeadLetters();
        loadStats();
        loadHistory();
    }
    
    async function deleteTask(taskId) {
        if (!confirm('Delete this failed task?')) return;
        await fetch(`/dead-letter/${taskId}`, { method: 'DELETE' });
        showToast('🗑️ Task deleted');
        loadDeadLetters();
        loadStats();
    }
    
    async function clearDeadLetters() {
        if (!confirm('Delete ALL failed tasks?')) return;
        await fetch('/dead-letter/clear', { method: 'DELETE' });
        showToast('🗑️ All cleared');
        loadDeadLetters();
        loadStats();
    }
    
    async function generateKey() {
        const name = document.getElementById('clientName').value;
        if (!name) {
            showToast('❌ Enter client name', true);
            return;
        }
        try {
            const res = await fetch('/admin/generate-key', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ client_name: name })
            });
            const data = await res.json();
            if (res.ok) {
                document.getElementById('keyResult').innerHTML = `<div style="background:#d4edda;padding:10px;border-radius:5px;margin-top:10px;"><strong>✅ Key Generated!</strong><br>Client: ${data.client}<br>Key: <code>${data.api_key}</code><br><small>Save this key securely!</small></div>`;
                document.getElementById('clientName').value = '';
                loadKeys();
                showToast(`✅ Key generated for ${name}`);
            } else {
                showToast(`❌ ${data.error}`, true);
            }
        } catch(e) {
            showToast('❌ Error', true);
        }
    }
    
    // Initial load
    loadStats();
    loadDeadLetters();
    loadHistory();
    loadKeys();
    setInterval(loadStats, 5000);
</script>
</body>
</html>
    """)

if __name__ == "__main__":
    import uvicorn
    print("=" * 60)
    print("🚀 Task Queue System Started Successfully!")
    print("=" * 60)
    print(f"📍 Dashboard: http://localhost:8000/dashboard")
    print(f"🔑 Demo API Key: demo_key_123456789")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=8000)
