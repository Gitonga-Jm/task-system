cd ~/task-queue-system
cat > models.py << 'EOF'
from pydantic import BaseModel
from datetime import datetime
from enum import Enum
from typing import Optional
import json

class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class Task(BaseModel):
    id: str
    type: str
    payload: dict
    max_retries: int = 3
    retries_remaining: int = 3
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = datetime.utcnow()
    last_error: Optional[str] = None

    def to_json(self) -> str:
        data = self.model_dump()
        data['created_at'] = data['created_at'].isoformat()
        data['status'] = data['status'].value
        return json.dumps(data)
    
    @staticmethod
    def from_json(json_str: str) -> "Task":
        data = json.loads(json_str)
        if 'created_at' in data and isinstance(data['created_at'], str):
            data['created_at'] = datetime.fromisoformat(data['created_at'])
        if 'status' in data and isinstance(data['status'], str):
            data['status'] = TaskStatus(data['status'])
        return Task(**data)
EOF
