import os
import json
import logging
import time
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from pydantic import BaseModel
from dotenv import load_dotenv
from typing import List

# Import isolated database connection profiles cleanly
from database import engine

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("OpsMesh.CoreAPI")

app = FastAPI(title="OpsMesh Cognitive Core API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "https://opsmesh-frontend.onrender.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ExecutionRequest(BaseModel):
    remediation_steps: list[str]

# Input schema validation structure for direct telemetry mock generation
class EventCreateRequest(BaseModel):
    service_name: str
    severity: str
    log_text: str

# =====================================================================
# REAL-TIME BROADCAST ENGINE (WebSocket Connection Hub)
# =====================================================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"📡 New dashboard client connected. Active connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"🔌 Client disconnected. Active connections: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """Pushes event frames downstream to every single active React client."""
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                # Catch zombie/ghost socket handles gracefully
                pass

manager = ConnectionManager()

@app.websocket("/api/ws/alerts")
async def websocket_endpoint(websocket: WebSocket):
    """Exposes an active real-time message stream pipeline for the UI layout dashboard."""
    await manager.connect(websocket)
    try:
        while True:
            # Keep the baseline polling tunnel open and listen for client heartbeats
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.warning(f"Unexpected WebSocket socket trace disconnect: {str(e)}")
        manager.disconnect(websocket)

@app.post("/api/events/broadcast")
async def trigger_internal_broadcast(event_data: dict):
    """Secure backchannel endpoint targeting async microservice ingestion pipeline signals."""
    await manager.broadcast(event_data)
    return {"status": "SUCCESS", "message": "Broadcast frame pushed downstream seamlessly."}

# =====================================================================
# METRICS COMPUTE BOUNDARY
# =====================================================================
@app.get("/api/metrics")
def get_opsmesh_metrics():
    """Compiles operational state aggregations filtered by ACTIVE status."""
    query = text("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) as critical,
            COUNT(CASE WHEN severity = 'HIGH' THEN 1 END) as high
        FROM incident_logs
        WHERE status = 'ACTIVE'
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(query).mappings().fetchone()
            return {
                "totalEvents": result["total"] or 0,
                "criticalTriggers": result["critical"] or 0,
                "highAlerts": result["high"] or 0,
                "pipelineStatus": "🟢 Active"
            }
    except Exception as e:
        logger.error(f"Database aggregation failure: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database aggregation failure: {str(e)}")

# =====================================================================
# LEDGER INGEST BOUNDARY (Unified Flat Output)
# =====================================================================
@app.get("/api/events")
def get_all_events():
    """Fetches the active telemetry ledger structured flat and chronologically."""
    query = text("SELECT * FROM incident_logs WHERE status = 'ACTIVE' ORDER BY timestamp DESC")
    try:
        with engine.connect() as conn:
            rows = conn.execute(query).mappings().fetchall()
            
            standardized_list = []
            for row in rows:
                raw_steps = row["remediation_steps"]
                standardized_steps = ["Inspect application log streams manually."]
                
                if raw_steps:
                    if isinstance(raw_steps, str):
                        try:
                            standardized_steps = json.loads(raw_steps)
                        except Exception:
                            standardized_steps = [raw_steps]
                    elif isinstance(raw_steps, list):
                        standardized_steps = raw_steps

                # Safely extract pre-calculated metrics JSON if present inside structural listings
                metrics_data = row["metrics"] if "metrics" in row else {}
                if isinstance(metrics_data, str):
                    try:
                        metrics_data = json.loads(metrics_data)
                    except Exception:
                        metrics_data = {}

                # 🟢 Flatten keys directly into the root level item dictionary
                standardized_list.append({
                    "id": row["id"],
                    "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None,
                    "service_name": row["service_name"],
                    "log_text": row["log_text"],
                    "classification": row["classification"] or "Unclassified Operational Alert",
                    "severity": row["severity"] or "MEDIUM",
                    "remediation_steps": standardized_steps,
                    
                    # Direct flat mappings for real-time WebSocket initialization states
                    "saturation_pct": metrics_data.get("saturation_pct", 42),
                    "system_status": metrics_data.get("system_status", "HEALTHY"),
                    "blast_radius": metrics_data.get("blast_radius", ["None Detected"]),
                    "downstream_latency_ms": metrics_data.get("downstream_latency_ms", 55)
                })
            return standardized_list
    except Exception as e:
        logger.error(f"Ledger extraction failure: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ledger extraction failure: {str(e)}")

# =====================================================================
# LEDGER INJECTION BOUNDARY (Unified Flat Output)
# =====================================================================
@app.post("/api/events")
def create_mock_event(payload: EventCreateRequest):
    """Direct injector endpoint to create custom severity logging contexts with inline default mock metrics."""
    insert_query = text("""
        INSERT INTO incident_logs (service_name, severity, log_text, classification, status, remediation_steps, metrics, timestamp)
        VALUES (:service, :sev, :log, :class, 'ACTIVE', :steps, :metrics, NOW())
        RETURNING id, timestamp
    """)
    
    log_upper = payload.log_text.upper()
    classification = "Unclassified Operational Alert"
    remediation_steps = ["Inspect application log streams manually."]
    
    # Establish local metric baselines for manual UI injection passes
    mock_metrics = {
        "saturation_pct": 42,
        "system_status": "HEALTHY",
        "blast_radius": ["None Detected"],
        "downstream_latency_ms": 55
    }
    
    if "REDIS" in log_upper:
        classification = "Redis Eviction Warning (Memory Pressure)" if payload.severity.upper() == "MEDIUM" else "Redis Connection Pool Exhaustion Anomaly"
        remediation_steps = [
            "redis-cli -h localhost -p 6379 CONFIG SET maxclients 20000",
            "redis-cli CLIENT KILL TYPE normal",
            "kubectl rollout restart deployment/redis-cluster-node"
        ]
        mock_metrics = {
            "saturation_pct": 58 if payload.severity.upper() == "MEDIUM" else 100,
            "system_status": "WARNING" if payload.severity.upper() == "MEDIUM" else "CRITICAL",
            "blast_radius": ["User-Session-Cache"] if payload.severity.upper() == "MEDIUM" else ["Payment-Processing-Worker", "User-Session-Cache"],
            "downstream_latency_ms": 140 if payload.severity.upper() == "MEDIUM" else 480
        }
    elif "POSTGRES" in log_upper or "DB_" in log_upper or "DATABASE" in log_upper:
        classification = "Relational DB Pool Connection Timeout"
        remediation_steps = [
            "psql -U postgres -d opsmesh -c \"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE age(clock_timestamp() - query_start) > interval '5 minutes';\"",
            "kubectl scale deployment/postgres-cluster-pool --replicas=3"
        ]
        mock_metrics = {
            "saturation_pct": 65 if payload.severity.upper() == "MEDIUM" else 95,
            "system_status": "WARNING" if payload.severity.upper() == "MEDIUM" else "CRITICAL",
            "blast_radius": ["Inventory-Service"] if payload.severity.upper() == "MEDIUM" else ["Order-Management-Service", "Inventory-Service"],
            "downstream_latency_ms": 210 if payload.severity.upper() == "MEDIUM" else 1200
        }
        
    try:
        with engine.connect() as conn:
            result = conn.execute(insert_query, {
                "service": payload.service_name,
                "sev": payload.severity.upper(),
                "log": payload.log_text,
                "class": classification,
                "steps": json.dumps(remediation_steps),
                "metrics": json.dumps(mock_metrics)
            })
            inserted_row = result.mappings().fetchone()
            conn.commit()
            
        # 🟢 Flattened broadcast structure matches UI expectations immediately
        broadcast_payload = {
            "id": inserted_row["id"],
            "timestamp": inserted_row["timestamp"].isoformat() if inserted_row["timestamp"] else None,
            "service_name": payload.service_name,
            "log_text": payload.log_text,
            "classification": classification,
            "severity": payload.severity.upper(),
            "remediation_steps": remediation_steps,
            
            # Root-level metrics propagation over WebSockets
            "saturation_pct": mock_metrics["saturation_pct"],
            "system_status": mock_metrics["system_status"],
            "blast_radius": mock_metrics["blast_radius"],
            "downstream_latency_ms": mock_metrics["downstream_latency_ms"]
        }
        
        import asyncio
        if asyncio.get_event_loop().is_running():
            asyncio.create_task(manager.broadcast(broadcast_payload))
            
        logger.info(f"➕ Successfully injected mock event alert context for service: {payload.service_name}")
        return {"status": "SUCCESS", "message": "Telemetry anomaly successfully appended to the active triage ledger."}
    except Exception as e:
        logger.error(f"Failed to inject direct ledger record: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database record injection failure: {str(e)}")
    
# =====================================================================
# DEEP-DIVE DIAGNOSTIC WORKFLOW NODE (Unified Flat Output)
# =====================================================================
@app.post("/api/events/{event_id}/inspect")
def inspect_event_deep_dive(event_id: int):
    """🟢 FETCHES PRE-CALCULATED METRICS DIRECTLY FLATTENED AT THE ROOT LEVEL"""
    fetch_query = text("SELECT * FROM incident_logs WHERE id = :id AND status = 'ACTIVE'")
    try:
        with engine.connect() as conn:
            row = conn.execute(fetch_query, {"id": event_id}).mappings().fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Selected active telemetry event row not found.")
            
            # Extract archived JSON metrics column values
            metrics_payload = row["metrics"] if "metrics" in row else {}
            if isinstance(metrics_payload, str):
                try:
                    metrics_payload = json.loads(metrics_payload)
                except Exception:
                    metrics_payload = {}

            # Establish safe fallbacks out of the JSON profile
            saturation_pct = metrics_payload.get("saturation_pct", 42)
            system_status = metrics_payload.get("system_status", "HEALTHY")
            blast_radius = metrics_payload.get("blast_radius", ["None Detected"])
            downstream_latency_ms = metrics_payload.get("downstream_latency_ms", 55)
            
            raw_steps = row["remediation_steps"]
            standardized_steps = ["Inspect application log streams manually."]
            
            if raw_steps:
                if isinstance(raw_steps, str):
                    try:
                        standardized_steps = json.loads(raw_steps)
                    except Exception:
                        standardized_steps = [raw_steps]
                elif isinstance(raw_steps, list):
                    standardized_steps = raw_steps
            
            # 🟢 Clean flat structure returning metrics fields right next to identity fields
            return {
                "id": row["id"],
                "service_name": row["service_name"],
                "log_text": row["log_text"],
                "classification": row["classification"] or "Unclassified Operational Alert",
                "severity": row["severity"] or "MEDIUM",
                "remediation_steps": standardized_steps,
                
                # Rendered directly at base root tier for seamless React component matching
                "saturation_pct": saturation_pct,
                "system_status": system_status,
                "blast_radius": blast_radius,
                "downstream_latency_ms": downstream_latency_ms
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Database direct metrics extraction failure: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database direct metrics extraction failure: {str(e)}")
        
# =====================================================================
# DECOUPLED OPERATIONS TIER 1: HIGH-FIDELITY SIMULATION RUNNER
# =====================================================================
@app.post("/api/events/{event_id}/execute-remediation")
def execute_remediation_logs(event_id: int, request: ExecutionRequest):
    """Intercepts blueprint steps and generates high-fidelity production terminal log simulations."""
    terminal_outputs = []
    
    time.sleep(1.2)

    for command in request.remediation_steps:
        cmd_clean = command.strip()
        
        if cmd_clean.startswith("Inspect ") or "manually" in cmd_clean.lower():
            output_log = f"[INFO] Local Operator Notice:\n👉 {cmd_clean}\n[STATUS] Manual assessment trace logged."
            terminal_outputs.append(output_log)
            continue
            
        if "redis-cli" in cmd_clean:
            if "maxclients" in cmd_clean:
                stdout = "OK"
                stderr = ""
            elif "CLIENT KILL" in cmd_clean:
                stdout = "OK (142 connected client pool sockets dropped successfully)"
                stderr = ""
            else:
                stdout = "OK"
                stderr = ""
                
        elif "kubectl" in cmd_clean:
            if "rollout restart" in cmd_clean:
                stdout = "deployment.apps/redis-cluster-node restarted\nWaiting for healthy pod lifecycle sync..."
                stderr = ""
            else:
                stdout = "command execution acknowledged within namespace target cluster context."
                stderr = ""
                
        elif "pg_terminate_backend" in cmd_clean or "psql" in cmd_clean:
            stdout = "pg_terminate_backend\n----------------------\n                    t\n(1 row total connections terminated)"
            stderr = ""
            
        else:
            stdout = f"Execution profile status tracking completed: process returned exit code 0"
            stderr = ""

        output_format = f"$ {cmd_clean}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        terminal_outputs.append(output_format)

    return {"status": "COMPLETED", "logs": terminal_outputs}

# =====================================================================
# DECOUPLED OPERATIONS TIER 2: MANUAL HUMAN VERIFICATION RESOLUTION GATE
# =====================================================================
@app.post("/api/events/{event_id}/resolve")
def human_resolve_event(event_id: int):
    """Explicit manual validation gate. Flips status ledger values to RESOLVED on human approval."""
    logger.info(f"🔒 Received manual validation confirm flag for event record: {event_id}")
    update_query = text("UPDATE incident_logs SET status = 'RESOLVED' WHERE id = :id")
    try:
        with engine.connect() as conn:
            conn.execute(update_query, {"id": event_id})
            conn.commit()
            logger.info(f"✅ Event ledger {event_id} successfully moved out of active feed matrix.")
            return {"status": "SUCCESS", "message": f"Event {event_id} approved and closed."}
    except Exception as e:
        logger.error(f"Failed to update event resolution state ledger: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update event resolution state ledger: {str(e)}")