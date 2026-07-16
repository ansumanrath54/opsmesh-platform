import os
import json
import logging
import time
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from pydantic import BaseModel
from dotenv import load_dotenv

# Import isolated database connection profiles cleanly
from database import engine

# Import the shared LangGraph variables from main.py
from main import diagnostic_graph, DiagnosticState

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
# LEDGER INGEST BOUNDARY
# =====================================================================
@app.get("/api/events")
def get_all_events():
    """Fetches the active telemetry ledger structured chronologically."""
    query = text("SELECT * FROM incident_logs WHERE status = 'ACTIVE' ORDER BY timestamp DESC")
    try:
        with engine.connect() as conn:
            rows = conn.execute(query).mappings().fetchall()
            
            standardized_list = []
            for row in rows:
                standardized_list.append({
                    "id": row["id"],
                    "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None,
                    "service_name": row["service_name"],
                    "log_text": row["log_text"],
                    "classification": row["classification"] or "Unclassified Operational Alert",
                    "severity": row["severity"] or "MEDIUM",
                    "remediation_steps": row["remediation_steps"] or ["Inspect application log streams manually."]
                })
            return standardized_list
    except Exception as e:
        logger.error(f"Ledger extraction failure: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ledger extraction failure: {str(e)}")

# =====================================================================
# LEDGER INJECTION BOUNDARY
# =====================================================================
@app.post("/api/events")
def create_mock_event(payload: EventCreateRequest):
    """Direct injector endpoint to create custom severity logging contexts for verification demos."""
    insert_query = text("""
        INSERT INTO incident_logs (service_name, severity, log_text, classification, status, remediation_steps, timestamp)
        VALUES (:service, :sev, :log, :class, 'ACTIVE', :steps, NOW())
    """)
    
    log_upper = payload.log_text.upper()
    classification = "Unclassified Operational Alert"
    remediation_steps = ["Inspect application log streams manually."]
    
    if "REDIS" in log_upper:
        classification = "Redis Eviction Warning (Memory Pressure)" if payload.severity.upper() == "MEDIUM" else "Redis Connection Pool Exhaustion Anomaly"
        remediation_steps = [
            "redis-cli -h localhost -p 6379 CONFIG SET maxclients 20000",
            "redis-cli CLIENT KILL TYPE normal",
            "kubectl rollout restart deployment/redis-cluster-node"
        ]
    elif "POSTGRES" in log_upper or "DB_" in log_upper or "DATABASE" in log_upper:
        classification = "Relational DB Pool Connection Timeout"
        remediation_steps = [
            "psql -U postgres -d opsmesh -c \"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE age(clock_timestamp() - query_start) > interval '5 minutes';\"",
            "kubectl scale deployment/postgres-cluster-pool --replicas=3"
        ]
        
    try:
        with engine.connect() as conn:
            conn.execute(insert_query, {
                "service": payload.service_name,
                "sev": payload.severity.upper(),
                "log": payload.log_text,
                "class": classification,
                # 🟢 Crucial Fix: Serialize list data to a JSON string payload mapping context
                "steps": json.dumps(remediation_steps) 
            })
            conn.commit()
        logger.info(f"➕ Successfully injected mock event alert context for service: {payload.service_name}")
        return {"status": "SUCCESS", "message": "Telemetry anomaly successfully appended to the active triage ledger."}
    except Exception as e:
        logger.error(f"Failed to inject direct ledger record: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database record injection failure: {str(e)}")
    
# =====================================================================
# DEEP-DIVE DIAGNOSTIC WORKFLOW NODE
# =====================================================================
@app.post("/api/events/{event_id}/inspect")
def inspect_event_deep_dive(event_id: int):
    fetch_query = text("SELECT * FROM incident_logs WHERE id = :id AND status = 'ACTIVE'")
    try:
        with engine.connect() as conn:
            row = conn.execute(fetch_query, {"id": event_id}).mappings().fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Selected active telemetry event row not found.")
            
            initial_diag_state = DiagnosticState(
                incident_id=row["id"],
                service_name=row["service_name"],
                log_text=row["log_text"]
            )
            analysis_result = diagnostic_graph.invoke(initial_diag_state)
            
            # --- SAFE ARRAY PARSING MATRIX ---
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
            
            return {
                "id": row["id"],
                "service_name": row["service_name"],
                "log_text": row["log_text"],
                "classification": row["classification"] or "Unclassified Operational Alert",
                "severity": row["severity"] or "MEDIUM",
                "remediation_steps": standardized_steps, # 🟢 Guaranteed clean list array sent to React
                "metrics": {
                    "saturation_pct": analysis_result.get("saturation_pct"),
                    "system_status": analysis_result.get("system_status"),
                    "blast_radius": analysis_result.get("blast_radius"),
                    "downstream_latency_ms": analysis_result.get("downstream_latency_ms")
                }
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Sub-agent orchestration routing failure: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Sub-agent orchestration routing failure: {str(e)}")
    
# =====================================================================
# DECOUPLED OPERATIONS TIER 1: HIGH-FIDELITY SIMULATION RUNNER
# =====================================================================
@app.post("/api/events/{event_id}/execute-remediation")
def execute_remediation_logs(event_id: int, request: ExecutionRequest):
    """Intercepts blueprint steps and generates high-fidelity production terminal log simulations."""
    terminal_outputs = []
    
    # Intentionally add a slight telemetry lag delay to simulate server communication during demo
    time.sleep(1.2)

    for command in request.remediation_steps:
        cmd_clean = command.strip()
        
        # 1. Catch Text Fallbacks
        if cmd_clean.startswith("Inspect ") or "manually" in cmd_clean.lower():
            output_log = f"[INFO] Local Operator Notice:\n👉 {cmd_clean}\n[STATUS] Manual assessment trace logged."
            terminal_outputs.append(output_log)
            continue
            
        # 2. Simulate Redis Automation Sequences
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
                
        # 3. Simulate Kubernetes Operations Sequences
        elif "kubectl" in cmd_clean:
            if "rollout restart" in cmd_clean:
                stdout = "deployment.apps/redis-cluster-node restarted\nWaiting for healthy pod lifecycle sync..."
                stderr = ""
            else:
                stdout = "command execution acknowledged within namespace target cluster context."
                stderr = ""
                
        # 4. Simulate Relational Database Operations Sequences
        elif "pg_terminate_backend" in cmd_clean or "psql" in cmd_clean:
            stdout = "pg_terminate_backend\n----------------------\n                    t\n(1 row total connections terminated)"
            stderr = ""
            
        # 5. Default Fallback String Output
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