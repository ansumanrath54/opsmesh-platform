import os
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
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ExecutionRequest(BaseModel):
    remediation_steps: list[str]

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
@app.get("/api/incidents")
def get_all_incidents():
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
# DEEP-DIVE DIAGNOSTIC WORKFLOW NODE
# =====================================================================
@app.post("/api/incidents/{incident_id}/inspect")
def inspect_incident_deep_dive(incident_id: int):
    """Triggers the LangGraph diagnostic sub-agent execution ring for a specific telemetry row."""
    fetch_query = text("SELECT * FROM incident_logs WHERE id = :id AND status = 'ACTIVE'")
    try:
        with engine.connect() as conn:
            row = conn.execute(fetch_query, {"id": incident_id}).mappings().fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Selected active incident row not found.")
            
            initial_diag_state = DiagnosticState(
                incident_id=row["id"],
                service_name=row["service_name"],
                log_text=row["log_text"]
            )
            
            analysis_result = diagnostic_graph.invoke(initial_diag_state)
            
            return {
                "id": row["id"],
                "service_name": row["service_name"],
                "log_text": row["log_text"],
                "classification": row["classification"],
                "severity": row["severity"],
                "remediation_steps": row["remediation_steps"],
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
@app.post("/api/incidents/{incident_id}/execute-remediation")
def execute_remediation_logs(incident_id: int, request: ExecutionRequest):
    """Intercepts blueprint steps and generates high-fidelity production terminal log simulations."""
    terminal_outputs = []
    
    # Intentionally add a slight telemetry lag delay to simulate server communication during demo
    time.sleep(1.2)

    for command in request.remediation_steps:
        # Base string normalization
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

        # Format output matching authentic server terminal output parameters
        output_format = f"$ {cmd_clean}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        terminal_outputs.append(output_format)

    return {"status": "COMPLETED", "logs": terminal_outputs}

# =====================================================================
# DECOUPLED OPERATIONS TIER 2: MANUAL HUMAN VERIFICATION RESOLUTION GATE
# =====================================================================
@app.post("/api/incidents/{incident_id}/resolve")
def human_resolve_incident(incident_id: int):
    """Explicit manual validation gate. Flips status ledger values to RESOLVED on human approval."""
    logger.info(f"🔒 Received manual validation confirm flag for incident record: {incident_id}")
    update_query = text("UPDATE incident_logs SET status = 'RESOLVED' WHERE id = :id")
    try:
        with engine.connect() as conn:
            conn.execute(update_query, {"id": incident_id})
            conn.commit()
            logger.info(f"✅ Incident ledger {incident_id} successfully moved out of active feed matrix.")
            return {"status": "SUCCESS", "message": f"Incident {incident_id} approved and closed."}
    except Exception as e:
        logger.error(f"Failed to update incident resolution state ledger: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update incident resolution state ledger: {str(e)}")