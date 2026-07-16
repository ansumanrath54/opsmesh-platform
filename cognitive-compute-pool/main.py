import os
import json
import logging
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

# Import isolated database layout structures cleanly
from database import engine, SessionLocal, IncidentRecord

# Import modern Direct Google GenAI SDK & Graph components
from google import genai
from google.genai import types
from langgraph.graph import StateGraph, END
from pydantic import BaseModel, Field
from typing import List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("OpsMesh.Worker")

load_dotenv()

# =====================================================================
# PHASE 4: COGNITIVE AGENT COMPUTE LAYER (LangGraph & Pydantic)
# =====================================================================
class IncidentState(BaseModel):
    service_name: str
    log_text: str
    classification: Optional[str] = None
    remediation_steps: List[str] = Field(default_factory=list)
    severity: Optional[str] = None

class LLMOutputSchema(BaseModel):
    classification: str = Field(description="High-level operational failure category")
    severity: str = Field(description="Severity rating exactly as LOW, MEDIUM, HIGH, CRITICAL")
    remediation_steps: List[str] = Field(description="Sequential list of explicit remediation commands.")

def analyze_incident_node(state: IncidentState) -> dict:
    client = genai.Client()
    prompt = (
        f"You are an expert autonomous ITOps engineer analyzing an infrastructure error.\n"
        f"Service Target: {state.service_name}\n"
        f"Log Telemetry Signature: {state.log_text}\n\n"
        f"Analyze the log text and determine classification, severity, and remediation steps."
    )
    try:
        response = client.models.generate_content(
            model='gemini-3.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=LLMOutputSchema,
                temperature=0.0,
                http_options={'timeout': 8.0}
            ),
        )
        parsed_output = LLMOutputSchema.model_validate_json(response.text)
        return {
            "classification": parsed_output.classification,
            "severity": parsed_output.severity.upper(),
            "remediation_steps": parsed_output.remediation_steps
        }
    except Exception as e:
        logger.warning(f"⚠️ API exception hit ({type(e).__name__}). Deploying Local Heuristic Engine.")
        log_upper = state.log_text.upper()
        classification = "Unclassified Operational Alert"
        severity = "MEDIUM"
        remediation_steps = ["Inspect application log streams manually."]
        
        if "REDIS" in log_upper:
            classification = "Redis Connection Pool Exhaustion Anomaly"
            severity = "CRITICAL" if "CRITICAL" in log_upper else "HIGH"
            remediation_steps = [
                "redis-cli -h localhost -p 6379 CONFIG SET maxclients 20000",
                "redis-cli CLIENT KILL TYPE normal",
                "kubectl rollout restart deployment/redis-cluster-node"
            ]
        elif "POSTGRES" in log_upper or "DB_" in log_upper or "DATABASE" in log_upper:
            classification = "Relational DB Pool Connection Timeout"
            severity = "CRITICAL" if "CRITICAL" in log_upper else "HIGH"
            remediation_steps = [
                "psql -U postgres -d opsmesh -c \"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE age(clock_timestamp() - query_start) > interval '5 minutes';\"",
                "kubectl scale deployment/postgres-cluster-pool --replicas=3"
            ]
        elif "CRITICAL" in log_upper:
            severity = "CRITICAL"
            classification = f"Critical Infrastructure Exception ({state.service_name})"
            remediation_steps = ["Check core microservice engine status metrics immediately."]
            
        return {"classification": classification, "severity": severity, "remediation_steps": remediation_steps}

workflow = StateGraph(IncidentState)
workflow.add_node("analyzer", analyze_incident_node)
workflow.set_entry_point("analyzer")
workflow.add_edge("analyzer", END)
agent_graph = workflow.compile()

# =====================================================================
# PHASE 5: MICRO-DIAGNOSTIC SUB-GRAPH WORKFLOW
# =====================================================================
class DiagnosticState(BaseModel):
    incident_id: int
    service_name: str
    log_text: str
    saturation_pct: int = 0
    blast_radius: List[str] = Field(default_factory=list)
    system_status: str = "Unknown"
    downstream_latency_ms: int = 0

class DiagnosticOutputSchema(BaseModel):
    saturation_pct: int = Field(description="Calculated resource pool saturation percentage")
    blast_radius: List[str] = Field(description="Downstream microservices impacted")
    system_status: str = Field(description="Calculated operational health state")
    downstream_latency_ms: int = Field(description="Added latency penalty hitting API gateway")

def run_deep_diagnostics(state: DiagnosticState) -> dict:
    client = genai.Client()
    prompt = f"Perform deep infrastructure diagnostics for {state.service_name} given: {state.log_text}"
    try:
        response = client.models.generate_content(
            model='gemini-3.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=DiagnosticOutputSchema,
                temperature=0.0,
                http_options={'timeout': 6.0}
            ),
        )
        parsed = DiagnosticOutputSchema.model_validate_json(response.text)
        return {
            "saturation_pct": parsed.saturation_pct,
            "blast_radius": parsed.blast_radius,
            "system_status": parsed.system_status.upper(),
            "downstream_latency_ms": parsed.downstream_latency_ms
        }
    except Exception:
        log_upper = state.log_text.upper()
        sat, status, radius, latency = 75, "DEGRADED", ["None Detected"], 45
        if "REDIS" in log_upper:
            sat, status, radius, latency = 100, "CRITICAL", ["Payment-Processing-Worker", "User-Session-Cache"], 480
        elif "POSTGRES" in log_upper or "DB_" in log_upper or "DATABASE" in log_upper:
            sat, status, radius, latency = 95, "CRITICAL", ["Order-Management-Service", "Inventory-Service"], 1200
        return {"saturation_pct": sat, "blast_radius": radius, "system_status": status, "downstream_latency_ms": latency}

diag_workflow = StateGraph(DiagnosticState)
diag_workflow.add_node("diagnostic_runner", run_deep_diagnostics)
diag_workflow.set_entry_point("diagnostic_runner")
diag_workflow.add_edge("diagnostic_runner", END)
diagnostic_graph = diag_workflow.compile()

# =====================================================================
# INGESTION BOUNDARY & CONSUMER RUNTIME LOOPS
# =====================================================================
def save_incident_to_db(service: str, log: str, result: dict):
    db = SessionLocal()
    try:
        record = IncidentRecord(
            service_name=service,
            log_text=log,
            classification=result.get("classification"),
            severity=result.get("severity"),
            remediation_steps=result.get("remediation_steps"),
            status="ACTIVE"
        )
        db.add(record)
        db.commit()
        logger.info("💾 Incident record successfully persisted to PostgreSQL archive.")
    except Exception as e:
        logger.error(f"Database commit failed: {str(e)}")
        db.rollback()
    finally:
        db.close()

def initialize_consumer() -> Consumer:
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    config = {'bootstrap.servers': kafka_servers, 'group.id': 'opsmesh-cognitive-workers', 'auto.offset.reset': 'earliest', 'enable.auto.commit': False}
    try:
        consumer = Consumer(config)
        consumer.subscribe(['telemetry-events'])
        logger.info(f"Successfully subscribed to telemetry-events topic on {kafka_servers}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to initialize Kafka consumer: {str(e)}")
        raise

def process_message(payload_str: str):
    try:
        payload = json.loads(payload_str)
        service_name = payload.get('serviceName', 'Unknown')
        log_text = payload.get('logText')
        if log_text:
            initial_state = IncidentState(service_name=service_name, log_text=log_text)
            result = agent_graph.invoke(initial_state)
            save_incident_to_db(service_name, log_text, result)
    except json.JSONDecodeError:
        logger.error("Failed to parse incoming log payload string to JSON.")

def start_worker_loop():
    consumer = initialize_consumer()
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF: continue
                else: raise KafkaException(msg.error())
            process_message(msg.value().decode('utf-8'))
            consumer.commit(asynchronous=True)
    except KeyboardInterrupt:
        logger.info("Shutdown signal caught.")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_worker_loop()