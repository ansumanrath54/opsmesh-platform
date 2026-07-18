import os
import logging
from sqlalchemy import create_engine, Column, String, Text, DateTime, Integer, ARRAY, text
from sqlalchemy.dialects.postgresql import JSONB # 🟢 1. Import native PostgreSQL JSONB type
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

logger = logging.getLogger("OpsMesh.Database")

DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://root_db_user:framework_master_password_2026@localhost:5432/opsmesh_analytics_datastore"
)
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class EventRecord(Base):
    """SQLAlchemy model for persistent logging of AI-remediated telemetry events."""
    __tablename__ = 'incident_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    service_name = Column(String(100), nullable=False)
    log_text = Column(Text, nullable=False)
    classification = Column(String(100))
    severity = Column(String(20))
    remediation_steps = Column(ARRAY(String))
    status = Column(String(20), default="ACTIVE")
    metrics = Column(JSONB, default={}) # 🟢 2. Add native JSONB metrics column schema mapping

# =====================================================================
# ENFORCED ISOLATED SCHEMA MIGRATION LOOPS
# =====================================================================
try:
    with engine.connect() as migration_conn:
        # 1. Force structural DDL alteration pass first for 'status' column
        migration_conn.execute(text("""
            ALTER TABLE incident_logs 
            ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'ACTIVE';
        """))
        
        # 🟢 3. Force structural DDL alteration pass for 'metrics' JSONB column mapping
        migration_conn.execute(text("""
            ALTER TABLE incident_logs 
            ADD COLUMN IF NOT EXISTS metrics JSONB DEFAULT '{}'::jsonb;
        """))
        
        migration_conn.commit()
        logger.info("🔧 Database schema patched successfully: 'status' and 'metrics' fields verified.")
except Exception as migration_error:
    logger.warning(f"⚠️ Inline migration skipped or handled: {str(migration_error)}")

# 2. Safely pre-bind metadata mappings safely
try:
    Base.metadata.create_all(bind=engine)
except Exception as e:
    logger.warning(f"Database pre-binding skipped: {str(e)}")