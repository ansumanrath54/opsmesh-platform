import os
import logging
from sqlalchemy import create_engine, Column, String, Text, DateTime, Integer, JSON, text
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

class IncidentRecord(Base):
    """SQLAlchemy model for persistent logging of AI-remediated events."""
    __tablename__ = 'incident_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    service_name = Column(String(100), nullable=False)
    log_text = Column(Text, nullable=False)
    classification = Column(String(100))
    severity = Column(String(20))
    remediation_steps = Column(JSON)  
    status = Column(String(20), default="ACTIVE")

# =====================================================================
# ENFORCED ISOLATED SCHEMA MIGRATION LOOPS
# =====================================================================
try:
    with engine.connect() as migration_conn:
        # 1. Force structural DDL alteration pass first
        migration_conn.execute(text("""
            ALTER TABLE incident_logs 
            ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'ACTIVE';
        """))
        migration_conn.commit()
        logger.info("🔧 Database schema patched successfully: 'status' column verified.")
except Exception as migration_error:
    logger.warning(f"⚠️ Inline migration skipped or handled: {str(migration_error)}")

# 2. Safely pre-bind metadata mappings safely
try:
    Base.metadata.create_all(bind=engine)
except Exception as e:
    logger.warning(f"Database pre-binding skipped: {str(e)}")