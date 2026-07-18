import React, { useState, useEffect, useRef } from 'react';

// Dynamically reads your Render production domain environment variable or defaults to localhost
const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export default function App() {
  const [metrics, setMetrics] = useState({ totalEvents: 0, criticalTriggers: 0, highAlerts: 0, pipelineStatus: '🟢 Active' });
  const [events, setEvents] = useState([]);
  const [selectedEvent, setSelectedEvent] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [inspectLoading, setInspectLoading] = useState(false);
  const [isExecuting, setIsExecuting] = useState(false);
  const [terminalLogs, setTerminalLogs] = useState([]);
  const [scriptRunFinished, setScriptRunFinished] = useState(false);
  const [isResolving, setIsResolving] = useState(false);

  const inspectorPanelRef = useRef(null);

  // Ingest boundary: pulls real-time metric aggregations and active triage row feeds
  const fetchOpsMeshData = async () => {
    try {
      const [metricsRes, eventsRes] = await Promise.all([
        fetch(`${API_BASE_URL}/api/metrics`),
        fetch(`${API_BASE_URL}/api/events`)
      ]);

      if (!metricsRes.ok || !eventsRes.ok) throw new Error('Network response failure.');

      const metricsData = await metricsRes.json();
      const eventsData = await eventsRes.json();

      setMetrics(metricsData);
      setEvents(eventsData);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  // Diagnostic Sub-Agent execution selector logic mapping
  const handleInspectClick = async (eventId) => {
    setInspectLoading(true);
    setTerminalLogs([]);
    setScriptRunFinished(false);
    try {
      const response = await fetch(`${API_BASE_URL}/api/events/${eventId}/inspect`, {
        method: 'POST'
      });
      if (!response.ok) throw new Error('Failed to run sub-agent dynamic diagnostic workflows.');
      const deepDiveData = await response.json();
      setSelectedEvent(deepDiveData);
      
      setTimeout(() => {
        if (inspectorPanelRef.current) {
          inspectorPanelRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
      }, 100);
    } catch (err) {
      setError(`Diagnostic Agent Failure: ${err.message}`);
    } finally {
      setInspectLoading(false);
    }
  };

  // Operations Tier 1 Playbook Automation Execution Engine
  const runRemediationScripts = async () => {
    setIsExecuting(true);
    setScriptRunFinished(false);
    setTerminalLogs(["[System] Connecting to shell runner container pipeline engine..."]);
    
    try {
      const response = await fetch(`${API_BASE_URL}/api/events/${selectedEvent.id}/execute-remediation`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ remediation_steps: selectedEvent.remediation_steps })
      });
      
      const data = await response.json();
      setTerminalLogs(data.logs || ["[Error] No diagnostic terminal outputs returned."]);
      setScriptRunFinished(true);
    } catch (err) {
      setError(`Script execution loop failed: ${err.message}`);
    } finally {
      setIsExecuting(false);
    }
  };

  // Operations Tier 2: Manual Human Verification Resolution Gate Configuration
  const markEventAsResolved = async () => {
    setIsResolving(true);
    try {
      const response = await fetch(`${API_BASE_URL}/api/events/${selectedEvent.id}/resolve`, {
        method: 'POST'
      });
      
      if (!response.ok) throw new Error('Failed to post resolution state token to database ledger.');
      
      setSelectedEvent(null);
      setTerminalLogs([]);
      setScriptRunFinished(false);
      fetchOpsMeshData();
    } catch (err) {
      setError(`Resolution confirmation failed: ${err.message}`);
    } finally {
      setIsResolving(false);
    }
  };

  useEffect(() => {
    fetchOpsMeshData();
    const interval = setInterval(fetchOpsMeshData, 4000); 
    return () => clearInterval(interval);
  }, []);

  if (loading && events.length === 0) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh', backgroundColor: '#0f172a', color: '#fff' }}>
        <h3>Initializing OpsMesh Interface Nodes...</h3>
      </div>
    );
  }

  // 🟢 Extract the flat root-level variables out of the selected event state payload safely
  const saturationPct = selectedEvent?.saturation_pct ?? 0;
  const systemStatus = selectedEvent?.system_status || 'UNKNOWN';
  const downstreamLatencyMs = selectedEvent?.downstream_latency_ms ?? 0;
  const blastRadius = selectedEvent?.blast_radius || [];

  return (
    <div style={{ minHeight: '100vh', backgroundColor: '#0f172a', color: '#f8fafc', fontFamily: 'system-ui, sans-serif', padding: '2rem' }}>
      
      <header style={{ marginBottom: '2rem', borderBottom: '1px solid #334155', paddingBottom: '1.5rem' }}>
        <h1 style={{ fontSize: '2.25rem', fontWeight: 'bold', color: '#38bdf8', margin: 0 }}>🤖 OpsMesh Cognitive Compute Control Panel</h1>
        <p style={{ color: '#94a3b8', marginTop: '0.5rem', fontSize: '1.1rem' }}>Decoupled Microservice Telemetry Data Engine Interface</p>
      </header>

      {error && (
        <div style={{ backgroundColor: '#7f1d1d', border: '1px solid #f87171', color: '#fca5a5', padding: '1rem', borderRadius: '0.5rem', marginBottom: '2rem' }}>
          <strong>Telemetry Sync Exception:</strong> {error}
          <button onClick={() => setError(null)} style={{ float: 'right', background: 'none', border: 'none', color: '#fff', cursor: 'pointer', fontWeight: 'bold' }}>✕</button>
        </div>
      )}

      {/* Metrics Counters Grid Component Layout */}
      <section style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '1.5rem', marginBottom: '2.5rem' }}>
        <div style={{ backgroundColor: '#1e293b', padding: '1.5rem', borderRadius: '0.75rem', border: '1px solid #334155' }}>
          <div style={{ color: '#94a3b8', fontSize: '0.875rem', fontWeight: '600', textTransform: 'uppercase' }}>Total Events Handled</div>
          <div style={{ fontSize: '2rem', fontWeight: 'bold', marginTop: '0.5rem' }}>{metrics.totalEvents}</div>
        </div>
        <div style={{ backgroundColor: '#1e293b', padding: '1.5rem', borderRadius: '0.75rem', border: '1px solid #334155', borderLeft: '4px solid #ef4444' }}>
          <div style={{ color: '#ef4444', fontSize: '0.875rem', fontWeight: '600', textTransform: 'uppercase' }}>Critical Triggers</div>
          <div style={{ fontSize: '2rem', fontWeight: 'bold', marginTop: '0.5rem', color: '#ef4444' }}>{metrics.criticalTriggers}</div>
        </div>
        <div style={{ backgroundColor: '#1e293b', padding: '1.5rem', borderRadius: '0.75rem', border: '1px solid #334155', borderLeft: '4px solid #f97316' }}>
          <div style={{ color: '#f97316', fontSize: '0.875rem', fontUk: '600', textTransform: 'uppercase' }}>High Alerts</div>
          <div style={{ fontSize: '2rem', fontWeight: 'bold', marginTop: '0.5rem', color: '#f97316' }}>{metrics.highAlerts}</div>
        </div>
        <div style={{ backgroundColor: '#1e293b', padding: '1.5rem', borderRadius: '0.75rem', border: '1px solid #334155' }}>
          <div style={{ color: '#4ade80', fontSize: '0.875rem', fontWeight: '600', textTransform: 'uppercase' }}>Gateway Status</div>
          <div style={{ fontSize: '1.5rem', fontWeight: 'bold', marginTop: '0.75rem', color: '#4ade80' }}>{metrics.pipelineStatus}</div>
        </div>
      </section>

      {/* Primary Triage Ledger Feed Grid */}
      <section style={{ marginBottom: '2.5rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1rem' }}>
          <h3 style={{ fontSize: '1.5rem', fontWeight: 'bold', margin: 0, color: '#e2e8f0' }}>📋 Active Event Triage Feed</h3>
          {inspectLoading && <span style={{ color: '#38bdf8', fontSize: '0.9rem', fontWeight: 'bold' }}>⚡ Sub-Agent orchestrating analytics...</span>}
        </div>
        <div style={{ overflowX: 'auto', backgroundColor: '#1e293b', borderRadius: '0.75rem', border: '1px solid #334155' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left' }}>
            <thead>
              <tr style={{ borderBottom: '2px solid #334155', backgroundColor: '#0f172a', color: '#94a3b8', fontSize: '0.875rem' }}>
                <th style={{ padding: '1rem' }}>TIMESTAMP</th>
                <th style={{ padding: '1rem' }}>TARGET SERVICE</th>
                <th style={{ padding: '1rem' }}>SEVERITY</th>
                <th style={{ padding: '1rem' }}>CLASSIFICATION</th>
                <th style={{ padding: '1rem', textAlign: 'right' }}>ACTION</th>
              </tr>
            </thead>
            <tbody>
              {events.map((event) => {
                const isCritical = event.severity === 'CRITICAL';
                const isHigh = event.severity === 'HIGH';
                let rowBg = selectedEvent?.id === event.id ? '#2563eb4f' : 'transparent';

                return (
                  <tr key={event.id} style={{ borderBottom: '1px solid #334155', backgroundColor: rowBg, transition: 'background-color 0.2s' }}>
                    <td style={{ padding: '1rem', fontSize: '0.9rem', color: '#cbd5e1' }}>{new Date(event.timestamp).toLocaleString()}</td>
                    <td style={{ padding: '1rem', fontWeight: '600' }}>{event.service_name}</td>
                    <td style={{ padding: '1rem' }}>
                      <span style={{ 
                        padding: '0.25rem 0.6rem', borderRadius: '0.25rem', fontSize: '0.75rem', fontWeight: 'bold',
                        backgroundColor: isCritical ? '#7f1d1d' : isHigh ? '#7c2d12' : '#1e3a8a',
                        color: isCritical ? '#fca5a5' : isHigh ? '#fed7aa' : '#bfdbfe'
                      }}>{event.severity}</span>
                    </td>
                    <td style={{ padding: '1rem', color: '#94a3b8' }}>{event.classification}</td>
                    <td style={{ padding: '1rem', textAlign: 'right' }}>
                      <button onClick={() => handleInspectClick(event.id)} style={{ backgroundColor: '#0284c7', color: '#fff', border: 'none', padding: '0.4rem 0.8rem', borderRadius: '0.375rem', cursor: 'pointer', fontWeight: '500' }}>
                        Inspect
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>

      {/* Dynamic Sub-Agent Diagnostic Workspace Panel Display */}
      {selectedEvent && (
        <section ref={inspectorPanelRef} style={{ borderTop: '2px solid #38bdf8', paddingTop: '2rem', marginTop: '1rem' }}>
          <div style={{ display: 'flex', justifycontent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
            <h3 style={{ fontSize: '1.5rem', fontWeight: 'bold', margin: 0, color: '#38bdf8' }}>🔍 Deep Cognitive Inspector Layer</h3>
            <button onClick={() => setSelectedEvent(null)} style={{ backgroundColor: '#334155', border: 'none', color: '#cbd5e1', padding: '0.4rem 0.8rem', borderRadius: '0.375rem', cursor: 'pointer' }}>
              Clear Workspace Selection
            </button>
          </div>
          
          <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
            
            {/* 🟢 Top Workspace Bar: Sub-Agent Structural Metrics (Now cleanly mapped to flat fields) */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '1.5rem', backgroundColor: '#131d31', padding: '1.25rem', borderRadius: '0.75rem', border: '1px solid #1e293b' }}>
              <div>
                <div style={{ color: '#94a3b8', fontSize: '0.8rem', fontWeight: '600', textTransform: 'uppercase' }}>Sub-Agent Health Check</div>
                <span style={{ 
                  display: 'inline-block', marginTop: '0.25rem', padding: '0.2rem 0.5rem', borderRadius: '0.25rem', fontSize: '0.85rem', fontWeight: 'bold',
                  backgroundColor: systemStatus === 'CRITICAL' ? '#7f1d1d' : systemStatus === 'WARNING' ? '#7c2d12' : '#064e3b',
                  color: systemStatus === 'CRITICAL' ? '#fca5a5' : systemStatus === 'WARNING' ? '#fed7aa' : '#a7f3d0'
                }}>{systemStatus}</span>
              </div>
              <div>
                <div style={{ color: '#94a3b8', fontSize: '0.8rem', fontWeight: '600', textTransform: 'uppercase' }}>Resource Saturation</div>
                <div style={{ fontSize: '1.35rem', fontWeight: 'bold', marginTop: '0.25rem', color: saturationPct >= 90 ? '#f87171' : saturationPct >= 60 ? '#fb923c' : '#4ade80' }}>
                  {saturationPct}%
                </div>
              </div>
              <div>
                <div style={{ color: '#94a3b8', fontSize: '0.8rem', fontWeight: '600', textTransform: 'uppercase' }}>Downstream Latency</div>
                <div style={{ fontSize: '1.35rem', fontWeight: 'bold', marginTop: '0.25rem', color: downstreamLatencyMs >= 400 ? '#f87171' : '#cbd5e1' }}>+{downstreamLatencyMs}ms</div>
              </div>
              <div>
                <div style={{ color: '#94a3b8', fontSize: '0.8rem', fontWeight: '600', textTransform: 'uppercase' }}>Blast Radius Impact</div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.35rem', marginTop: '0.35rem' }}>
                  {blastRadius.length > 0 && blastRadius[0] !== "None Detected" ? blastRadius.map((srv, idx) => (
                    <span key={idx} style={{ backgroundColor: '#1e293b', border: '1px solid #334155', color: '#cbd5e1', padding: '0.15rem 0.4rem', borderRadius: '0.25rem', fontSize: '0.75rem', fontFamily: 'monospace' }}>
                      {srv}
                    </span>
                  )) : <span style={{ color: '#64748b', fontSize: '0.85rem', fontStyle: 'italic' }}>None Detected</span>}
                </div>
              </div>
            </div>

            {/* Bottom Split Layout Grid Focus Zones */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(450px, 1fr))', gap: '2rem' }}>
              
              {/* Left Column Focus Zone: System Trace Log */}
              <div style={{ backgroundColor: '#1e293b', padding: '1.5rem', borderRadius: '0.75rem', border: '1px solid #334155' }}>
                <h4 style={{ margin: '0 0 1rem 0', color: '#38bdf8', fontSize: '1.1rem' }}>Raw Error Signature ({selectedEvent.service_name})</h4>
                <pre style={{ backgroundColor: '#0f172a', padding: '1rem', borderRadius: '0.5rem', overflowX: 'auto', color: '#f87171', fontFamily: 'monospace', fontSize: '0.9rem', border: '1px solid #1e293b', whiteSpace: 'pre-wrap' }}>
                  {selectedEvent.log_text}
                </pre>
              </div>

              {/* Right Column Focus Zone: AI Blueprint Control Operations */}
              <div style={{ backgroundColor: '#1e293b', padding: '1.5rem', borderRadius: '0.75rem', border: '1px solid #334155', display: 'flex', flexDirection: 'column', justifyContent: 'space-between' }}>
                <div>
                  <h4 style={{ margin: '0 0 1rem 0', color: '#4ade80', fontSize: '1.1rem' }}>🤖 Gemini Remediation Blueprint</h4>
                  <div style={{ marginBottom: '1rem' }}>
                    <span style={{ color: '#94a3b8', fontSize: '0.9rem' }}>AI Classification Route: </span>
                    <span style={{ fontWeight: '600', color: '#f1f5f9' }}>{selectedEvent.classification}</span>
                  </div>
                  <div style={{ color: '#e2e8f0', fontWeight: '600', marginBottom: '0.5rem', fontSize: '0.95rem' }}>Recommended Execution Sequences:</div>
                  <ul style={{ listStyleType: 'none', padding: 0, margin: 0 }}>
                    {Array.isArray(selectedEvent.remediation_steps) ? (
                      selectedEvent.remediation_steps.map((step, index) => (
                        <li key={index} style={{ display: 'flex', alignItems: 'flex-start', marginBottom: '0.75rem', backgroundColor: '#0f172a', padding: '0.75rem', borderRadius: '0.375rem', border: '1px solid #334155' }}>
                          <span style={{ backgroundColor: '#22c55e', color: '#0f172a', fontWeight: 'bold', borderRadius: '50%', minWidth: '1.25rem', height: '1.25rem', display: 'flex', justifyContent: 'center', alignItems: 'center', fontSize: '0.75rem', marginRight: '0.75rem', marginTop: '0.1rem' }}>
                            {index + 1}
                          </span>
                          <span style={{ fontFamily: 'monospace', color: '#cbd5e1', fontSize: '0.9rem' }}>{step}</span>
                        </li>
                      ))
                    ) : (
                      <li style={{ fontFamily: 'monospace', color: '#cbd5e1' }}>{String(selectedEvent.remediation_steps)}</li>
                    )}
                  </ul>

                  {/* Operational Subprocess Terminal Shell Output Window Display Log Component */}
                  {terminalLogs.length > 0 && (
                    <div style={{ marginTop: '1.5rem', backgroundColor: '#020617', border: '1px solid #1e293b', padding: '0.75rem', borderRadius: '0.375rem', fontFamily: 'monospace', fontSize: '0.85rem' }}>
                      <div style={{ color: '#64748b', borderBottom: '1px solid #1e293b', paddingBottom: '0.25rem', marginBottom: '0.5rem', fontWeight: 'bold' }}>📟 LIVE CONTAINER TEST RUN TERMINAL LOGS</div>
                      <pre style={{ margin: 0, color: '#cbd5e1', whiteSpace: 'pre-wrap', maxHeight: '200px', overflowY: 'auto' }}>
                        {terminalLogs.join('\n\n')}
                      </pre>
                    </div>
                  )}
                </div>

                {/* Two-Tier Decoupled Operational Control Buttons Layer */}
                <div style={{ marginTop: '1.5rem', borderTop: '1px solid #334155', paddingTop: '1.25rem', display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                  
                  {/* TIER 1: RUN LOG FETCH ENGINE */}
                  <button
                    onClick={runRemediationScripts}
                    disabled={isExecuting || isResolving}
                    style={{
                      width: '100%', padding: '0.75rem', borderRadius: '0.5rem', border: 'none', fontWeight: 'bold', fontSize: '0.95rem', cursor: (isExecuting || isResolving) ? 'not-allowed' : 'pointer', transition: 'all 0.2s',
                      backgroundColor: isExecuting ? '#475569' : '#0284c7', color: '#fff'
                    }}
                  >
                    {isExecuting ? '⏳ Running Infrastructure Script Loop...' : '⚡ Step 1: Execute Playbook Shell Commands'}
                  </button>

                  {/* TIER 2: MANUAL HUMAN CONFIRMATION VERIFICATION GATE */}
                  {scriptRunFinished && (
                    <button
                      onClick={markEventAsResolved}
                      disabled={isResolving}
                      style={{
                        width: '100%', padding: '0.75rem', borderRadius: '0.5rem', border: 'none', fontWeight: 'bold', fontSize: '0.95rem', cursor: isResolving ? 'not-allowed' : 'pointer', transition: 'all 0.2s',
                        backgroundColor: '#22c55e', color: '#0f172a', boxShadow: '0 4px 6px rgba(34, 197, 94, 0.2)'
                      }}
                    >
                      {isResolving ? '⏳ Archiving Record Ledger...' : '✅ Step 2: Logs Look Good - Resolve Incident'}
                    </button>
                  )}
                </div>

              </div>

            </div>
          </div>
        </section>
      )}
    </div>
  );
}