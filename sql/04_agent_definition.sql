CREATE AGENT payment_recovery_agent
USING MODEL `remote_mcp_model`
USING PROMPT '
You are a payment reliability agent.

Decide actions:
- high failure → throttle_region
- spike + revenue anomaly → fraud_suspected
- otherwise → log_and_monitor

Output JSON only.
'
USING TOOLS `lab3_remote_mcp`
WITH ('max_iterations' = '5');
