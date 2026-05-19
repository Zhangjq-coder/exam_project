#!/bin/bash
# AeroSense API Test Commands
# Run these commands to verify all endpoints

BASE_URL="http://localhost:5000/api/v1"

echo "=== 1. Health Check (200) ==="
curl -s $BASE_URL/health | python3 -m json.tool

echo -e "\n=== 2. List Sensors (200) ==="
curl -s $BASE_URL/sensors | python3 -m json.tool

echo -e "\n=== 3. Latest Reading - temperature (200) ==="
curl -s $BASE_URL/sensors/temperature/latest | python3 -m json.tool

echo -e "\n=== 4. Latest Reading - invalid type (400) ==="
curl -s $BASE_URL/sensors/invalid/latest | python3 -m json.tool

echo -e "\n=== 5. Sensor Stats - temperature, days=7 (200) ==="
curl -s "$BASE_URL/sensors/temperature/stats?days=7" | python3 -m json.tool

echo -e "\n=== 6. Sensor Stats - invalid days (400) ==="
curl -s "$BASE_URL/sensors/temperature/stats?days=100" | python3 -m json.tool

echo -e "\n=== 7. Sensor Stats - non-numeric days (400) ==="
curl -s "$BASE_URL/sensors/temperature/stats?days=abc" | python3 -m json.tool

echo -e "\n=== 8. Anomalies - temperature, limit=5 (200) ==="
curl -s "$BASE_URL/anomalies?sensor=temperature&limit=5" | python3 -m json.tool

echo -e "\n=== 9. Anomalies - invalid sensor type (400) ==="
curl -s "$BASE_URL/anomalies?sensor=invalid&limit=5" | python3 -m json.tool

echo -e "\n=== 10. Anomalies - invalid limit (400) ==="
curl -s "$BASE_URL/anomalies?limit=101" | python3 -m json.tool

echo -e "\n=== 11. POST Reading - valid payload (201) ==="
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"sensor": "temperature", "value": 25.5, "unit": "C", "source": "test-site"}' \
  $BASE_URL/readings | python3 -m json.tool

echo -e "\n=== 12. POST Reading - missing field (422) ==="
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"sensor": "temperature", "value": 25.5}' \
  $BASE_URL/readings | python3 -m json.tool

echo -e "\n=== 13. POST Reading - invalid sensor (422) ==="
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"sensor": "invalid", "value": 25.5, "unit": "C", "source": "test-site"}' \
  $BASE_URL/readings | python3 -m json.tool

echo -e "\n=== 14. POST Reading - invalid JSON (400) ==="
curl -s -X POST -H "Content-Type: application/json" \
  -d 'not-a-json' \
  $BASE_URL/readings | python3 -m json.tool

echo -e "\n=== 15. 404 Not Found ==="
curl -s $BASE_URL/nonexistent | python3 -m json.tool

echo -e "\n=== 16. 405 Method Not Allowed ==="
curl -s -X DELETE $BASE_URL/health | python3 -m json.tool