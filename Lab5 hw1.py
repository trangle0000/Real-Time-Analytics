
%%file app.py
from flask import Flask, request, jsonify
from datetime import datetime

app = Flask(__name__)

# Homework 1: Global counters for /stats endpoint
counters = {
    "total": 0,
    "low": 0,
    "medium": 0,
    "high": 0,
    "critical": 0
}

def score_transaction(tx):
    score = 0
    rules = []
    
    amount = tx.get("amount", 0)
    
    # Homework 2: Validation - if amount is negative, return error
    if amount < 0:
        return {"error": "Amount cannot be negative", "valid": False}
    
    if amount > 3000:
        score += 3
        rules.append("R1: amount > 3000")
    
    if tx.get("category") == "electronics" and amount > 1500:
        score += 2
        rules.append("R2: electronics > 1500")
    
    if tx.get("hour", 12) < 6:
        score += 2
        rules.append("R3: night hour")
    
    risk_level = "CRITICAL" if score >= 7 else "HIGH" if score >= 5 else "MEDIUM" if score >= 2 else "LOW"
    
    return {"score": score, "risk_level": risk_level, "triggered_rules": rules, "valid": True}

@app.route("/score", methods=["POST"])
def score():
    global counters
    
    tx = request.get_json()
    
    if not tx or "amount" not in tx:
        return jsonify({"error": "Missing required field 'amount'"}), 400
    
    result = score_transaction(tx)
    
    # Homework 2: Handle validation error
    if not result.get("valid", True):
        return jsonify(result), 400
    
    result["tx_id"] = tx.get("tx_id", "unknown")
    result["timestamp"] = datetime.now().isoformat()
    
    # Update counters for /stats
    counters["total"] += 1
    risk_level = result["risk_level"].lower()
    if risk_level in counters:
        counters[risk_level] += 1
    
    return jsonify(result)

@app.route("/health")
def health():
    return jsonify({"status": "ok", "version": "1.0-hw"})

# Homework 1: /stats endpoint
@app.route("/stats")
def stats():
    total = counters["total"]
    
    if total == 0:
        return jsonify({
            "total_requests": 0,
            "breakdown": counters,
            "message": "No requests processed yet"
        })
    
    return jsonify({
        "total_requests": total,
        "breakdown": {
            "LOW": counters["low"],
            "MEDIUM": counters["medium"],
            "HIGH": counters["high"],
            "CRITICAL": counters["critical"],
        },
        "percentage": {
            "LOW": f"{counters['low']/total*100:.1f}%",
            "MEDIUM": f"{counters['medium']/total*100:.1f}%",
            "HIGH": f"{counters['high']/total*100:.1f}%",
            "CRITICAL": f"{counters['critical']/total*100:.1f}%",
        }
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
