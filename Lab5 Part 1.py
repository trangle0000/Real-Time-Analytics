@app.route("/")
def home():
    return jsonify({
        "message": "Welcome to the transaction monitoring system!",
        "endpoints": [
            "GET /health",
            "POST /score",
            "GET /stats",
        ]
    })

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "version": "1.0-rules",
        "timestamp": datetime.now().isoformat(),
    })
