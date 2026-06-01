@app.route("/hello")
def hello():
    name = request.args.get("name", "stranger")
    return jsonify({"message": f"Hello, {name}!"})

@app.route("/transaction/<tx_id>")
def get_transaction(tx_id):
    return jsonify({
        "tx_id": tx_id,
        "status": "found",
        "note": "To score, use POST /score"
    })
