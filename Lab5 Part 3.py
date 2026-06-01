@app.route("/echo", methods=["POST"])
def echo():
    data = request.get_json()
    return jsonify({
        "received": data,
        "field_count": len(data) if data else 0,
    })
