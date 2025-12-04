from flask import Flask, render_template
import json

app = Flask(__name__)

@app.route("/")
def dashboard():
    table_stats = []  # 데이터 없음

    return render_template(
        "dashboard.html",
        table_stats=table_stats,
        table_json=json.dumps(table_stats)
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
