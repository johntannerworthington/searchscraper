from flask import Flask, request, send_file, render_template, redirect
import os
from scraper import run_search_scraper

app = Flask(__name__)
UPLOADS_DIR = "uploads"
os.makedirs(UPLOADS_DIR, exist_ok=True)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/search", methods=["POST"])
def search():
    query_file = request.files["queries"]
    serper_api_key = request.form["serper_api_key"]

    queries_path = os.path.join(UPLOADS_DIR, "queries.csv")
    query_file.save(queries_path)

    session_id = run_search_scraper(queries_path, serper_api_key)
    return redirect(f"/download/{session_id}")

@app.route("/download/<session_id>")
def download_file(session_id):
    file_path = os.path.join(UPLOADS_DIR, session_id, "output.csv")
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        return f"❌ File not found for session ID: {session_id}", 404

if __name__ == "__main__":
    app.run(debug=True)
