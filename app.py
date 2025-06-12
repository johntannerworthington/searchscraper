from flask import Flask, request, send_file, render_template
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

    output_path = run_search_scraper(queries_path, serper_api_key)
    return send_file(output_path, as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True)
