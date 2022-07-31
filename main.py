from flask import Flask
import os
from modules.mydictionary import *

app = Flask(__name__)

@app.route('/')
def index():
    return "Testing API Rest"

if __name__ == '__main__':
    app.run(port=int(os.environ.get("PORT", 8080)),host='0.0.0.0',debug=True)

