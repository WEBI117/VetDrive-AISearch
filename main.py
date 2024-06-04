import os
from flask import Flask
import chatgpt

app = Flask(__name__)


@app.route('/')
def landing():
    return chatgpt.message()