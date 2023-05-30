from flask import Flask, jsonify
from . import app
from . import data

data.get_data()


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/test")
def test():
    return "<p>test</p>"


@app.route("/data")
def return_data():
    data.get_data()
    return data.data_object
