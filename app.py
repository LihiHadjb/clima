from flask import Flask, request
from retriever import Retriever
from flask import jsonify
from dbInit import DBInitializer

app = Flask(__name__)

# init = DBInitializer()
# init.create_table()
# init.load_data("data")

retriever = Retriever()


@app.route('/weather/data')
def data():
    lat = float(request.args.get('lat'))
    lon = float(request.args.get('lon'))
    return jsonify(retriever.get_data_for_location(lat, lon))


@app.route('/weather/summarize')
def summarize():
    lat = float(request.args.get('lat'))
    lon = float(request.args.get('lon'))
    return jsonify(retriever.get_summary_for_location(lat, lon))


if __name__ == '__main__':
    app.run()
