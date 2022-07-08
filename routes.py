from flask import Flask, jsonify
from sql_services import  *

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False


@app.route("/")
def stock_project():
    return "This is Spark Assignment"


# Query - 1
@app.route("/max_diff_stock_api", methods=["GET"])
def max_diff_stock_api():
    max_diff_stock = get_max_diff_stock()
    dict_to_return = {"Maximum percentage difference in stocks": max_diff_stock}
    return jsonify(dict_to_return)


# Query - 2
@app.route("/most_traded_stock_api", methods=["GET"])
def most_traded_stock_api():
    most_traded_stock = get_most_traded_stock()
    dict_to_return = {"Most Traded Stocks On Each Day": most_traded_stock}
    return jsonify(dict_to_return)


# Query - 3
@app.route("/max_gap_api", methods=["GET"])
def max_gap_api():
    max_gap = get_max_gap()
    dict_to_return = {"Stock with Maximum Gap": max_gap}
    return jsonify(dict_to_return)


# Query - 4
@app.route("/max_stock_diff_api", methods=["GET"])
def max_stock_diff_api():
    max_stock_diff = get_max_stock_diff()
    dict_to_return = {"Stock with Maximum difference in price": max_stock_diff}
    return jsonify(dict_to_return)


# Query - 5
@app.route("/std_deviation_stocks", methods=["GET"])
def std_deviation_stocks():
    std = get_std_deviation_stocks()
    dict_to_return = {"Standard Deviation of Stock Price": std}
    return jsonify(dict_to_return)


# Query - 6
@app.route("/mean_median_stock_price", methods=["GET"])
def mean_median_stock_price():
    mean_median = get_mean_median_stock_price()
    dict_to_return = {"Mean & Median Stock Price": mean_median}
    return jsonify(dict_to_return)


# Query - 7
@app.route("/avg_volume", methods=["GET"])
def avg_volume():
    std = get_avg_volume()
    dict_to_return = {"Average volume of Stock Price": std}
    return jsonify(dict_to_return)


# Query 8
@app.route("/highest_avg_volume", methods=["GET"])
def highest_avg_volume():
    std = get_highest_avg_volume()
    dict_to_return = {"Maximum average volume of Stock Price": std}
    return jsonify(dict_to_return)


# Query 9
@app.route("/highest_lowest_stock_prices", methods=["GET"])
def highest_lowest_stock_prices():
    std = get_highest_lowest_stock_prices()
    dict_to_return = {"Highest and Lowest Stock Price": std}
    return jsonify(dict_to_return)


if __name__ == '__main__':
    app.run(debug=True)
