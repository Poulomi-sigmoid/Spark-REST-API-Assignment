from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def spark_dataframe():
    spark_df = spark.read.csv('csv_database/*.csv', sep=',', header=True)
    stocks = spark_df.createOrReplaceTempView("STOCKS")
    return stocks


# 1
def get_max_diff_stock():
    try:
        stocks = spark_dataframe()
        data = spark.sql(
            "Select stock_table.company, stock_table.date, stock_table.max_diff_stock_percent from (Select date,"
            "company,(high-open)/open*100 as max_diff_stock_percent, dense_rank() OVER ( "
            "partition by date order by (high-open)/open desc ) as dense_rank FROM stocks)stock_table where "
            "stock_table.dense_rank=1").collect()
        results = []
        for row in data:
            results.append(
                {'date': row['date'], 'company': row['company'],
                 'max_diff_stock_percent': row['max_diff_stock_percent']})
        return results
    except Exception as e:
        return {"Error": e}


# 2
def get_most_traded_stock():
    try:
        stocks = spark_dataframe()
        data = spark.sql("Select stock_table.company, stock_table.date, stock_table.volume from (Select date, company, "
                         "volume, dense_rank() over (partition by date order by int(volume) desc) as dense_rank from "
                         "stocks)stock_table where stock_table.dense_rank=1").collect()
        results = []
        for row in data:
            results.append(
                {'date': row['date'], 'company': row['company'], 'volume': row['volume']})
        return results
    except Exception as e:
        return {"Error": e}


# 3
def get_max_gap():
    try:
        stocks = spark_dataframe()
        data = spark.sql("Select stock_table.company,abs(stock_table.previous_close-stock_table.open) as max_gap from ("
                         "Select company, open, date, close, lag(close,1,35.724998) over(partition by company order by "
                         "date) as previous_close from stocks asc)stock_table order by max_gap desc limit 1").collect()
        results = []
        for row in data:
            results.append(
                {'company': row['company'], 'max_gap': row['max_gap']})
        return results
    except Exception as e:
        return {"Error": e}


# 4
def get_max_stock_diff():
    try:
        stocks = spark_dataframe()
        data = spark.sql("Select stock_table.company, stock_table.open, stock_table.high, (stock_table.high - "
                         "stock_table.open) as max_diff from (Select company, (Select open from stocks limit 1) as "
                         "open, max(high) as high from stocks group by company)stock_table order by max_diff desc "
                         "limit 1").collect()
        results = []
        for row in data:
            results.append(
                {'company': row['company'], 'open': row['open'], 'high': row['high'], 'max_diff': row['max_diff']})
        return results
    except Exception as e:
        return {"Error": e}


# 5
def get_std_deviation_stocks():
    try:
        stocks = spark_dataframe()
        data = spark.sql(
            "Select company, stddev_samp(Volume) as std_deviation from stocks group by company").collect()
        results = []
        for row in data:
            results.append(
                {'company': row['company'], 'std_deviation': row['std_deviation']})
        return results
    except Exception as e:
        return {"Error": e}


# 6
def get_mean_median_stock_price():
    try:
        stocks = spark_dataframe()
        data = spark.sql(
            "Select company, avg(open) as mean, percentile_approx(open,0.5) as median from stocks group by company").collect()
        results = []
        for row in data:
            results.append(
                {'company': row['company'], 'mean': row['mean'], 'median': row['median']})
        return results
    except Exception as e:
        return {"Error": e}


# 7
def get_avg_volume():
    try:
        stocks = spark_dataframe()
        data = spark.sql("Select company, avg(Volume) as avg_volume from stocks group by company order by avg_volume "
                         "desc").collect()
        results = []
        for row in data:
            results.append(
                {'company': row['company'], 'avg_volume': row['avg_volume']})
        return results
    except Exception as e:
        return {"Error": e}


# 8
def get_highest_avg_volume():
    try:
        stocks = spark_dataframe()
        data = spark.sql("Select stock_table.company, stock_table.avg_volume as max_avg_volume from "
                         "(Select company, avg(Volume) as avg_volume from stocks group by company "
                         "order by avg_volume desc limit 1)stock_table").collect()
        results = []
        for row in data:
            results.append(
                {'company': row['company'], 'max_avg_volume': row['max_avg_volume']})
        return results
    except Exception as e:
        return {"Error": e}


# 9
def get_highest_lowest_stock_prices():
    try:
        stocks = spark_dataframe()
        data = spark.sql(
            "Select company, max(High) as highest_price, min(Low) as lowest_price from stocks group by company").collect()
        results = []
        for row in data:
            results.append(
                {'company': row['company'], 'highest_price': row['highest_price'], 'lowest_price': row['lowest_price']})
        return results
    except Exception as e:
        return {"Error": e}
