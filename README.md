# CC Flink Time Series Forecasting

Goal we would like to forecast the next values of a stock price using the new `ML_FORECAST()` function in CC Flink.

## Approach

We first create the table and insert some test data. This test data is linear increasing with some error term.
```
CREATE TABLE pneff_stocks (
    stock_price double,
    ts TIMESTAMP(3) NOT NULL
);
```

```
INSERT INTO pneff_stocks (stock_price, ts) VALUES
  (9.98, TIMESTAMP '2024-11-19 10:00:00.000'),
  (9.31, TIMESTAMP '2024-11-19 10:01:00.000'),
  (15.74, TIMESTAMP '2024-11-19 10:02:00.000'),
  (22.62, TIMESTAMP '2024-11-19 10:03:00.000'),
  (16.33, TIMESTAMP '2024-11-19 10:04:00.000'),
  (18.83, TIMESTAMP '2024-11-19 10:05:00.000'),
  (30.4, TIMESTAMP '2024-11-19 10:06:00.000'),
  (28.84, TIMESTAMP '2024-11-19 10:07:00.000'),
  (25.15, TIMESTAMP '2024-11-19 10:08:00.000'),
  (32.71, TIMESTAMP '2024-11-19 10:09:00.000'),
  (30.18, TIMESTAMP '2024-11-19 10:10:00.000'),
  (32.67, TIMESTAMP '2024-11-19 10:11:00.000'),
  (38.71, TIMESTAMP '2024-11-19 10:12:00.000'),
  (30.43, TIMESTAMP '2024-11-19 10:13:00.000'),
  (33.88, TIMESTAMP '2024-11-19 10:14:00.000'),
  (42.19, TIMESTAMP '2024-11-19 10:15:00.000'),
  (42.44, TIMESTAMP '2024-11-19 10:16:00.000'),
  (51.57, TIMESTAMP '2024-11-19 10:17:00.000'),
  (47.96, TIMESTAMP '2024-11-19 10:18:00.000'),
  (47.94, TIMESTAMP '2024-11-19 10:19:00.000'),
  (64.83, TIMESTAMP '2024-11-19 10:20:00.000'),
  (58.87, TIMESTAMP '2024-11-19 10:21:00.000'),
  (62.84, TIMESTAMP '2024-11-19 10:22:00.000'),
  (57.88, TIMESTAMP '2024-11-19 10:23:00.000'),
  (64.78, TIMESTAMP '2024-11-19 10:24:00.000'),
  (70.55, TIMESTAMP '2024-11-19 10:25:00.000'),
  (66.75, TIMESTAMP '2024-11-19 10:26:00.000'),
  (76.88, TIMESTAMP '2024-11-19 10:27:00.000'),
  (74.5, TIMESTAMP '2024-11-19 10:28:00.000'),
  (78.54, TIMESTAMP '2024-11-19 10:29:00.000');
```

```
SELECT * FROM pneff_stocks;
```

image


We know apply the new functionality which applies an ARIMA model on our time series data. For more information about the individual parameters, etc, see the [documentation)(https://staging-docs-independent.confluent.io/docs-cloud/PR/4751/current/ai/forecast.html).

```
CREATE TABLE pneff_stocks_predict AS SELECT ML_FORECAST(
 stock_price,
 ts,
 JSON_OBJECT('p' VALUE 1, 'q' VALUE 1, 'd' VALUE 1, 'minTrainingSize' VALUE 10, 'horizon' VALUE 5)) AS forecast
FROM pneff_stocks
```

We set the `horizon` to 5 to predict the next 5 values

Let's check the structure of the output

```
DESCRIBE EXTENDED `team-global`.`csta_global_cluster`.`pneff_stocks_predict`;
```

image

Now let's scrape only the predicted values via

```
SELECT
  t.`timestamp` AS ts,
  t.`forecast_value` As prediction
FROM `pneff_stocks_predict`,
UNNEST(`forecast`) AS t;
```

image 
