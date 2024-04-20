import streamlit as st
from visualizations import (
                        hollow_candlesticks,
                        plot_one_series,
                        donut_chart,
                        donut_chart_corr,
                        plot_timeseries_forecast
)
from data_tools import (
                        load_data,
                        load_and_process_BigQuery,
                        load_partitioned_data,
                        aggregate_by_date,
                        merge_and_correlate,
                        merge_corr_dfs
)
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from sklearn.preprocessing import StandardScaler
import os
import altair as alt
###
### RUN APP:
#╰─ streamlit run dashboard_app.py --server.port 8502                                                                                                       ─╯
#
os.environ['PYSPARK_PYTHON']        = '/Users/sebastianwefers/anaconda3/envs/dashboard_env/bin/python3.10'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/sebastianwefers/anaconda3/envs/dashboard_env/bin/python3.10'

def main():
    # Set up your page layout, title, and load data
    # Page configuration
    st.set_page_config(
        page_title="CryptoPulse Dashboard",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded")

    alt.themes.enable("dark")
    # Title
    st.title("CryptoPulse Dashboard")


    # Load data from local
    # ===================================================================
    home_dir       = os.path.expanduser("~")
    data_path = os.path.join(
        home_dir, "Desktop", "DataEng_Crypto_Sentiment", "playground", "data",
    )
    binance, fearngreed, reddit, forecast = load_data(
        path1 = os.path.join(data_path, "raw", "binance"),
        path2 = os.path.join(data_path, "raw", "alternative_me"),
        path3 = os.path.join(data_path, "processed", "reddit_sentiment"),
        path4 = os.path.join(data_path, "processed", "binance_forecast")
    )

    # Load data from BigQuery:
    # ===================================================================
    #project_id       = "dataengineering-411512"

    #reddit = load_and_process_BigQuery(
    #    query      = f"SELECT * FROM `{project_id}.cryptopulse.reddit`",
    #    project_id = project_id,
    #    date_col   = "Date"
    #)

    #binance = load_and_process_BigQuery(
    #    query      = f"SELECT * FROM `{project_id}.cryptopulse.binance`",
    #    project_id = project_id,
    #    date_col   = "Close_Time"
    #)

    #fearngreed = load_and_process_BigQuery(
    #    query      = f"SELECT * FROM `{project_id}.cryptopulse.fear_and_greed_index`",
    #    project_id = project_id,
    #    date_col   = "Date"
    #)

    #forecast = load_and_process_BigQuery(
    #    query      = f"SELECT * FROM `{project_id}.cryptopulse.forecast`",
    #    project_id = project_id,
    #    date_col   = "ds"
    #)
    # sort all dataframes by date
    binance    = binance.sort_values("Close Time")
    fearngreed = fearngreed.sort_values("Date")
    reddit     = reddit.sort_values("Date")
    forecast   = forecast.sort_values("ds")

    # Initialize Spark Session
    #spark = SparkSession.builder.appName("Data Loading App").getOrCreate()
    #data_path = os.path.join(home_dir, "Desktop", "DataEng_Crypto_Sentiment", "playground", "data")

    #binance, fearngreed, reddit, forecast = load_partitioned_data(
    #    spark = spark,
    #    path1 = os.path.join(data_path, "raw", "binance"),
    #    path2 = os.path.join(data_path, "raw", "alternative_me"),
    #    path3 = os.path.join(data_path, "processed", "reddit_sentiment"),
    #    path4 = os.path.join(data_path, "processed", "binance_forecast")
    #)

    # Visualizations
    # ===================================================================

    # 1) hollow candlesticks
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # user input for moving average periods
    options = [
        3, 5, 7, 8, 10, 12, 13, 14, 20, 21, 26, 30, 34, 50, 55,
        60, 89, 100, 120, 144, 200, 233, 365
    ]

    moving_avg_periods = st.multiselect(
        "Select moving average periods (choose any three)",
        options = options,
        default = [10, 20, 30]  # Optional: specify default selections
    )
    if len(moving_avg_periods) > 3:
        st.error("Please select no more than three periods.")
        moving_avg_periods = moving_avg_periods[:3]  # Keep only the first three selections


    hollow_candlesticks_fig = hollow_candlesticks(
        ohlc               = binance,
        open_or_close      = "Open Time",
        moving_avg_periods = moving_avg_periods
    )
    st.plotly_chart(hollow_candlesticks_fig, use_container_width=True)



    col1, col2 = st.columns([2, 1])

    with col1:
        # 2) forecast plot
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # if forecast is not empty or does not exist
        if not forecast.empty or forecast is not None:
            timeserires_forecast_plot = plot_timeseries_forecast(
                    historical_data = forecast[forecast['y'].notna()],
                    forecasted_data = forecast[forecast['y'].isna()]
                )
            st.plotly_chart(timeserires_forecast_plot, use_container_width=True)

            # 3) Fear and Greed Index
            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            fearngreed_time_series = plot_one_series(
                df1                   = fearngreed.reset_index(),
                x1                    = "Date",              #!!!!!!!!!! timestamp siwtched to Date due to Key ERROR
                y1                    = "value",
                label1                = "Fear and Greed Index",
                color1                = "yellow",
                title                 = "Fear & Greed Index",
                convert_to_percentage = False,
                range_y               = [0, 100]
            )
            st.plotly_chart(fearngreed_time_series, use_container_width=True)
        else:
            st.error("No forecast data available. Check data availability.")


        # 4) time series plot for reddit sentiment
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        if "Positive Score" in reddit.columns:
            reddit_agg = aggregate_by_date(
                df       = reddit,
                freq     = "D",
                col_name = "Positive Score",
                date_col = "Date"
            )
            reddit_sentiment_time_series = plot_one_series(
                df1                   = reddit_agg,
                x1                    = "Date",
                y1                    = "Positive Score",
                label1                = "Reddit Sentiment",
                color1                = "lightgreen",
                title                 = "Reddit Sentiment",
                convert_to_percentage = True,
                range_y               = [0, 100]
            )
            st.plotly_chart(reddit_sentiment_time_series, use_container_width=True)
        else:
            st.error("Reddit sentiment data not found. Check data availability.")

    with col2:
        # 5) Donut chart for correlation between fearngreed index and crypto price
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # options for selecting columns from binance to be correlated with fearngreed index
        options = ['Open', 'High', 'Low', 'Close', 'Volume']
        if selected_metric := st.selectbox(
            "Select a metric to correlate with the Fear & Greed Index",
            options = options,
            index   = options.index("Close"),  # Default to 'Close'
        ):
            if correlations := merge_and_correlate(
                df1       = binance,
                df2       = fearngreed.reset_index(),
                date_col1 = "Close Time",
                date_col2 = "Date",
                cols1     = [selected_metric],
                col2      = "value",
            ):
                correlation_value = correlations[selected_metric]  # Directly access the value using the key

                # Visualization: Using a bar chart or other visual might be more appropriate
                st.plotly_chart(
                    donut_chart_corr(
                        value          = correlation_value,
                        title          = f"Correlation: Fear & Greed Index vs {selected_metric}",
                        positive_color = "red",
                        negative_color = "darkblue"
                    ),
                    use_container_width=True
                )
            else:
                st.write("No correlation could be computed. Check data availability.")

        # 6) Donut chart for fearngreed index
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Convert dates to datetime at the start of the day for `start_date` and end of the day for `end_date`
        # Calculate dates for the last seven days
        end_date_default   = datetime.now()
        start_date_default = end_date_default - timedelta(days=7)

        # Use calculated defaults in st.date_input
        start_date = st.date_input('Start date', value=start_date_default)
        end_date   = st.date_input('End date', value=end_date_default)

        start_dt   = pd.Timestamp(start_date)
        end_dt     = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        # Filter the data for specified date range with respect to "Date" column not index
        mask =  (fearngreed["Date"] >= start_dt) & (fearngreed["Date"] <= end_dt)
        filtered_fearngreed = fearngreed.loc[mask]
        
        #mask = (fearngreed.index >= start_dt) & (fearngreed.index <= end_dt)
        #filtered_fearngreed = fearngreed.loc[mask]

        if filtered_fearngreed.empty:
            st.write("No data available for the selected date. Please choose a different date.")
        else:
            # figure
            fearngreed_donut = donut_chart(
                value          = filtered_fearngreed["value"].mean(),
                title          = "Fear & Greed Index",
                positive_color = "yellow",
                negative_color = "orange"
            )
            st.plotly_chart(fearngreed_donut, use_container_width=True)


        # 7) Donut chart for sentiment analysis
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Aggregate data based on user selection
        if "Positive Score" in reddit.columns:
            # Aggregate data based on user selection
            aggregated_df = aggregate_by_date(
                df       = reddit,
                freq     = "D",
                col_name = "Positive Score",
                date_col = "Date"
            )

            # Filter aggregated data based on selected date range
            mask = (aggregated_df['Date'] >= pd.Timestamp(start_date)) & (aggregated_df['Date'] <= pd.Timestamp(end_date))
            filtered_df = aggregated_df.loc[mask]

            if filtered_df.empty:
                st.write("No data available for the selected date. Please choose a different date.")
            else:
                # Calculate average sentiment score for the selected date range
                avg_sentiment = filtered_df['Positive Score'].mean()

                # Figure
                sentiment_donut = donut_chart(
                    value          = avg_sentiment,
                    title          = "Reddit Sentiment",
                    positive_color = "lightgreen",
                    negative_color = "orangered"
                )
                st.plotly_chart(sentiment_donut, use_container_width=True)
        else:
            st.error("Reddit sentiment data not found. Check data availability.")



if __name__ == "__main__":
    main()
