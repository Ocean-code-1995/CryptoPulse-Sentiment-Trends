#
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import plotly.graph_objects as go



def plot_timeseries_forecast(historical_data, forecasted_data):
    # Create a line for historical data
    trace1 = go.Scatter(
        x=historical_data['ds'],
        y=historical_data['y'],
        mode='lines',
        name='Historical Data',
        line=dict(color='lightblue')
    )
    
    # Create a line for forecasted data
    trace2 = go.Scatter(
        x=forecasted_data['ds'],
        y=forecasted_data['yhat'],
        mode='lines',
        name='Forecast',
        line=dict(color='orangered', dash='dash')
    )

    # Create a band for the confidence interval (optional)
    trace3 = go.Scatter(
        x=forecasted_data['ds'].tolist() + forecasted_data['ds'][::-1].tolist(),
        y=forecasted_data['yhat_upper'].tolist() + forecasted_data['yhat_lower'][::-1].tolist(),
        fill='toself',
        fillcolor='rgba(255, 0, 0, 0.3)',
        line=dict(color='rgba(255,255,255,0)'),
        hoverinfo="skip",
        showlegend=True,
        name='Confidence Interval'
    )
    
    data = [trace1, trace2, trace3]

    layout = go.Layout(
        title='Historical vs. Forecasted Data',
        # title color
        titlefont=dict(color='white'),
        xaxis=dict(title='Date', tickformat='%d %b %Y', showgrid=True, gridcolor='gray', tickfont=dict(color='white')),
        yaxis=dict(title='Value', showgrid=True, gridcolor='gray', tickfont=dict(color='white')),
        plot_bgcolor='black',
        paper_bgcolor='black',
        hovermode='closest',
        font=dict(color='white')
    )

    fig = go.Figure(data=data, layout=layout)
    fig.update_xaxes(showline=True, linewidth=2, linecolor='white', tickfont=dict(color='white'), tickangle=0)
    fig.update_yaxes(showline=True, linewidth=2, linecolor='white', tickfont=dict(color='white'))
    
    return fig





def donut_chart(value:int, title: str, positive_color:str, negative_color: str) -> go.Figure:
    """
    Plot a donut chart displaying a single value, typically a correlation coefficient.

    Parameters:
    - value: The correlation coefficient or any other single metric to display.
    - title: Title of the plot.
    - positive_color: Color to use if the value is positive.
    - negative_color: Color to use if the value is negative.
    """

    # Normalize the value to a positive scale for display
    normalized_value = abs(value) * 100  if value < 1 else value  # Convert to percentage
    colors = [positive_color, 'lightgray'] if value > 0 else [negative_color, 'lightgray']

    # Create the donut chart
    fig = go.Figure(
        go.Pie(
            values=[normalized_value, 100 - normalized_value],  # Value and remainder
            hole=0.7,
            marker_colors=colors,
            hoverinfo='label+percent',
            textinfo='none',
            domain={'x': [0.25, 0.75], 'y': [0.25, 0.75]}  # Adjusting domain to reduce size

        )
    )

    # Add text in the center of the donut chart
    fig.add_annotation(
        text=f'{normalized_value:.1f}%', x=0.5, y=0.5, font_size=20, showarrow=False,
        font=dict(family="Arial, sans-serif", size=16, color=positive_color if value > 0 else negative_color)
    )

    # Customizing the layout
    fig.update_layout(
        title=dict(
            text=title, x=0.5, y=0.93, xanchor='center', yanchor='top',
            font=dict(family="Verdana, sans-serif", size=14, color=positive_color if value > 0 else negative_color)
        ),
        showlegend=False,
        paper_bgcolor='rgba(0,0,0,0)',  # Transparent background
        plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
        margin=dict(l=10, r=10, t=15, b=10)  # Adjust margins to fit your layout
    )

    # Custom hover text
    fig.update_traces(
        hovertemplate='%{label}: %{value:.1f}%'
    )

    return fig


def donut_chart_corr(value: float, title: str, positive_color: str, negative_color: str) -> go.Figure:
    """
    Plot a donut chart displaying a single value, typically a correlation coefficient, including the sign.
    The value is displayed as a percentage with its sign preserved.
    """
    # Determine the color based on the value's sign
    color = positive_color if value >= 0 else negative_color

    # Normalize the value to display it as a percentage on the chart
    normalized_value = abs(value) * 100

    fig = go.Figure(go.Pie(
        values=[normalized_value, 100 - normalized_value],  # Value and remainder
        hole=0.7,
        marker_colors=[color, 'lightgray'],
        hoverinfo='label+percent',
        textinfo='none',
        domain={'x': [0.25, 0.75], 'y': [0.25, 0.75]}  # Adjusting domain to reduce size
    ))

    # Add the correlation value as text in the center of the donut, formatted as a percentage with the sign
    fig.add_annotation(
        text=f'{value*100:+.1f}%',  # Formatting as a signed percentage with one decimal place
        x=0.5, y=0.5,
        font_size=20,
        showarrow=False,
        font=dict(family="Arial, sans-serif", size=16, color=color)
    )

    fig.update_layout(
        title=dict(
            text=title,
            x=0.5, y=0.93,
            xanchor='center', yanchor='top',
            font=dict(family="Verdana, sans-serif", size=14, color=color)
        ),
        showlegend=False,
        paper_bgcolor='rgba(0,0,0,0)',  # Transparent background
        plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
        margin=dict(l=10, r=10, t=15, b=10)
    )

    return fig


def plot_one_series(
    df1:pd.DataFrame, x1:str, y1:str, label1:str, color1:str,
    title:str, convert_to_percentage:bool, range_y: list
    ) -> go.Figure:
    """
    Plot one data series in a Plotly graph, with an option to convert y-values to percentage.

    Parameters:
    - df1: DataFrame containing the data to plot.
    - x1: Column in df1 to use for the x-axis.
    - y1: Column in df1 to use for the y-axis.
    - label1: Label for the data series.
    - color1: Color for the data series line.
    - title: Plot title.
    - convert_to_percentage: Boolean, whether to check and convert y-values to percentage.
    """
    # Create a copy of the DataFrame to avoid modifying the original data
    df_plot = df1.copy()

    # Check and convert y-values to percentage if necessary
    if convert_to_percentage and df_plot[y1].between(0, 1).all():
        df_plot[y1] = df_plot[y1] * 100  # Convert to percentage

    fig = go.Figure()

    # Add the data series
    fig.add_trace(go.Scatter(x=df_plot[x1], y=df_plot[y1], mode="lines", name=label1, line=dict(color=color1)))

    # Update layout
    fig.update_layout(
        title=title,
        xaxis_title='Date',
        yaxis_title="%",  # Append unit if converted to percentage
        plot_bgcolor='black',
        paper_bgcolor='black',
        font=dict(color='white'),
        xaxis=dict(tickformat='%d %b %Y', showgrid=True, gridcolor='gray'),
        yaxis=dict(showgrid=True, gridcolor='gray', range=range_y)
    )

    # Update axes for better visibility
    fig.update_xaxes(showline=True, linewidth=2, linecolor='white', tickfont=dict(color='white'), tickangle=0)
    fig.update_yaxes(showline=True, linewidth=2, linecolor='white', tickfont=dict(color='white'))

    return fig


# Modify the hollow_candlesticks function to accept ohlc directly
def hollow_candlesticks(
    ohlc: pd.DataFrame, open_or_close:str, moving_avg_periods: list = None, start_date=None, end_date=None
    ) -> go.Figure:
    """
    """
    ohlc = ohlc.copy()
    ohlc[open_or_close] = pd.to_datetime(ohlc[open_or_close])
    ohlc.set_index(open_or_close, inplace=True)
    if moving_avg_periods is None:
        moving_avg_periods = [10, 20, 30]
    # Filter the DataFrame based on the specified start and end dates, if provided
    if start_date and end_date:
        ohlc = ohlc[(ohlc.index >= pd.to_datetime(start_date)) & (ohlc.index <= pd.to_datetime(end_date))]

    ohlc["previousClose"] = ohlc["Close"].shift(1)
    ohlc["color"] = np.where(ohlc["Close"] > ohlc["previousClose"], "lightgreen", "orangered")
    ohlc["fill"] = np.where(ohlc["Close"] > ohlc["Open"], "rgba(255, 0, 0, 0)", ohlc["color"])
    ohlc["Percentage"] = ohlc["Volume"]*100/ohlc["Volume"].sum()
    for moving_avg_period in moving_avg_periods:
        ohlc[f"moving_avg_{moving_avg_period}"] = (
            ohlc["Close"].rolling(window=moving_avg_period).mean()
        )

    price_bins = ohlc.copy()
    price_bins["Close"] = price_bins["Close"].round()
    price_bins = price_bins.groupby("Close", as_index=False)["Volume"].sum()
    price_bins["Percentage"] = price_bins["Volume"]*100/price_bins["Volume"].sum()
    fig = make_subplots(
        rows=2,
        cols=2,
        shared_xaxes="columns",
        shared_yaxes="rows",
        column_width=[0.8, 0.2],
        row_heights=[0.8, 0.2],
        horizontal_spacing=0,
        vertical_spacing=0,
        subplot_titles=[
            "Candlestick", 
            "Price Bins", 
            "Volume", 
            ""
            ]
    )
    showlegend = True
    for index, row in ohlc.iterrows():
        color = dict(fillcolor=row["fill"], line=dict(color=row["color"]))
        fig.add_trace(
            go.Candlestick(
                x=[index],
                open=[row["Open"]],
                high=[row["High"]],
                low=[row["Low"]],
                close=[row["Close"]],
                increasing=color,
                decreasing=color,
                showlegend=showlegend,
                name="GE",
                legendgroup="Hollow Candlesticks"
            ),
            row=1,
            col=1
        )
        showlegend = False
    fig.add_trace(
        go.Bar(
            x=ohlc.index,
            y=ohlc["Volume"],
            text=ohlc["Percentage"],
            marker_line_color=ohlc["color"],
            marker_color=ohlc["fill"],
            name="Volume",
            texttemplate="%{text:.2f}%",
            hoverinfo="x+y",
            textfont=dict(color="white")
        ),
        col=1,
        row=2,
    )
    fig.add_trace(
        go.Bar(
            y=price_bins["Close"],
            x=price_bins["Volume"],
            text=price_bins["Percentage"],
            name="Price Bins",
            orientation="h",
            marker_color="yellow",
            texttemplate="%{text:.2f}% @ %{y}",
            hoverinfo="x+y"
        ),
        col=2,
        row=1,
    )
    # Adding the Moving Average Lines
    colors = ["lightblue", "lightgreen", "lightcoral"]
    for i in range(len(moving_avg_periods)):
        # lines for moving averages
        fig.add_trace(
            go.Scatter(
                x=ohlc.index,
                y=ohlc[f"moving_avg_{moving_avg_periods[i]}"],
                mode="lines",
                line=dict(color=colors[i], width=1),
                name="MAVG",
            )
        )

    fig.update_xaxes(
        rangebreaks=[dict(bounds=["sat", "mon"])],
        rangeslider_visible=False,
        col=1
    )
    fig.update_xaxes(
        showticklabels=True,
        showspikes=True,
        showgrid=True,
        col=2,
        row=1
    )
    fig.update_layout(
        template="plotly_dark",
        hovermode="x unified",
        title_text="Hollow Candlesticks"
    )
    return fig



