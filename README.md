# ***`CryptoPulse: Streamlining Sentiments & Trends`***
---
---

## ***Description***
CryptoPulse leverages cutting-edge data engineering to analyze cryptocurrency market sentiments and forecast trends. By integrating real-time social media analysis with financial data, it provides actionable insights into crypto market dynamics. This project embodies a comprehensive pipeline—from data ingestion and sentiment analysis to trend forecasting and interactive visualization—facilitating informed investment decisions in the volatile world of cryptocurrencies.

## ***Structure***
- run `tree` in the root directory via terminal to get the project structure displayed as tree:
```
project_root
├── NLP
│   └── sentiment_analysis
├── README.md
├── dashboard
├── data
│   ├── processed
│   └── raw
├── data_acquisition
│   ├── batch_processing
│   │   ├── binance
│   │   │   ├── b_fetch.py
│   │   │   ├── b_main.py
│   │   │   └── b_spark.py
│   │   └── reddit
│   │       ├── r_fetch.py
│   │       ├── r_main.py
│   │       └── r_spark.py
│   └── stream_processing
├── infrastructure
│   ├── docker
│   └── terraform
├── machine_learning
│   ├── models
│   └── training
├── playground
│   ├── binance_scraper.ipynb
│   ├── crypto_panic.ipynb
│   └── reddit_scraper.ipynb
├── project_directory_overview.png
└── requirements.txt
```

## ***Diagram***

>>>>><<<<<>>>>>

## 1) **Infrastructure as Code (IaC)**
---


## 2) Data Sources
---
### 2.1) Reddit
### 2.2) Binance


## 3) Data Pipelines to Google Cloud Storage
---
### 3.1) Streaming Data:
If the goal is to leverage real-time insights from streaming data, processing for sentiment before storage is generally recommended. This approach allows you to act on or display sentiment insights with minimal latency.

### 3.2) Batch Processing of Historical Data:
If flexibility and the ability to reprocess data are important, storing raw data first and then processing it might be more beneficial. This is especially true if you are experimenting with different sentiment analysis models or parameters.

