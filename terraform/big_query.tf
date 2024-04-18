#############################################
###            BigQuery Resources         ###
#############################################

# Define BigQuery Dataset
#===========================================================================
resource "google_bigquery_dataset" "cryptopulse_dataset" {
  dataset_id                  = "cryptopulse_dataset"
  friendly_name               = "CryptoPulse Dataset"
  description                 = "This dataset stores the tables for the dashboard."
  location                    = var.region
  project                     = local.project_id
  default_table_expiration_ms = 3600000 # 1 hour in milliseconds

  labels = {
    environment = "development"
  }
}

# Define BigQuery Tables
#===========================================================================

#   ---   (1) binance   -----------------
resource "google_bigquery_table" "binance" {
  dataset_id          = google_bigquery_dataset.cryptopulse_dataset.dataset_id
  table_id            = "binance_table"
  project             = local.project_id
  #location   = var.region
  deletion_protection = false


  schema = jsonencode([
    {
      name = "Open_Time",
      type = "DATETIME",
      mode = "REQUIRED"
    },
    {
      name = "Open",
      type = "FLOAT",
      mode = "REQUIRED"
    },
    {
      name = "High",
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "Low",
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "Close",
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "Volume",
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "Close_Time",
      type = "DATETIME",
      mode = "REQUIRED"
    },
    {
      name = "Quote_Asset_Volume",
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "Number_Of_Trades",
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "Taker_Buy_Base_Asset_Volume",
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "Taker_Buy_Quote_Asset_Volume",
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "Year",
      type = "INTEGER",
      mode = "REQUIRED"
    },
    {
      name = "Month",
      type = "INTEGER",
      mode = "REQUIRED"
    },
    {
      name        = "event_properties",
      type        = "STRING",
      mode        = "NULLABLE",
      description = "JSON format of properties"
    }
  ])

  #expiration_time = timeadd(timestamp(), "24h") # 720 hours in seconds

  labels = {
    environment = "development"
  }

}

#   ---   (2) fear_and_greed   -----------------
resource "google_bigquery_table" "fear_and_greed" {
  dataset_id          = google_bigquery_dataset.cryptopulse_dataset.dataset_id
  table_id            = "fear_and_greed_table"
  project             = local.project_id
  deletion_protection = false

  schema = jsonencode([
    {
      name = "value",
      type = "INTEGER",
      mode = "REQUIRED"
    },
    {
      name = "value_classification",
      type = "STRING",
      mode = "REQUIRED"
    },
    {
      name = "Date",
      type = "DATETIME",
      mode = "REQUIRED"
    },
    {
      name = "Year",
      type = "INTEGER",
      mode = "REQUIRED"
    },
    {
      name = "Month",
      type = "INTEGER",
      mode = "REQUIRED"
    },
    {
      name        = "event_properties",
      type        = "STRING",
      mode        = "NULLABLE",
      description = "JSON format of properties"
    }
  ])

  #expiration_time = timeadd(timestamp(), "24h") # 720 hours in seconds

  labels = {
    environment = "development"
  }
}

#   ---   (3) reddit   -----------------
resource "google_bigquery_table" "reddit" {
  dataset_id          = google_bigquery_dataset.cryptopulse_dataset.dataset_id
  table_id            = "reddit_table"
  project             = local.project_id
  deletion_protection = false

  schema = jsonencode([

    {
      name = "Title",
      type = "STRING",
      mode = "REQUIRED"
    },
    {
      name = "Body",
      type = "STRING",
      mode = "REQUIRED"

    },
    {
      name = "Score",
      type = "INTEGER",
      mode = "REQUIRED"
    },
    {
      name = "Total_Comments",
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "Votes",
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "Date",
      type = "DATETIME",
      mode = "REQUIRED"
    },
    {
      name = "Currency",
      type = "STRING",
      mode = "REQUIRED"
    },
    {
      name = "Title_Body",
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "Year",
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "Month",
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "Sentiment",
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "Sentiment Score",
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "Positive Score",
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "Negative Score",
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name        = "event_properties",
      type        = "STRING",
      mode        = "NULLABLE",
      description = "JSON format of properties"
    }
  ])

  #expiration_time = timeadd(timestamp(), "24h") # 720 hours in seconds

  labels = {
    environment = "development"
  }
}

#   ---   (4) forecast   -----------------
resource "google_bigquery_table" "forecast" {
  dataset_id           = google_bigquery_dataset.cryptopulse_dataset.dataset_id
  table_id             = "forecast_table"
  project              = local.project_id
  deletion_protection  = false

  schema = jsonencode([
    {
      name = "yhat",
      type = "INTEGER",
      mode = "REQUIRED"
    },
    {
      name = "ds",
      type = "DATETIME",
      mode = "REQUIRED"
    },
    {
      name = "y",
      type = "INTEGER",
      mode = "REQUIRED"
    },
    {
      name = "yhat_upper",
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "yhat_lower",
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "Year",
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "Month",
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name        = "event_properties",
      type        = "STRING",
      mode        = "NULLABLE",
      description = "JSON format of properties"
    }
  ])
  # │ Inappropriate value for attribute "expiration_time": a number is required.
  #expiration_time = timeadd(timestamp(), "24h") # 720 hours in seconds

  labels = {
    environment = "development"
  }
}