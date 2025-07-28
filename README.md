# Big Data-Driven Weather Prediction and Visualization Framework for Indian Cities

🔍 Project Overview: Weather Data Prediction & Analysis for Indian Cities
This project is focused on analyzing and processing large-scale historical weather data for over 5000 Indian cities to enable weather pattern insights, predictions, and visualizations.

## 📦 Dataset Source

**Kaggle** – [Indian 5000 Cities Weather Data (2010–2014)](https://www.kaggle.com/datasets/mukeshdevrath007/indian-5000-cities-weather-data)

---

## 📊 Project Overview

The primary goal of this project is to:

- 📥 Ingest hourly weather data (2010–2014) from Kaggle.
- 🧹 Clean and normalize data (e.g., handle missing values, correct `snow_depth` issues).
- ⏱️ Aggregate data from hourly to **12-hour intervals (AM/PM)**.
- 🏙️ Merge multiple files per city (e.g., `mumbai.csv`, `mumbai_1.csv`).
- 📍 Map each city to its respective **Indian state**.
- 🚀 Export a **single clean CSV per city** back to an AWS S3 bucket.

---

## ⚙️ Tech Stack

- **Apache Spark** on **AWS EMR**
- **Python (PySpark)**
- **AWS S3** – Data storage
- **AWS CLI** – File operations
- **Jupyter Notebook** (on EMR) – For interactive development

---

## 📁 Dataset Description

Each city’s raw weather file contains the following columns:

| Column Name            | Description                               |
| ---------------------- | ----------------------------------------- |
| `datetime`             | Date and time (hourly)                    |
| `temperature_2m`       | Temperature at 2 meters height (°C)       |
| `relative_humidity_2m` | Relative humidity (%)                     |
| `dew_point_2m`         | Dew point temperature (°C)                |
| `apparent_temperature` | "Feels like" temperature (°C)             |
| `precipitation`        | Total precipitation (mm)                  |
| `rain`                 | Rainfall amount (mm)                      |
| `snowfall`             | Snowfall amount (mm)                      |
| `snow_depth`           | Snow depth on ground (cm)                 |
| `wind_speed_10m`       | Wind speed at 10 meters (km/h)            |
| `...`                  | 10+ additional weather-related attributes |

> 🗂️ **File Naming Format**:  
> Files are named like `cityname.csv`, `cityname_1.csv`, etc. to represent duplicate/overlapping datasets for a single city.

---

## 📤 Output

- Cleaned and transformed **12-hour interval CSV files** per city.
- All outputs are stored in **AWS S3**, organized by city and state.

---

## 📬 Contact

For feedback or questions, feel free to reach out:

**Gourav Sharma**  
📧 gourav@example.com  
🌐 [LinkedIn](https://linkedin.com/in/yourprofile) | [GitHub](https://github.com/yourgithub)

---
