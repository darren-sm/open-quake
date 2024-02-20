# Open Quake

This is a simple pipeline for [DOST Philippine Institute of Volcanology and Seismology (DOST-PHIVOLCS) data](https://earthquake.phivolcs.dost.gov.ph/). Phivolcs is the institution responsible in forecasting volcanic eruptions and earthquakes in the Philippine region.

## Dashboard

~~[Dashboard Link](https://lookerstudio.google.com/s/grdqh93EDy4) (Updated every minute)~~

> Dashboard link taken down due to process costs. 

![Dashboard](./docs/dashboard.gif)

## How it works

![Flow](./docs/flow.png)

**Data**

Phivolcs issue a bulletin for every earthquake activity recorded on their stations around the Philippines. The project processes the 2019-present data.

|        DateTime         | Location (Lat & Long) |   Depth    | Magnitude |                Origin                 | Intensity  |
| :---------------------: | :-------------------: | :--------: | :-------: | :-----------------------------------: | :--------: |
| GMT+8; seconds included |  2 decimal precision  | Kilometers |   Size    | Tectonic Movement / Volcanic Activity | Perception |

**Pipeline**

The bulletin is parsed into a structured data to be saved into parquet format. The file is then uploaded into a Google Cloud Storage bucket which is used by BigQuery as data source for an external table.

The process repeats every minute (polling) to check if new data/bulletin is submitted by Phivolcs. Looker Studio is used to create a live dashboard.
