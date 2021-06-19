<h2>COVID19_MA_Gov_Data_Pull</h2>
<h4>Automatically extracts selected data from daily and weekly updated info from <a href="https://www.mass.gov/info-details/covid-19-response-reporting">mass.gov COVID19 tracking site</a>.</h4>
<h4>In actual deployment this is used in a Docker container, triggered on schedule by Airflow, loaded to BigQuery, then transformed by [dbt](https://www.getdbt.com/), automatically updating [Looker](https://looker.com/) dashboards for user consumption. The processed data can also be git pushed to production if needed.</h4>
