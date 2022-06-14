# Anton_Slizh_M07_SparkSQL
## Look source code of Databricks notebooks at <i>/notebooks</i>
## Look the screenshots of execution plans of queries at <i>/screenshots</i>
## Look how to set up environment, run notebooks and final results at <i>/report.docx</i> file

# Task
* Copy hotel/weather and expedia data from Azure ADLS gen2 storage into provisioned with terraform Azure ADLS gen2 storage.
* Create Databricks Notebooks 
* Using Spark SQL calculate and visualize in Databricks Notebooks (for queries use hotel_id - join key, srch_ci- checkin, srch_co - checkout:
  * Top 10 hotels with max absolute temperature difference by month.
  * Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.
  * For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.
* For designed queries analyze execution plan. Map execution plan steps with real query. Specify the most time (resource) consuming part.

