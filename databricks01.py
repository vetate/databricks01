# Databricks notebook source
# MAGIC %md
# MAGIC ### This is my first notebook

# COMMAND ----------

def __validate_libraries():
    import requests
    try:
        site = "https://github.com/databricks-academy/dbacademy"
        response = requests.get(site)
        error = f"Unable to access GitHub or PyPi resources (HTTP {response.status_code} for {site})."
        assert response.status_code == 200, "{error} Please see the \"Troubleshooting | {section}\" section of the \"Version Info\" notebook for more information.".format(error=error, section="Cannot Install Libraries")
    except Exception as e:
        if type(e) is AssertionError: raise e
        error = f"Unable to access GitHub or PyPi resources ({site})."
        raise AssertionError("{error} Please see the \"Troubleshooting | {section}\" section of the \"Version Info\" notebook for more information.".format(error=error, section="Cannot Install Libraries")) from e

def __install_libraries():
    global pip_command
    
    specified_version = f"Build-Scripts"
    key = "dbacademy.library.version"
    version = spark.conf.get(key, specified_version)

    if specified_version != version:
        print("** Dependency Version Overridden *******************************************************************")
        print(f"* This course was built for {specified_version} of the DBAcademy Library, but it is being overridden via the Spark")
        print(f"* configuration variable \"{key}\". The use of version Build-Scripts is not advised as we")
        print(f"* cannot guarantee compatibility with this version of the course.")
        print("****************************************************************************************************")

    try:
        from dbacademy import dbgems  
        installed_version = dbgems.lookup_current_module_version("dbacademy")
        if installed_version == version:
            pip_command = "list --quiet"  # Skipping pip install of pre-installed python library
        else:
            print(f"WARNING: The wrong version of dbacademy is attached to this cluster. Expected {version}, found {installed_version}.")
            print(f"Installing the correct version.")
            raise Exception("Forcing re-install")

    except Exception as e:
        # The import fails if library is not attached to cluster
        if not version.startswith("v"): library_url = f"git+https://github.com/databricks-academy/dbacademy@{version}"
        else: library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{version}/dbacademy-{version[1:]}-py3-none-any.whl"

        default_command = f"install --quiet --disable-pip-version-check {library_url}"
        pip_command = spark.conf.get("dbacademy.library.install", default_command)

        if pip_command != default_command:
            print(f"WARNING: Using alternative library installation:\n| default: %pip {default_command}\n| current: %pip {pip_command}")
        else:
            # We are using the default libraries; next we need to verify that we can reach those libraries.
            __validate_libraries()

__install_libraries()

# COMMAND ----------

# MAGIC %pip $pip_command

# COMMAND ----------

remote_files = ["/customers_delta/","/customers_delta/_delta_log/","/customers_delta/part-00000-8722151a-730c-4c60-9d4c-0f0fa8326362-c000.snappy.parquet", "/customers_delta/_delta_log/00000000000000000000.crc", "/customers_delta/_delta_log/00000000000000000000.json", "/customers.csv"]

# COMMAND ----------

from dbacademy import dbgems
from dbacademy.dbhelper import DBAcademyHelper, Paths, CourseConfig, LessonConfig

course_config = CourseConfig(course_code = "gswdeod",
                             course_name = "get-started-with-data-engineering-on-databricks",
                             data_source_name = "get-started-with-data-engineering-on-databricks",
                             data_source_version = "v01",
                             install_min_time = "1 min",
                             install_max_time = "5 min",
                             remote_files = remote_files,
                             supported_dbrs = ["13.2.x-scala2.12", "13.2.x-photon-scala2.12", "13.2.x-cpu-ml-scala2.12"],
                             expected_dbrs = "13.2.x-scala2.12, 13.2.x-photon-scala2.12, 13.2.x-cpu-ml-scala2.12")


lesson_config = LessonConfig(name = None,
                             create_schema = False,
                             create_catalog = True,
                             requires_uc = True,
                             installing_datasets = True,
                             enable_streaming_support = False,
                             enable_ml_support = False)


# COMMAND ----------

# lesson: Writing delta 
def _create_eltwss_users_update():
    import time
    import pyspark.sql.functions as F
    start = int(time.time())
    print(f"\nCreating the table \"customers_dirty\"", end="...")

    df = spark.createDataFrame(data=[(None, None, None, None), (None, None, None, None), (None, None, None, None)], 
                               schema="user_id: string, user_first_touch_timestamp: long, email:string, updated:timestamp")
    (spark.read
          .load(f"{DA.paths.datasets}/customers_delta")
          .withColumn("updated", F.current_timestamp())
          
          .write
          .mode("overwrite")
          .saveAsTable("customers_dirty"))
    
    total = spark.read.table("customers_dirty").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")
