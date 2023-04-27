
import os
import duckdb
import dbt
#import airflow

def main(args):
    # sql db creation
    path_folder = args["path_folder"]
    path_folder_data_raw = os.path.join(path_folder, "data", "raw")
    file_name_database = "data.db"
    path_file_database = os.path.join(path_folder_data_raw, file_name_database)
    sql_dbms = duckdb
    db_conn = sql_dbms.connect(path_file_database)
    cursor = db_conn.cursor()
    
    # sql table
    path_file_zipcode_coordinates = os.path.join(path_folder_data_raw,"zipcode_coordinates.csv")
    path_file_oa_offers = os.path.join(path_folder_data_raw,"offer_acceptance_offers.csv")
    path_file_oa_orders = os.path.join(path_folder_data_raw,"offer_acceptance_orders.csv")
    file_name_create_tables_sql = "create_tables.sql"
    path_file_create_tables_sql = os.path.join(path_folder, "src", "etl", file_name_create_tables_sql)
    with open(path_file_create_tables_sql,"r") as f:
        sql = f.read()
        f.close()
    sql = sql.format(path_file_oa_orders=path_file_oa_orders,
                     path_file_oa_offers=path_file_oa_offers,
                     path_file_zipcode_coordinates=path_file_zipcode_coordinates)
    create_tables_sql = sql
    
    # sql table creation
    cursor.sql(create_tables_sql)
    db_conn.commit()

    # dbt yaml load
    path_folder_dbt_flock = os.path.join(path_folder, "src","dataclean", "dbt_flock")
    path_folder_dbt_flock_models = os.path.join(path_folder_dbt_flock, "models")

    path_file_profile_yaml = "dbt_flock/profile.yml"
    with open(path_file_profile_yaml, 'r') as f:
        profile_yaml = yaml.safe_load(f)
        f.close()
    profile_yaml["default"]["dev"]["path"] = path_file_database

    yaml_string = yaml.dump(profile_yaml)
    with open(path_file_profile_yaml, "w") as f:
        f.write(yaml_string)
        f.close()

    path_file_project_yaml = "dbt_flock/project.yml"
    with open(path_file_project_yaml, 'r') as f:
        project_yaml = yaml.safe_load(f)
        f.close()
    filtering_ftl = 0
    explode_references = 1

    project_yaml["vars"]["filtering_ftl"] = filtering_ftl
    project_yaml["vars"]["explode_references"] = explode_references

    path_file_dbt_oa_orders_temp_sql = os.path.join(path_folder_dbt_flock_models,"create_oa_orders_temp.sql")
    path_file_dbt_oa_offers_temp_sql = os.path.join(path_folder_dbt_flock_models,"create_oa_orders_temp.sql")
    path_file_dbt_oa_sql = os.path.join(path_folder_dbt_flock_models,"create_oa.sql")
              
    project_yaml["models"]["oa_orders_temp"]["sql"] = path_file_dbt_oa_orders_temp_sql
    project_yaml["models"]["oa_offers_temp"]["sql"] = path_file_dbt_oa_offers_temp_sql
    project_yaml["models"]["oa"]["sql"] = path_file_dbt_oa_sql

    yaml_string = yaml.dump(project_yaml)
    with open(path_file_project_yaml, "w") as f:
        f.write(yaml_string)
        f.close()
    
    # dbt run
              
    # metro-cluster
    assert False
