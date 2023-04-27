
import os
import duckdb
import dbt
#import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

class Tasks:
    """
    A class that is processed through the airflow dag,
    meant to hold specific functions for the PythonOperator
    as well as hold or reference intermediately created variables and constants
    """
    def __init__(self, args):
        self.args = args

    def setup_database(self):
        """
        Creates the sql database that this airflow dag and dbt will use
        """
        path_folder = self.args["path_folder"]
        path_folder_data_raw = os.path.join(path_folder, "data", "raw")
        file_name_database = "data.db"
        path_file_database = os.path.join(path_folder_data_raw, file_name_database)
        self.args["path_file_database"] = path_file_database

        sql_dbms = duckdb

        db_conn = sql_dbms.connect(path_file_database)
        self.db_conn = db_conn
        
        cursor = db_conn.cursor()
        self.cursor = cursor
    def import_csv_to_sql(self):
        """
        Puts the csv tables into the newly-made sql database
        At minimum, only the offers, orders, and zipcode coordinate tables are necessary for this project. 
        """        
        path_folder = self.args["path_folder"]

        path_file_csv = os.path.join(path_folder,"data","raw","offer_acceptance_orders.csv")
        path_file_sql = os.path.join(path_folder,"src","etl","import_orders_csv_into_sql_table.sql")
        f = open(path_file_sql,"r")
        sql = f.read().format(path_file_csv=path_file_csv)
        self.cursor.sql(sql)
        self.db_conn.commit()
        f.close()

        path_file_csv = os.path.join(path_folder,"data","raw","offer_acceptance_offers.csv")
        path_file_sql = os.path.join(path_folder,"src","etl","import_offers_csv_into_sql_table.sql")
        f = open(path_file_sql,"r")
        sql = f.read().format(path_file_csv=path_file_csv)
        self.cursor.sql(sql)
        self.db_conn.commit()
        f.close()        

        path_file_csv = os.path.join(path_folder,"data","raw","zipcode_coordinates.csv")
        path_file_sql = os.path.join(path_folder,"src","etl","import_zipcode_coordinates_csv_into_sql_table.sql")
        f = open(path_file_sql,"r")
        sql = f.read().format(path_file_csv=path_file_csv)
        self.cursor.sql(sql)
        self.db_conn.commit()
        f.close()     


    def configure_dbt_yamls(self):
        """
        Configures the dbt's profiles and project yaml with the appropriate configs
        """
        path_file_database = self.args["path_file_database"]
        path_file_profile_yaml = "src/dataclean/dbt_flock/profile.yml"
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
        project_yaml["models"]["oa_orders_temp"]["sql"] = "dbt_flock/models/create_oa_orders_temp.sql"
        project_yaml["models"]["oa_offers_temp"]["sql"] = "dbt_flock/models/create_oa_orders_temp.sql"
        project_yaml["models"]["oa"]["sql"] = "dbt_flock/models/create_oa.sql"
        yaml_string = yaml.dump(project_yaml)
        with open(path_file_project_yaml, "w") as f:
            f.write(yaml_string)
            f.close()

    def export_dbt_sql_to_df(self):
        columns_result = self.cursor.sql("SELECT COLUMN_NAME FROM Information_schema.columns where Table_name like 'oa'")
        columns = pd.DataFrame(columns_result.fetchall())

        df_result = cursor.sql("SELECT * FROM oa")
        df = pd.DataFrame(df_result.fetchall())
        df.columns = columns.iloc[:,0].values.tolist()
        print(df.shape)
        self.df = df

def main(args):

    default_args = {
        'owner': 'my_user',
        'start_date': datetime(2022, 1, 1),
        'retries': 1,
        #'retry_delay': datetime.timedelta(minutes=5)
    }

    dag = DAG('flock_dag', default_args=default_args, schedule_interval=None)

    workflow = Tasks(args)
    
    # sql db creation
    task_setup_database = PythonOperator(
        task_id="setup_database",
        python_callable=workflow.setup_database,
        dag=dag,
    )
    
    # sql table
    task_import_csv_to_sql = PythonOperator(
        task_id="import_csv_to_sql",
        python_callable=workflow.setup_database,
        dag=dag,
    )    
    # dbt yaml load
    task_configure_dbt_yamls = PythonOperator(
        task_id="configure_dbt_yamls",
        python_callable=workflow.configure_dbt_yamls,
        dag=dag,
    )
    
    # dbt run
    task_run_dbtflock1 = BashOperator(
        task_id="run_dbtflock1",
        bash_command="src/dataclean/run_dbtflock1.sh",
        dag=dag,        
    )
    task_export_dbt_sql_to_df = BashOperator(
        task_id="export_dbt_sql_to_df",
        python_callable=workflow.export_dbt_sql_to_df,
        dag=dag,       
    )

    task_import_csv_to_sql.set_upstream(task_setup_database)
    task_configure_dbt_yamls.set_upstream(task_import_csv_to_sql)
    task_run_dbtflock1.set_upstream(task_configure_dbt_yamls)
    task_export_dbt_sql_to_df.set_upstream(task_run_dbtflock1)


    


    # metro-cluster
#if __name__ == "__main__": main(args)
