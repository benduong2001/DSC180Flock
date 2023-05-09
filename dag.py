
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
        file_name_database = "flockdata.db"
        path_file_database = os.path.join(path_folder,"src", "dataclean", "dbtflock", "data",file_name_database)
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
        Configures the dbt's project yaml with the appropriate configs
        """
        
        path_file_project_yaml = "dbtflock/dbt_project.yml"
        with open(path_file_project_yaml, 'r') as f:
            project_yaml = yaml.safe_load(f)
            f.close()
        params = {
        "filtering_ftl": 0,
        "explode_references": 1,
        "add_features_for_visuals_map_zips": 0,
        }
        project_yaml["vars"] = params
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
        oa = df
        self.oa = oa

    def add_metro_cluster(self):
        oa = self.oa
        metro_cluster = util.temp_build_metro_cluster(oa)
        metro_cluster.plot_map(path_folder=path_folder)
        orig_proximity_column_names = ["ORIG_"+metro_cluster.group_column_name+"="+str(i) for i in range(metro_cluster.group_amount)]
        orig_metro_cluster_columns = util.temp_build_metro_cluster_columns(oa, metro_cluster,"X_COORD_ORIG","Y_COORD_ORIG",orig_proximity_column_names)
        oa = util.add_metro_cluster_columns(oa, orig_metro_cluster_columns)

        #oa_numerical_column_names += orig_proximity_column_names

        dest_proximity_column_names = ["DEST_"+metro_cluster.group_column_name+"="+str(i) for i in range(metro_cluster.group_amount)]
        dest_metro_cluster_columns = util.temp_build_metro_cluster_columns(oa, metro_cluster,"X_COORD_DEST","Y_COORD_DEST",dest_proximity_column_names)
        oa = util.add_metro_cluster_columns(oa, dest_metro_cluster_columns)
        #logger.info("oa.shape", oa.shape)
        #oa_numerical_column_names += dest_proximity_column_names

        self.metro_cluster = metro_cluster
        self.oa = oa

    def submodels(self):
        
        












def dataclean(args):

    """## Data File Reading"""
    path_folder = args["path_folder"]
    path_folder_data = args["path_folder_data"]
    path_folder_data_raw = os.path.join(path_folder_data,"raw")
    path_folder_data_temp = os.path.join(path_folder_data,"temp")
    path_folder_data_final = os.path.join(path_folder_data,"final")

    logging.basicConfig(filename=os.path.join(path_folder,'generated_visualizations','logged.txt'),level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

    #print(logger)
    logger = logging.getLogger()


    
    column_names_metadata = 
    oa_numerical_column_names = column_names_metadata["oa"]["numerical"]
    oa_orders_boolean_column_names = column_names_metadata["oa_orders"]["boolean"]
    oa_orders_numerical_column_names = column_names_metadata["oa_orders"]["numerical"]
    oa_orders_loggable_numerical_column_names = column_names_metadata["oa_orders"]["loggable"]
    oa_orders_categorical_column_names = column_names_metadata["oa_orders"]["categorical"]
    weight_column_name = "LEAD_TIME"


    # part 4: the model building and training 

    # drops the column names in dropped_column_names
    # At this point, oa should FULLY have numerical column names
    # However, if the variable do_ohe was set to False, then the categorical column names are still waiting to be encoded.

    numerical_column_names_1 = oa_numerical_column_names + oa_orders_numerical_column_names + oa_orders_boolean_column_names
    numerical_column_names_2 = oa_orders_loggable_numerical_column_names
    categorical_column_names_0 = oa_orders_categorical_column_names
    temp_zscore_step = []
    # temp_zscore_step = [('scaler', StandardScaler())]
    numerical_pipeline_1 = Pipeline([
                ('imputer', SimpleImputer(strategy='mean')),
            ]+temp_zscore_step)
    numerical_pipeline_2 = Pipeline([
                ('imputer', SimpleImputer(strategy='mean')),
                ('log_transform', FunctionTransformer(np.log1p, feature_names_out="one-to-one")),
            ]+temp_zscore_step)
    numerical_transformer = ColumnTransformer(
        transformers=[
            ('num_1', numerical_pipeline_1, numerical_column_names_1),
            ('num_2', numerical_pipeline_2, numerical_column_names_2)])

    categorical_pipeline_0 = Pipeline([
                ('imputer', SimpleImputer(strategy='constant', fill_value='__NULL__')),
                ('ohe',OneHotEncoder(handle_unknown='ignore', categories='auto', sparse=False, dtype=int)),
            ])
    categorical_transformer = ColumnTransformer(
        transformers=[('cat_0',categorical_pipeline_0, categorical_column_names_0)]
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numerical_transformer, numerical_column_names_1 + numerical_column_names_2),
            ('cat', categorical_transformer, categorical_column_names_0)
        ]
    )
    preprocessor_pipeline = Pipeline([("preprocessor",preprocessor)])



    target_column_names=[
        "LOG_RATE_USD",
        "SD_LOG_RATE_USD",
        "ORDER_OFFER_AMOUNT",
    ]
    # there are 2 models to make, one for avg and one for stdev,
    avg_pipeline = Pipeline([
        ('preprocessor_pipeline', preprocessor),
        ('model',LinearRegression())
    ])
    np.random.seed(1)
    target_column_name="LOG(RATE_USD)"
    temp_target_column_names = target_column_names.copy()
    temp_target_column_names.remove(target_column_name)
    input_df = oa.drop(columns=temp_target_column_names)    
    temp_train_test_indexer = np.random.choice([1,0],size=input_df.shape[0],p=[4/5,1/5])
    X = input_df.drop(columns=[target_column_name])
    avg_X = X
    y = input_df[target_column_name]
    X_train = X.loc[temp_train_test_indexer == 1]
    y_train = y.loc[temp_train_test_indexer == 1]
    X_test = X.loc[temp_train_test_indexer == 0]
    y_test = y.loc[temp_train_test_indexer == 0]
    X_train_weight_column = X_train[weight_column_name]
    X_train.drop(columns=[weight_column_name],inplace=True)
    X_test.drop(columns=[weight_column_name],inplace=True)
    avg_pipeline.fit(X_train,y_train,model__sample_weight=X_train_weight_column)
    self.avg_pipeline = avg_pipeline
    predictions = avg_pipeline.predict(X_test)

    logger.info("\n CorrCoef of Avg Model: {0}".format(str(np.corrcoef(y_test,predictions)[0][1])))
    fig, ax = plt.subplots(figsize=(10,5))
    ax.set_title("Actual vs Predicted Avg Rates (Log-transformed)")
    ax.set_xlabel("Actual Avg Rates Test Y-values")
    ax.set_ylabel("Predicted Avg Rates Test Y-values")
    ax.scatter(y_test,predictions,alpha=0.3,s=5)
    fig.savefig(os.path.join(path_folder,'generated_visualizations','avg_model_r2_scatter.png'),bbox_inches='tight')

    
    # now, this next part is for stdev model
    sd_pipeline = Pipeline([
        ('preprocessor_pipeline', preprocessor),
        ('model',RandomForestClassifier(5,class_weight="balanced"))
    ])
    np.random.seed(1)
    target_column_name="SD_LOG(RATE_USD)"
    temp_target_column_names = target_column_names.copy()
    temp_target_column_names.remove(target_column_name)
    input_df = oa.drop(columns=temp_target_column_names)
    temp_train_test_indexer = np.random.choice([1,0],size=input_df.shape[0],p=[4/5,1/5])
    X = input_df.drop(columns=[target_column_name])
    sd_X = X
    sd_median = np.median(input_df[target_column_name])
    y = (input_df[target_column_name]>sd_median).astype(int)
    logger.info("\n sd_median: {0}".format(str(sd_median)))    
    X_train = X.loc[temp_train_test_indexer == 1]
    y_train = y.loc[temp_train_test_indexer == 1]
    X_test = X.loc[temp_train_test_indexer == 0]
    y_test = y.loc[temp_train_test_indexer == 0]
    X_train_weight_column = X_train[weight_column_name]
    X_train.drop(columns=[weight_column_name],inplace=True)
    X_test.drop(columns=[weight_column_name],inplace=True)
    sd_pipeline.fit(X_train,y_train,model__sample_weight=X_train_weight_column)
    self.sd_pipeline = sd_pipeline
    get_sd_by_tier = np.vectorize(lambda x: sd_median*int(x))

    output_df = pd.DataFrame()
    output_df["REFERENCE_NUMBER"] = reference_number_column
    avg_prediction_column = avg_pipeline.predict(avg_X)
    output_df["PREDICTED_LOG_AVG"] = avg_prediction_column
    sd_prediction_column = sd_pipeline.predict(sd_X)
    sd_prediction_column = get_sd_by_tier(sd_prediction_column)
    output_df["PREDICTED_STDEV"] = sd_prediction_column
    # writing output file
    file_name_temp_avg_stdev = args["file_name_temp_avg_stdev"]
    path_file_temp_avg_stdev = os.path.join(path_folder_data_temp, file_name_temp_avg_stdev)
    output_df.to_csv(path_file_temp_avg_stdev,index=False)



































def main(args):

    workflow = Tasks(args)
    workflow.setup_database()
    workflow.configure_dbt_yamls()
    workflow.run_dbtflock1()
    workflow.export_dbt_sql_to_df()
    workflow.add_metro_cluster()
    


    metro_cluster = util.temp_build_metro_cluster(oa)
    metro_cluster.plot_map(path_folder=path_folder)
    orig_proximity_column_names = ["ORIG_"+metro_cluster.group_column_name+"="+str(i) for i in range(metro_cluster.group_amount)]
    orig_metro_cluster_columns = util.temp_build_metro_cluster_columns(oa, metro_cluster,"X_COORD_ORIG","Y_COORD_ORIG",orig_proximity_column_names)
    oa = util.add_metro_cluster_columns(oa, orig_metro_cluster_columns)

    oa_numerical_column_names += orig_proximity_column_names

    dest_proximity_column_names = ["DEST_"+metro_cluster.group_column_name+"="+str(i) for i in range(metro_cluster.group_amount)]
    dest_metro_cluster_columns = util.temp_build_metro_cluster_columns(oa, metro_cluster,"X_COORD_DEST","Y_COORD_DEST",dest_proximity_column_names)
    logger.info("\n row loss metro cluster 2")
    oa = util.add_metro_cluster_columns(oa, dest_metro_cluster_columns)
    #logger.info("oa.shape", oa.shape)
    oa_numerical_column_names += dest_proximity_column_names
    

    default_args = {
        'owner': 'my_user',
        'start_date': datetime(2022, 1, 1),
        'retries': 1,
        #'retry_delay': datetime.timedelta(minutes=5)
    }

    dag = DAG('flock_dag', default_args=default_args, schedule_interval=None)
    
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
    task_export_dbt_sql_to_df = PythonOperator(
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
