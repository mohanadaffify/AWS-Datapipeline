from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
    INSERT INTO {}
        {};
    """
    delete_Sql="""
       TRUNCATE TABLE {};
       """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 load_sql_statment="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.load_sql_statment=sql_statment

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Deleting dimension table data : {self.table}")
        redshift.run(LoadDimensionOperator.delete_sql.format(self.table))
        
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.load_sql_statment
              )
        
        self.log.info(f"Loading Dimensions table '{self.table}' into Redshift")
        redshift.run(formatted_sql)