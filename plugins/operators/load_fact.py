from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    """
    Loads fact table in Redshift from data in staging table
    
    :redshift_conn_id: Redshift connection ID
    :table: Target table in Redshift to load
    :sql_stmt: SQL query for getting data to load into target table
    
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table = table
        self.sql_stmt= sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading fact table {self.table}")
        sql = """INSERT INTO {} 
                    {}; 
                    COMMIT;""".format(self.table, 
                                      self.sql_stmt)
        redshift.run(sql)
        self.log.info('LoadFactOperator loaded')
