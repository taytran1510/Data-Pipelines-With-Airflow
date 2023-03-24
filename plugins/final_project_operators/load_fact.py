from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",
                 append_only = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if not self.append_only:
            self.log.info("Delete {} fact table".format(self.table))
            redshift_hook.run("DELETE FROM {}".format(self.table))

        self.log.info(f'Loading fact table {self.table}')
        insert_statement = f"INSERT INTO {self.table} \n{self.sql}"
        redshift_hook.run(insert_statement)
        self.log.info(f"Successfully completed insert into {self.table}")