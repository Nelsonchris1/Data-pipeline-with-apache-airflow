from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    :aws_credentials_id:  AWS credentials ID containing Access key and secrete access key
    :redshift_conn_id: Redshift connection ID
    :table: Target staging table in redshift to copy data into
    :s3_bucket: S3 bucket where JSON data resides
    :s3_key: Path in S3 bucket where JSON data files reside
    :Json_path: JSON file path of the data to be copied
    :file_Type: file type e.g json, csv, txt etc
    :region : AWS Region where the source data is located
    """
    
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 file_type="",
                 delimiter=",",
                 ignore_headers=1,
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.file_type = file_type
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.region = region

    def execute(self, context):
        
        aws_hook= AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Copying data from S3 bucket to Reshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        
        if self.file_type == "json":
            qry = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY '{credentials.secret_key}' REGION '{self.region}' JSON '{self.json_path}' COMPUPDATE OFF"
            
            redshift.run(qry)
            
        if self.file_type == "csv":
            qry = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY '{credentials.secret_key}' REGION '{self.region}' IGNOREHEADER '{self.ignore_headers}' DELIMETER '{self.delimeter}'"
            
            redshift.run(qry)
        
        self.log.info('StageToRedshiftOperator copied')





