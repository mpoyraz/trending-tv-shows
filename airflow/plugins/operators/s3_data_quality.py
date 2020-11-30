from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class S3DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    template_fields = ['prefix']

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix='',
                 delimiter='',
                 aws_conn_id='aws_default',
                 *args, **kwargs):

        super(S3DataQualityOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        self.log.info('S3DataQualityOperator is starting')
        self.log.info('Bucket : {}, key prefix: {} and delimiter : {}'.format(self.bucket, self.prefix, self.delimiter))

        # Initialize S3 hook
        hook = S3Hook(aws_conn_id='aws_default')

        # Get the list of keys
        keys = hook.list_keys(bucket_name=self.bucket,
                              prefix=self.prefix,
                              delimiter=self.delimiter)

        # Check the number of keys retrieved
        if keys is None or len(keys) == 0:
            # If no keys found, raise a value error
            self.log.info('No keys retrieved for given the given bucket, prefix and delimiter')
            raise ValueError('No keys retrieved for given the given bucket, prefix and delimiter')
        else:
            self.log.info('{} keys are retrieved, the list of keys:'.format(len(keys)))
            self.log.info(keys)

        self.log.info('S3DataQualityOperator is completed')