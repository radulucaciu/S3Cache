import boto3
import datetime
import hashlib
import pandas as pd
import sqlalchemy as sa

from botocore.errorfactory import ClientError
from io import StringIO


class S3Cache:
    _GRANULARITIES = set(['daily', 'weekly', 'monthly'])

    def __init__(self, bucket, folder, conn=None, host=None, db=None, port=5439, username=None, password=None, verbose=False):
        if conn is not None:
            self._conn = conn
        else:
            if (host and db and port and username and password) is None:
                raise Exception("If conn is None, host, db, port, username and password need to be provided and not be None")

        self._verbose = verbose
        self._conn = self._create_db_connection(host, db, username, password, port, dialect='redshift', driver='redshift_connector')
        self._bucket = bucket
        self._folder = folder


    def query(self, sql: str, refresh: str = 'weekly'):
        if refresh not in self._GRANULARITIES:
            raise Exception("refresh needs to be one of [{}]".format(','.join(self._GRANULARITIES)))

        file_key = self._get_file_key(sql, refresh)
        try:
            df = self._get_data_from_bucket(file_key)
            self._log("Got cached data...")
        except ClientError:
            self._log("Could not find cached data, pulling from Redshift...")
            df = pd.read_sql(sql, self._conn)
            self._log("Got data from Redshfit, writing cache...")
            self._write_data_to_bucket(df, file_key)
        
        return df

    def _create_db_connection(self, host: str, db: str, username: str,
                         password: str, port: int, dialect: str, driver: str=None) -> sa.engine.Engine:
        if driver is None:
            full_dialect = dialect
        else:
            full_dialect = '{d}+{driver}'.format(d=dialect, driver=driver)
        url = "{d}://{u}:{p}@{h}:{port}/{db}".format(
            d=full_dialect,
            u=username,
            p=password,
            h=host,
            port=port,
            db=db)
        engine = sa.create_engine(url, connect_args={'sslmode': 'verify-ca'})
        return engine.connect()

    def _log(self, msg):
        if self._verbose:
            print(msg)

    def _get_timestamp_for_granularity(self, granularity: str) -> str:
        if granularity == 'daily':
            dt = datetime.date.today()
        elif granularity == 'weekly':
            dt = datetime.date.today()
            dt = dt - datetime.timedelta(days=dt.weekday())
        elif granularity == 'monthly':
            dt = datetime.date.today().replace(day=1)
            
        return int(datetime.datetime.combine(dt, datetime.datetime.min.time()).timestamp())


    def _get_file_key(self, sql: str, granularity: str):
        filename = '{}_{}.{}'.format(
            hashlib.md5(sql.encode()).hexdigest(), 
            self._get_timestamp_for_granularity(granularity), 'csv')
        return '{}/{}'.format(self._folder, filename)


    def _get_data_from_bucket(self, pathInBucket: str):
        s3_client = boto3.client('s3')
        s3 = boto3.resource('s3')

        # Check to see if the file exists on S3. This is much faster than other methods like
        # loading the file. Will raise a *ClientError* if file is not found
        s3_client.head_object(Bucket=self._bucket, Key=pathInBucket)

        # Read the content into a pandas dataframe and returns
        response = s3_client.get_object(Bucket=self._bucket, Key=pathInBucket)
        return pd.read_csv(response.get("Body"))


    def _write_data_to_bucket(self, df: pd.DataFrame, pathInBucket: str):
        s3_client = boto3.client('s3')
        s3 = boto3.resource('s3')
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)

        obj_write = s3.Object(self._bucket, pathInBucket)
        obj_write.put(Body=csv_buffer.getvalue())
