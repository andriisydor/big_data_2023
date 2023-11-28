
class CSVManager:
    """
    Manages data read and write using csv files.
    """

    def __init__(self, spark_session, data_directory_path):
        """
        Args:
            spark_session (pyspark.sql.SparkSession): current spark session.
            data_directory_path (string): full path to data directory of the project.
        """

        self.__spark_session = spark_session
        self.__data_directory_path = data_directory_path

    def read(self, path_in_data_dir, schema=None, header=True):
        """Reads PySpark dataframe from csv file from data_directory_path.

        Args:
            path_in_data_dir (string): path to csv file or directory with csv files in data_directory_path.
            header (bool): does csv have header as first row.
            schema: schema for dataset

        Returns:
            pyspark.sql.DataFrame: dataframe from csv file.
        """

        source_path = f'{self.__data_directory_path}{path_in_data_dir}'
        if schema:
            data_frame = self.__spark_session.read.csv(source_path, schema=schema, header=header)
        else:
            data_frame = self.__spark_session.read.csv(source_path, header=header)
        return data_frame

    def write(self, data_frame, directory_path_in_data_dir):
        """Writes PySpark dataframe to csv file in data_directory_path.

        Args:
            data_frame (pyspark.sql.DataFrame): dataframe to write.
            directory_path_in_data_dir (string): path to result directory in data_directory_path.
        """

        directory_to_save_path = f'{self.__data_directory_path}{directory_path_in_data_dir}'
        data_frame.write.csv(directory_to_save_path, header=True)
