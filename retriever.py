from psycopg2.extras import RealDictCursor
from dbInit import DBInitializer


class Retriever():
    def __init__(self):
        self.initializer = DBInitializer()
        self.connection = self.initializer.login()
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)

    def execute_query_for_location(self, lat, lon, query, isResultList):
        params = (lat, lon,)
        self.cursor.execute(query, params)

        if isResultList:
            return self.cursor.fetchall()
        else:
            return self.cursor.fetchone()

    def get_data_for_location(self, lat, lon):
        query = """
        SELECT forecastTime, Temperature, Precipitation
        FROM forecasts 
        WHERE latitude = %s AND longitude = %s
        """

        return self.execute_query_for_location(lat, lon, query, True)

    def get_summary_for_location(self, lat, lon):
        query = """
        SELECT AVG(Temperature) AS "Temperature_avg",
              AVG(Precipitation) AS "Precipitation_avg",
              MAX(Temperature) AS "Temperature_max",
              MAX(Precipitation) AS "Precipitation_max",
              MIN(Temperature) AS "Temperature_min",
              MIN(Precipitation) AS "Precipitation_min"
        FROM forecasts
        WHERE latitude = %s AND longitude = %s
        """

        name_to_value = self.execute_query_for_location(lat, lon, query, False)
        return self.arrange_by_value_type(name_to_value)

    def arrange_by_value_type(self, name_to_value):
        max_values = {}
        max_values['Temperature'] = name_to_value['Temperature_max']
        max_values['Precipitation'] = name_to_value['Precipitation_max']

        min_values = {}
        min_values['Temperature'] = name_to_value['Temperature_min']
        min_values['Precipitation'] = name_to_value['Precipitation_min']

        avg_values = {}
        avg_values['Temperature'] = name_to_value['Temperature_avg']
        avg_values['Precipitation'] = name_to_value['Precipitation_avg']

        result = {}
        result['max'] = max_values
        result['min'] = min_values
        result['avg'] = avg_values

        return result




