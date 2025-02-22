TABLES_CONFIG = [
    {
        'table_name': 'airlines',
        'csv_file': 'airlines.csv',
        'primary_key': 'iata_code',
        'foreign_keys': [],
        'column_mapping': {
            'iata_code': 'CHAR(8)',      
            'airline': 'VARCHAR(255)'     
        }
    },
    {
        'table_name': 'airports',
        'csv_file': 'airports.csv',
        'primary_key': 'iata_code',
        'foreign_keys': [],
        'column_mapping': {
            'iata_code': 'CHAR(8)',      
            'airport': 'VARCHAR(255)',    
            'city': 'VARCHAR(100)',       
            'state': 'VARCHAR(100)',      
            'country': 'VARCHAR(100)',    
            'latitude': 'DOUBLE PRECISION',
            'longitude': 'DOUBLE PRECISION'
        }
    },
    {
        'table_name': 'flights',
        'csv_file': 'flights.csv',
        'primary_key': 'unique_id',
        'foreign_keys': [],
        'column_mapping': {
            'year': 'INTEGER',
            'month': 'INTEGER',
            'day': 'INTEGER',
            'day_of_week': 'INTEGER',
            'airline': 'CHAR(8)',            
            'flight_number': 'VARCHAR(50)',  
            'tail_number': 'VARCHAR(50)',    
            'origin_airport': 'VARCHAR(50)', 
            'destination_airport': 'VARCHAR(50)', 
            'scheduled_departure': 'VARCHAR(50)',  
            'departure_time': 'VARCHAR(50)',       
            'departure_delay': 'INTEGER',
            'taxi_out': 'INTEGER',
            'wheels_off': 'VARCHAR(50)',    
            'scheduled_time': 'INTEGER',
            'elapsed_time': 'INTEGER',
            'air_time': 'INTEGER',
            'distance': 'INTEGER',
            'wheels_on': 'VARCHAR(50)',     
            'taxi_in': 'INTEGER',
            'scheduled_arrival': 'VARCHAR(100)',  
            'arrival_time': 'VARCHAR(100)',       
            'arrival_delay': 'INTEGER',
            'diverted': 'BOOLEAN',
            'cancelled': 'BOOLEAN',
            'cancellation_reason': 'VARCHAR(255)',  
            'air_system_delay': 'INTEGER',
            'security_delay': 'INTEGER',
            'airline_delay': 'INTEGER',
            'late_aircraft_delay': 'INTEGER',
            'weather_delay': 'INTEGER'
        }
    }
]

COLUMN_TYPE_MAPPING = {config['table_name']: config['column_mapping'] for config in TABLES_CONFIG}