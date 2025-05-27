# Airflow Data Migration Project

This project is designed to facilitate the migration of data from an Oracle database to a Cassandra database using Apache Airflow. The project includes a set of DAGs, utilities, and configurations to streamline the data migration process.

## Project Structure

```
airflow-data-migration
├── dags
│   ├── __init__.py
│   ├── oracle_to_cassandra_dag.py
│   └── utils
│       ├── __init__.py
│       ├── oracle_connection.py
│       └── cassandra_connection.py
├── plugins
│   ├── __init__.py
│   └── operators
│       ├── __init__.py
│       └── migration_operators.py
├── config
│   ├── airflow.cfg
│   └── connections.yaml
├── scripts
│   ├── setup_connections.py
│   └── migrate_data.py
├── sql
│   ├── oracle_queries.sql
│   └── cassandra_schema.cql
├── logs
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## Setup Instructions

1. **Clone the Repository**: 
   Clone this repository to your local machine.

   ```bash
   git clone <repository-url>
   cd airflow-data-migration
   ```

2. **Install Dependencies**: 
   Install the required Python packages using pip.

   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Airflow**: 
   Modify the `config/airflow.cfg` file to set up your Airflow environment according to your needs.

4. **Set Up Connections**: 
   Use the `scripts/setup_connections.py` script to set up the necessary connections for Oracle and Cassandra in Airflow.

5. **Run Airflow**: 
   Start the Airflow services using Docker.

   ```bash
   docker-compose up
   ```

6. **Access the Airflow UI**: 
   Open your web browser and navigate to `http://localhost:8080` to access the Airflow web interface.

## Usage

- The main DAG for migrating data from Oracle to Cassandra is defined in `dags/oracle_to_cassandra_dag.py`. 
- You can trigger the DAG from the Airflow UI or schedule it to run at specified intervals.
- The migration process involves extracting data from Oracle, transforming it as needed, and loading it into Cassandra.

## Logging

Logs generated during the execution of the DAGs will be stored in the `logs` directory.

## Contributing

Contributions to this project are welcome. Please submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.