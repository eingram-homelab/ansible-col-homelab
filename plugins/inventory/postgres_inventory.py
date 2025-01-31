import psycopg2
from psycopg2 import Error
import os
import json
import getpass
from ansible.plugins.inventory import BaseInventoryPlugin
from ansible.errors import AnsibleParserError

DOCUMENTATION = r"""
    name: postgres_inventory
    plugin_type: inventory
    short_description: An Ansible plugin that retrieves inventory data from Postgres
    description:
        - Reads servers from a Postgres DB and populates inventory
    options:
        plugin:
            description: The name of this plugin
            required: true
            choices: ['postgres_inventory']
        db_host:
            description: Postgres host
            required: true
        db_port:
            description: Postgres port
            required: false
            default: "5432"
        db_name:
            description: Database name
            required: true
        db_table:
            description: Table name
            required: true
        db_user:
            description: Database user
            required: true
        db_password:
            description: Database password or use environment variable
            required: false
"""



class InventoryModule(BaseInventoryPlugin):
    NAME = "postgres_inventory"

    def db_connection(db_host, db_port, db_name, db_user, db_password):
        try:
            connection = psycopg2.connect(
                user = db_user,
                password = db_password,
                host = db_host,
                port = db_port,
                database = db_name
            )
            # cursor = connection.cursor()
            print("Connected to the database")
            return connection
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)

    def get_inventory(connection, db_table):
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {db_table}")
        records = cursor.fetchall()
        return records

    def convert_to_json(inventory_data):
        json_data = []
        for row in inventory_data:
            json_data.append({
                "server_name": row[0],
                "os": row[1],
                "hardware_type": row[2],
                "env": row[3],
                "server_fqdn": row[4],
                "ip_address": row[5]
            })
        print(json.dumps(json_data, indent=4))
    
    def verify_file(self, path):
        if not path.endswith('postgres_plugin.yml') and not path.endswith('postgres_plugin.yaml'):
            return False
        return super(InventoryModule, self).verify_file(path)
    
    def parse(self, inventory, loader, path, cache=True):
        super(InventoryModule, self).parse(inventory, loader, path, cache)
        self._read_config_data(path)
        try:
            db_host = self.get_option('db_host')
            db_port = self.get_option('db_port')
            db_name = self.get_option('db_name')
            db_table = self.get_option('db_table')
            db_user = self.get_option('db_user')
            db_password = self.get_option('db_password')
        except AnsibleParserError as e:
            raise AnsibleParserError(f"Missing required options: {e}")
        connection = db_connection(db_host, db_port, db_name, db_user, db_password)
        records = get_inventory(connection, db_table)
        for row in records:
            hostname = row[0]
            self.inventory.add_host(hostname)
            # Map additional columns as needed
            self.inventory.set_variable(hostname, 'ansible_host', row[5])
