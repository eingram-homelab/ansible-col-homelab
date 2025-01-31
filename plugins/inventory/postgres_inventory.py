
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

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

import psycopg2
from psycopg2 import Error
import os
import json
import getpass
from ansible.plugins.inventory import BaseInventoryPlugin
from ansible.errors import AnsibleParserError

class InventoryModule(BaseInventoryPlugin):
    NAME = "postgres_inventory"

    def verify_file(self, path):
        if not path.endswith('postgres_plugin.yml') and not path.endswith('postgres_plugin.yaml'):
            return False
        return super(InventoryModule, self).verify_file(path)
    
    def parse(self, inventory, loader, path, cache=True):
        super(InventoryModule, self).parse(inventory, loader, path, cache)
        self._read_config_data(path)
        try:
            db_table = self.get_option('db_table')
            db_params = {
                "db_host": self.get_option('db_host'),
                "db_port": self.get_option('db_port'),
                "db_name": self.get_option('db_name'),
                "db_user": self.get_option('db_user'),
                "db_password": self.get_option('db_password')
            }
        except AnsibleParserError as e:
            raise AnsibleParserError(f"Missing required options: {e}")
            
        with PostgresInventory(**db_params) as inventory:
            records = inventory.get_inventory(db_table)
        
            # json_output = inventory.convert_to_json(records)
            # print(json_output)
            for row in records:
                # print(f"Getting info for {row[4]}")
                server_name = row[0]
                os_ver = row[1]
                hardware_type = row[2]
                env = row[3]
                server_fqdn = row[4]
                ip_address = row[5]
                os_family = row[6]
                
                if os_family == "RedHat" or os_family == "Debian":
                    ansible_user = "ansible"
                elif os_family == "Windows" and "homelab.local" in server_fqdn:
                    ansible_user = "ansible@HOMELAB.LOCAL"
                elif os_family == "Windows" and "local.lan" in server_fqdn:
                    ansible_user = "ansible"
                else:
                    print(f"Unknown OS family for {server_fqdn}")
                    quit()
                
                self.inventory.add_host(server_fqdn)
                self.inventory.set_variable(server_fqdn, "ansible_host", ip_address)
                self.inventory.set_variable(server_fqdn, "ansible_user", ansible_user)
        #         if os_family not in inventory_dict:
        #             inventory_dict[os_family] = {
        #                 "hosts": []
        #             }
        #             inventory_dict["all"]["children"].append(os_family)
                    
        #         inventory_dict[os_family]["hosts"].append(server_fqdn)
        #         inventory_dict["_meta"]["hostvars"][server_fqdn] = {
        #             "ansible_host": ip_address,
        #             "os": os_ver,
        #             "hardware_type": hardware_type,
        #             "env": env,
        #             "ansible_user": ansible_user
        #         }
        # print(json.dumps(inventory_dict, indent=2))

class PostgresInventory:
    """ Class to connect to a PostgreSQL database and retrieve inventory data """
    def __init__(self, db_host, db_port, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

    def __enter__(self):
        self.connect()
        return self
    
    def connect(self):
        try:
            self.connection = psycopg2.connect(
                user = self.db_user,
                password = self.db_password,
                host = self.db_host,
                port = self.db_port,
                database = self.db_name
            )
            # print("Connected to the database")
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
            raise
            exit(1)
            
    def get_inventory(self, db_table):
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {db_table}")
        records = cursor.fetchall()
        return records
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.connection.close()
        # print("Connection closed")


    