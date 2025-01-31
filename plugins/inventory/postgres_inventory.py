
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r"""
    name: postgres_inventory
    short_description: An Ansible plugin that retrieves inventory data from Postgres
    author: Edward Ingram
    description:
        - Reads servers from a Postgres DB and populates inventory
    extends_documentation_fragment:
        - inventory_cache
    requirements:
        - psycopg2
    options:
        plugin:
            description: The name of this plugin
            required: true
            choices: ['eingram23.homelab.postgres_inventory']
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
            description: 
            - Database user
            - Accepts vault encrypted variable.
            - Accepts Jinja to template the value
            required: true
        db_password:
            description: 
            - Database password or use environment variable
            - Accepts vault encrypted variable.
            - Accepts Jinja to template the value
            required: false

"""

import psycopg2

from ansible.plugins.inventory import BaseInventoryPlugin, Cacheable, Templar
from ansible.errors import AnsibleError, AnsibleParserError
from ansible.parsing.yaml.objects import AnsibleVaultEncryptedUnicode

try:
    # pyscopg2 is required for this plugin to connect to postgres database
    import psycopg2
except Exception as e:
    print(f"Failed to import psycopg2: {e}")
    raise SystemExit(1)

from psycopg2 import Error

class InventoryModule(BaseInventoryPlugin):
    NAME = "eingram23.homelab.postgres_inventory"

    def verify_file(self, path):
        ''' return true/false if this is possibly a valid file for this plugin to consume '''
        valid = False
        if super(InventoryModule, self).verify_file(path):
            # base class verifies that file exists and is readable by current user
            if path.endswith(('postgres.yaml', 'postgres.yml')):
                valid = True
        return valid
    
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
        except AnsibleError as e:
            raise AnsibleError(f"Missing required options: {e}")
            
        if self.templar.is_template(db_password):
            db_password = self.templar.template(variable=db_password, disable_lookups=False)
        elif isinstance(db_password, AnsibleVaultEncryptedUnicode):
            db_password = db_password.data

        if self.templar.is_template(db_user):
            db_user = self.templar.template(variable=db_user, disable_lookups=False)
        elif isinstance(db_user, AnsibleVaultEncryptedUnicode):
            db_user = db_user.data
            
        db_params = {
            "db_host": self.get_option('db_host'),
            "db_port": self.get_option('db_port'),
            "db_name": self.get_option('db_name'),
            "db_user": self.get_option('db_user'),
            "db_password": self.get_option('db_password')
        }

        with PostgresInventory(**db_params) as inventory:
            records = inventory.get_inventory(db_table)
            for row in records:
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
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
            raise SystemExit(1)
            
    def get_inventory(self, db_table):
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {db_table}")
        records = cursor.fetchall()
        return records
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.connection.close()



    