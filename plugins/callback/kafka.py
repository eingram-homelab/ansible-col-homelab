from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = '''
    name: eingram23.homelab.kafka
    type: notification
    short_description: Sends Ansible events to Kafka
    description:
        - This callback plugin sends Ansible events as messages to a Kafka topic
    requirements:
        - confluent_kafka python package
    options:
      bootstrap_servers:
        description: Comma-separated list of Kafka broker addresses
        env:
          - name: KAFKA_BOOTSTRAP_SERVERS
        ini:
          - section: callback_kafka
            key: bootstrap_servers
      topic:
        description: Kafka topic to publish messages to
        env:
          - name: KAFKA_TOPIC
        ini:
          - section: callback_kafka
            key: topic
        default: test-topic1
      ssl_ca_pem:
        description: Path to CA certificate for SSL connection
        env:
          - name: KAFKA_SSL_CA_PEM
        ini:
          - section: callback_kafka
            key: ssl_ca_pem
        required: false
      ssl_certificate_pem:
        description: Path to client certificate for SSL connection
        env:
          - name: KAFKA_SSL_CERTIFICATE_PEM
        required: false
      ssl_key_pem:
        description: Path to client key for SSL connection
        env:
          - name: KAFKA_SSL_KEY_PEM
        required: false
      ssl_key_password:
        description: Password for the client key
        env:
          - name: KAFKA_SSL_KEY_PASSWORD
        required: false
      security_protocol:
        description: Security protocol to use (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
        env:
          - name: KAFKA_SECURITY_PROTOCOL
        default: SSL
'''

import json
import socket
import uuid
import datetime
from ansible.plugins.callback import CallbackBase
from ansible.errors import AnsibleError, AnsibleParserError
from ansible.module_utils.common.text.converters import to_native

try:
    from confluent_kafka import Producer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False


class CallbackModule(CallbackBase):
    """
    Ansible callback plugin that sends events to Kafka
    """

    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'notification'
    CALLBACK_NAME = 'eingram23.homelab.kafka'
    CALLBACK_NEEDS_ENABLED = True

    def __init__(self):
        super(CallbackModule, self).__init__()
        self.producer = None
        self.host = socket.gethostname()
        self.session_id = str(uuid.uuid4())
        self.task_start_times = {}
        
        # Init extra vars
        self.extra_vars = {}
        
        # Track hosts already reported as unreachable for this playbook run
        self.unreachable_seen = set()

        # Ensure disabled flag has a default
        self.disabled = False
        
        # Init options
        self.bootstrap_servers = None
        self.topic = None
        self.ssl_ca_pem = None
        self.ssl_certificate_pem = None
        self.ssl_key_pem = None
        self.ssl_key_password = None
        self.security_protocol = None

    def set_options(self, task_keys=None, var_options=None, direct=None):
        super(CallbackModule, self).set_options(task_keys=task_keys, var_options=var_options, direct=direct)

        # Get configuration from environment variables or set defaults
        self.bootstrap_servers = self.get_option('bootstrap_servers')
        self.topic = self.get_option('topic')
        self.ssl_ca_pem = self.get_option('ssl_ca_pem')
        self.ssl_certificate_pem = self.get_option('ssl_certificate_pem')
        self.ssl_key_pem = self.get_option('ssl_key_pem')
        self.ssl_key_password = self.get_option('ssl_key_password')
        self.security_protocol = self.get_option('security_protocol')

        # Initialize Kafka producer if confluent-kafka is available
        if not HAS_KAFKA:
            self.disabled = True
            self._display.warning("The 'confluent_kafka' python module is required for the kafka_callback plugin")
            return

        try:
            conf = {
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': f'ansible-{self.host}',
                'security.protocol': self.security_protocol,
                'delivery.timeout.ms': 30000
            }

            # Add SSL config if security protocol is SSL
            if self.security_protocol in ('SSL', 'SASL_SSL'):
                conf['ssl.ca.pem'] = self.ssl_ca_pem
                
                if self.ssl_certificate_pem:
                    conf['ssl.certificate.pem'] = self.ssl_certificate_pem
                
                if self.ssl_key_pem:
                    conf['ssl.key.pem'] = self.ssl_key_pem
                
                if self.ssl_key_password:
                    conf['ssl.key.password'] = self.ssl_key_password

            self.producer = Producer(conf)
            self._display.display("Kafka producer initialized")
        except Exception as e:
            self.disabled = True
            raise AnsibleError("Error initializing Kafka producer: %s" % to_native(e))

    def delivery_report(self, err, msg):
        """Callback for message delivery results"""
        if err is not None:
            raise AnsibleError('Message delivery failed: %s' % to_native(err))
        else:
            self._display.display(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

    def send_message(self, event_type, event_data):
        """Send event to Kafka topic"""
        if self.disabled or not self.producer:
            return
        
        try:
            # Create the message with common fields
            message = {
                'event_type': event_type,
                'timestamp': datetime.datetime.now().isoformat(),
                'host': self.host,
                'session_id': self.session_id,
                'data': event_data
            }
            
            # Convert message to JSON and send to Kafka
            message_json = json.dumps(message)
            self.producer.produce(
                topic=self.topic,
                value=message_json.encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)  # Trigger delivery reports without blocking
        except Exception as e:
            raise AnsibleError('Error sending kafka message: %s' % to_native(e))

    def v2_playbook_on_start(self, playbook):
        """Playbook start event"""
        self.send_message('playbook_start', {
            'playbook': playbook._file_name
            # 'playbook_uuid': self._uuid
        })

    def v2_playbook_on_play_start(self, play):
        vm = play.get_variable_manager()
        # global extra_vars
        self.extra_vars = vm.extra_vars
        self.vm = vm.get_vars()
        # self._display.display(f"Extra Vars: {self.extra_vars}")
        # self._display.display(f"All Vars: {self.vm}")
        """Play start event"""
        #  self.send_message('play_start', {
        #      'play': play.name,
        #      'var1': self.extra_vars.get('var1')
            # 'test': self.test,
            # 'play_uuid': str(play._uuid),
            # 'playbook_uuid': self._uuid
        # })

    # def v2_playbook_on_task_start(self, task, is_conditional):
    #     """Task start event"""
    #     task_uuid = str(task._uuid)
    #     self.task_start_times[task_uuid] = datetime.datetime.now()
    #     # if task.name == 'Test':
    #     self.send_message('task_start', {
    #         'task': task.name,
    #         'task_uuid': task_uuid,
    #         'task_action': task.action,
    #         'is_conditional': is_conditional
    #         # 'playbook_uuid': self._uuid
    #     })

    def v2_runner_on_ok(self, result):
        """Task success event"""
        task_uuid = str(result._task._uuid)
        duration = None
        testvar = result._task.args.get('testvar', None)
        testvar = str(testvar)

        if task_uuid in self.task_start_times:
            start_time = self.task_start_times[task_uuid]
            duration = (datetime.datetime.now() - start_time).total_seconds()
        # if 'callback' in result._task.name:
        self.send_message('task_ok', {
            'task': result._task.name,
            'task_uuid': task_uuid,
            'task_action': result._task.action,
            'host': result._host.name,
            'changed': result._result.get('changed', False),
            'result': result._result,
            'duration': duration,
            'testvariable': testvar
          # 'playbook_uuid': self._uuid
        })

    # def v2_runner_on_failed(self, result, ignore_errors=False):
    #     """Task failure event"""
    #     task_uuid = str(result._task._uuid)
    #     duration = None
    #     if task_uuid in self.task_start_times:
    #         start_time = self.task_start_times[task_uuid]
    #         duration = (datetime.datetime.now() - start_time).total_seconds()
    #     self.send_message('task_failed', {
    #         'task': result._task.name,
    #         'task_uuid': task_uuid,
    #         'task_action': result._task.action,
    #         'host': result._host.name,
    #         'message': self._dump_results(result._result),
    #         'ignore_errors': ignore_errors,
    #         'duration': duration
    #         # 'playbook_uuid': self._uuid
    #     })

    # def v2_runner_on_skipped(self, result):
    #     """Task skipped event"""
    #     self.send_message('task_skipped', {
    #         'task': result._task.name,
    #         'task_uuid': str(result._task._uuid),
    #         'task_action': result._task.action,
    #         'host': result._host.name
    #         # 'playbook_uuid': self._uuid
    #     })

    def v2_runner_on_unreachable(self, result):
        """Task unreachable event"""
        host = str(result._host.name)

        # Check if the host has already been reported as unreachable
        if host in self.unreachable_seen:
            self._display.display(f"Skipping unreachable event for host {host} as it is already marked unreachable.")
            return  # Skip processing if the host is already marked as unreachable

        # Mark the host as unreachable for the entire playbook
        self.unreachable_seen.add(host)
        self._display.display(f"Host {host} marked as unreachable.")

        try:
            self.send_message('host_unreachable', {
                'task': result._task.name,
                'task_uuid': str(result._task._uuid),
                'task_action': result._task.action,
                'host': host,
                'result': result._result,
                'var1': self.extra_vars.get('var1')
            })
        except Exception as e:
            raise AnsibleError(f"Error sending unreachable message: {to_native(e)}")
        
    def v2_playbook_on_stats(self, stats):
        """Playbook completion event with stats"""
        hosts = sorted(stats.processed.keys())
        summary = {}
        for host in hosts:
            summary[host] = stats.summarize(host)
        
        self.send_message('playbook_stats', {
            # 'playbook_uuid': self._uuid,
            'status': summary
        })

        self.unreachable_seen.clear()  # Clear unreachable hosts for next playbook run
        self.producer.flush()  # Ensure all messages are sent before exiting
