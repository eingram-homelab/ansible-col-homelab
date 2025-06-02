from confluent_kafka import Producer
import json
import time
import socket
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def delivery_report(err, msg):
    """
    Callback function for produced messages.
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def kafka_producer():
    """
    Create a Kafka producer using confluent-kafka and send a test message to a topic.
    """
    try:
        # Configuration with TLS enabled
        conf = {
            'bootstrap.servers': 'bootstrap.local.lan:443',  # Use your Kafka server address
            'client.id': socket.gethostname(),
            'acks': 'all',  # Wait for all replicas to acknowledge
            
            # TLS/SSL Configuration
            'security.protocol': 'SSL',
            'ssl.ca.location': '/etc/pki/ca-trust/source/anchors/ca.crt',  # Update with your CA cert path
            # Uncomment and update these if client authentication is required
            # 'ssl.certificate.location': 'tls.crt',
            # 'ssl.key.location': '/path/to/client-key.pem',
            # 'ssl.key.password': 'keypassword',  # If your key is password-protected
            
            # Optional: Set to false in dev environments with self-signed certs
            'ssl.endpoint.identification.algorithm': 'https'
        }

        # Create producer
        producer = Producer(conf)

        # Topic to send messages to
        topic = "test-topic1"
        
        # Example message
        message = {
            "id": 1,
            "timestamp": time.time(),
            "data": "Test Message",
            "source": "test_producer.py"
        }

        # Serialize the message to JSON
        message_json = json.dumps(message)
        
        # Produce message
        producer.produce(
            topic=topic,
            value=message_json.encode('utf-8'),
            callback=delivery_report
        )
        
        # Wait for any outstanding messages to be delivered and delivery reports received
        producer.flush()
        
        logger.info("Producer completed")
        
    except Exception as e:
        logger.error(f"Error producing Kafka message: {e}")

if __name__ == "__main__":
    kafka_producer()