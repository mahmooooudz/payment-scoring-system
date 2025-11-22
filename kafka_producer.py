"""
Kafka Producer - Simulates payment transactions
This sends test transactions to Kafka for the consumer to process
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'payment-transactions'

def create_transaction(transaction_id):
    """
    Generate a realistic payment transaction
    Mix of legitimate and suspicious transactions for testing
    """
    # 70% legitimate, 30% suspicious for testing
    is_suspicious = random.random() < 0.3
    
    if is_suspicious:
        # Suspicious transaction patterns
        late_night_hours = list(range(0, 6)) + list(range(22, 24))  # Fixed: convert to lists first
        return {
            'transaction_id': f'TXN{transaction_id:06d}',
            'amount': round(random.uniform(500, 5000), 2),
            'merchant_category': random.choice(['electronics', 'jewelry', 'online', 'wire_transfer']),
            'hour': random.choice(late_night_hours),  # Late night
            'day_of_week': random.randint(0, 6),
            'card_present': 0,  # Online transaction
            'distance_from_home': round(random.uniform(100, 800), 2),
            'transaction_velocity': random.randint(8, 20),  # Many recent transactions
            'timestamp': datetime.now().isoformat()
        }
    else:
        # Legitimate transaction patterns
        return {
            'transaction_id': f'TXN{transaction_id:06d}',
            'amount': round(random.uniform(5, 200), 2),
            'merchant_category': random.choice(['grocery', 'restaurant', 'gas', 'pharmacy', 'retail']),
            'hour': random.randint(8, 22),  # Normal hours
            'day_of_week': random.randint(0, 6),
            'card_present': random.choice([0, 1]),
            'distance_from_home': round(random.uniform(0, 30), 2),
            'transaction_velocity': random.randint(1, 5),  # Normal velocity
            'timestamp': datetime.now().isoformat()
        }

def send_transactions(num_transactions=100, delay=0.5):
    """
    Send test transactions to Kafka
    """
    try:
        # Create Kafka producer
        print(f"🔄 Connecting to Kafka at {KAFKA_BROKER}...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        
        print(f"✅ Connected to Kafka!")
        print(f"📤 Sending {num_transactions} transactions to topic '{KAFKA_TOPIC}'...")
        print("=" * 70)
        
        for i in range(1, num_transactions + 1):
            transaction = create_transaction(i)
            
            # Send to Kafka
            future = producer.send(KAFKA_TOPIC, value=transaction)
            
            try:
                # Wait for send confirmation
                record_metadata = future.get(timeout=10)
                
                # Print status
                risk_indicator = "⚠️ " if transaction['amount'] > 500 else "✅"
                print(f"{risk_indicator} Sent [{i:3d}/{num_transactions}]: "
                      f"ID={transaction['transaction_id']}, "
                      f"Amount=${transaction['amount']:7.2f}, "
                      f"Category={transaction['merchant_category']:15s}, "
                      f"Partition={record_metadata.partition}")
                
            except KafkaError as e:
                print(f"❌ Failed to send transaction {i}: {e}")
            
            # Add delay between transactions to simulate real-time flow
            time.sleep(delay)
        
        # Ensure all messages are sent
        producer.flush()
        print("=" * 70)
        print(f"✅ Successfully sent {num_transactions} transactions!")
        print(f"📊 Check MySQL database for scored results")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Troubleshooting:")
        print("   1. Make sure Docker containers are running: docker ps")
        print("   2. Check Kafka logs: docker logs kafka")
        print("   3. Verify Kafka is ready (can take 30-60 seconds after startup)")
    
    finally:
        if 'producer' in locals():
            producer.close()

def send_specific_test_cases():
    """
    Send specific test cases to verify model behavior
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        print("\n🧪 Sending specific test cases...")
        print("=" * 70)
        
        test_cases = [
            {
                'name': 'Legitimate - Small grocery purchase',
                'transaction': {
                    'transaction_id': 'TEST_LEGIT_001',
                    'amount': 45.50,
                    'merchant_category': 'grocery',
                    'hour': 14,
                    'day_of_week': 3,
                    'card_present': 1,
                    'distance_from_home': 2.5,
                    'transaction_velocity': 2,
                    'timestamp': datetime.now().isoformat()
                },
                'expected': 'LOW RISK'
            },
            {
                'name': 'Suspicious - Large online electronics purchase at 3 AM',
                'transaction': {
                    'transaction_id': 'TEST_FRAUD_001',
                    'amount': 2999.99,
                    'merchant_category': 'electronics',
                    'hour': 3,
                    'day_of_week': 6,
                    'card_present': 0,
                    'distance_from_home': 450.0,
                    'transaction_velocity': 15,
                    'timestamp': datetime.now().isoformat()
                },
                'expected': 'HIGH RISK'
            },
            {
                'name': 'Medium Risk - Unusual but possible',
                'transaction': {
                    'transaction_id': 'TEST_MEDIUM_001',
                    'amount': 350.00,
                    'merchant_category': 'retail',
                    'hour': 20,
                    'day_of_week': 5,
                    'card_present': 1,
                    'distance_from_home': 50.0,
                    'transaction_velocity': 7,
                    'timestamp': datetime.now().isoformat()
                },
                'expected': 'MEDIUM RISK'
            }
        ]
        
        for test in test_cases:
            producer.send(KAFKA_TOPIC, value=test['transaction'])
            print(f"📤 {test['name']}")
            print(f"   Expected: {test['expected']}")
            time.sleep(1)
        
        producer.flush()
        print("=" * 70)
        print("✅ Test cases sent!")
        
    except Exception as e:
        print(f"❌ Error sending test cases: {e}")
    finally:
        if 'producer' in locals():
            producer.close()

if __name__ == "__main__":
    print("=" * 70)
    print("🚀 KAFKA PRODUCER - Payment Transaction Simulator")
    print("=" * 70)
    
    print("\nOptions:")
    print("  1. Send 100 random transactions (default)")
    print("  2. Send specific test cases")
    print("  3. Send custom number of transactions")
    
    choice = input("\nEnter choice (1-3) or press Enter for default: ").strip()
    
    if choice == '2':
        send_specific_test_cases()
    elif choice == '3':
        try:
            num = int(input("How many transactions? "))
            delay = float(input("Delay between transactions (seconds)? "))
            send_transactions(num, delay)
        except ValueError:
            print("❌ Invalid input. Using defaults.")
            send_transactions()
    else:
        send_transactions()
    
    print("\n✅ Done! Check the consumer terminal for processing logs.")