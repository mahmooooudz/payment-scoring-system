"""
Kafka Consumer - Real-Time Payment Scoring Pipeline
This is the main component that:
1. Reads transactions from Kafka
2. Scores them using the ML model
3. Writes results to MySQL
"""

import json
import joblib
import mysql.connector
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import time
from datetime import datetime

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'payment-transactions'
KAFKA_GROUP_ID = 'payment-scoring-group'

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'rootpassword',
    'database': 'payment_scoring'
}

# Risk thresholds
RISK_THRESHOLDS = {
    'HIGH': 0.7,
    'MEDIUM': 0.3
}

class PaymentScoringPipeline:
    """
    Real-time payment scoring pipeline
    """
    
    def __init__(self):
        """Initialize the pipeline"""
        self.model = None
        self.label_encoder = None
        self.consumer = None
        self.db_connection = None
        self.processed_count = 0
        self.start_time = time.time()
        
    def load_model(self):
        """Load the trained fraud detection model"""
        try:
            print("🔄 Loading fraud detection model...")
            self.model = joblib.load('model/fraud_model.pkl')
            self.label_encoder = joblib.load('model/label_encoder.pkl')
            print("✅ Model loaded successfully!")
        except FileNotFoundError:
            print("❌ Model not found! Please run 'python model/train_model.py' first.")
            raise
        except Exception as e:
            print(f"❌ Error loading model: {e}")
            raise
    
    def connect_kafka(self):
        """Connect to Kafka consumer"""
        try:
            print(f"🔄 Connecting to Kafka at {KAFKA_BROKER}...")
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"✅ Connected to Kafka topic: {KAFKA_TOPIC}")
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            raise
    
    def connect_mysql(self):
        """Connect to MySQL database"""
        try:
            print("🔄 Connecting to MySQL database...")
            self.db_connection = mysql.connector.connect(**MYSQL_CONFIG)
            print("✅ Connected to MySQL!")
        except Exception as e:
            print(f"❌ Failed to connect to MySQL: {e}")
            raise
    
    def prepare_features(self, transaction):
        """
        Prepare transaction features for model prediction
        """
        # Encode merchant category
        try:
            merchant_encoded = self.label_encoder.transform([transaction['merchant_category']])[0]
        except ValueError:
            # Unknown category - use a default encoding
            merchant_encoded = 0
        
        # Create feature vector
        features = pd.DataFrame({
            'amount': [transaction['amount']],
            'merchant_category_encoded': [merchant_encoded],
            'hour': [transaction['hour']],
            'day_of_week': [transaction['day_of_week']],
            'card_present': [transaction['card_present']],
            'distance_from_home': [transaction['distance_from_home']],
            'transaction_velocity': [transaction['transaction_velocity']]
        })
        
        return features
    
    def score_transaction(self, transaction):
        """
        Score a transaction for fraud risk
        Returns: (fraud_score, risk_level)
        """
        try:
            # Prepare features
            features = self.prepare_features(transaction)
            
            # Get fraud probability
            fraud_score = self.model.predict_proba(features)[0][1]
            
            # Determine risk level
            if fraud_score >= RISK_THRESHOLDS['HIGH']:
                risk_level = 'HIGH'
            elif fraud_score >= RISK_THRESHOLDS['MEDIUM']:
                risk_level = 'MEDIUM'
            else:
                risk_level = 'LOW'
            
            return fraud_score, risk_level
        
        except Exception as e:
            print(f"❌ Error scoring transaction: {e}")
            return 0.0, 'ERROR'
    
    def save_to_mysql(self, transaction, fraud_score, risk_level):
        """
        Save scored transaction to MySQL database
        """
        try:
            cursor = self.db_connection.cursor()
            
            query = """
            INSERT INTO scored_transactions 
            (transaction_id, amount, merchant_category, hour, day_of_week, 
             card_present, distance_from_home, transaction_velocity, 
             fraud_score, risk_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                fraud_score = VALUES(fraud_score),
                risk_level = VALUES(risk_level),
                processed_at = CURRENT_TIMESTAMP
            """
            
            values = (
                transaction['transaction_id'],
                transaction['amount'],
                transaction['merchant_category'],
                transaction['hour'],
                transaction['day_of_week'],
                transaction['card_present'],
                transaction['distance_from_home'],
                transaction['transaction_velocity'],
                float(fraud_score),
                risk_level
            )
            
            cursor.execute(query, values)
            self.db_connection.commit()
            cursor.close()
            
            return True
        
        except Exception as e:
            print(f"❌ Error saving to MySQL: {e}")
            self.db_connection.rollback()
            return False
    
    def process_transaction(self, transaction):
        """
        Main processing pipeline: score and save transaction
        """
        start_time = time.time()
        
        # Score transaction
        fraud_score, risk_level = self.score_transaction(transaction)
        
        # Save to database
        success = self.save_to_mysql(transaction, fraud_score, risk_level)
        
        # Calculate processing time
        processing_time = (time.time() - start_time) * 1000  # Convert to ms
        
        # Update counter
        self.processed_count += 1
        
        # Log result
        risk_emoji = {
            'HIGH': '🚨',
            'MEDIUM': '⚠️',
            'LOW': '✅',
            'ERROR': '❌'
        }
        
        status = "✅" if success else "❌"
        print(f"{status} [{self.processed_count:4d}] {risk_emoji.get(risk_level, '❓')} "
              f"ID={transaction['transaction_id']:15s} | "
              f"Amount=${transaction['amount']:7.2f} | "
              f"Score={fraud_score:.4f} | "
              f"Risk={risk_level:6s} | "
              f"Time={processing_time:5.1f}ms")
        
        return success
    
    def run(self):
        """
        Main consumer loop
        """
        try:
            # Initialize components
            self.load_model()
            self.connect_kafka()
            self.connect_mysql()
            
            print("\n" + "=" * 80)
            print("🚀 PAYMENT SCORING PIPELINE RUNNING")
            print("=" * 80)
            print(f"📊 Listening for transactions on topic: {KAFKA_TOPIC}")
            print(f"💾 Saving results to MySQL database: {MYSQL_CONFIG['database']}")
            print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80)
            print("\nWaiting for transactions...\n")
            
            # Consume and process messages
            for message in self.consumer:
                transaction = message.value
                self.process_transaction(transaction)
                
                # Print stats every 50 transactions
                if self.processed_count % 50 == 0:
                    elapsed = time.time() - self.start_time
                    rate = self.processed_count / elapsed
                    print(f"\n📊 Stats: {self.processed_count} transactions processed | "
                          f"Rate: {rate:.1f} trans/sec | "
                          f"Runtime: {elapsed:.1f}s\n")
        
        except KeyboardInterrupt:
            print("\n\n⏹️  Shutting down gracefully...")
        
        except Exception as e:
            print(f"\n❌ Fatal error: {e}")
        
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up connections"""
        print("\n🔄 Cleaning up...")
        
        if self.consumer:
            self.consumer.close()
            print("✅ Kafka consumer closed")
        
        if self.db_connection:
            self.db_connection.close()
            print("✅ MySQL connection closed")
        
        # Print final statistics
        elapsed = time.time() - self.start_time
        print("\n" + "=" * 80)
        print("📊 FINAL STATISTICS")
        print("=" * 80)
        print(f"Total transactions processed: {self.processed_count}")
        print(f"Total runtime: {elapsed:.2f} seconds")
        if self.processed_count > 0:
            print(f"Average processing rate: {self.processed_count/elapsed:.2f} transactions/second")
        print("=" * 80)

if __name__ == "__main__":
    print("=" * 80)
    print("🎯 REAL-TIME PAYMENT SCORING SYSTEM")
    print("=" * 80)
    print("\n⚙️  Initializing pipeline...\n")
    
    pipeline = PaymentScoringPipeline()
    pipeline.run()