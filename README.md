# Real-Time Payment Fraud Detection System

A real-time ML system that reads payment transactions from Kafka, scores them for fraud, and stores results in MySQL.

---

## Project Overview

This system demonstrates production ML deployment with:
- **Real-time streaming** via Apache Kafka
- **92% accuracy** fraud detection model
- **Sub-50ms latency** per transaction
- **Complete pipeline** from ingestion to storage

**Architecture:**
```
Kafka Producer → Kafka Broker → Consumer (ML Model) → MySQL Database
```

---

## Model Performance

**Training Results:**
- Accuracy: 92%
- ROC AUC: 0.9593
- Precision (Fraud): 78%
- Recall (Fraud): 84%
- Inference Time: 30-50ms

**Test Results (100 Transactions):**
| Risk Level | Count | Avg Score | Avg Amount |
|-----------|-------|-----------|------------|
| HIGH | 34 | 0.9518 | $2,094 |
| MEDIUM | 27 | 0.5019 | $108 |
| LOW | 39 | 0.1718 | $94 |

**Key Features:**
- `transaction_velocity` (35.6%) - Most important
- `distance_from_home` (35.4%)
- `amount` (12.2%)
- `card_present`, `merchant_category`, `hour`, `day_of_week`

---

## Quick Start

**Prerequisites:**
- Docker Desktop
- Python 3.8+

**Setup:**

1. **Start infrastructure:**
```bash
docker-compose up -d
```

2. **Setup MySQL:**
```bash
docker cp mysql_setup.sql mysql-container:/tmp/
docker exec -i mysql-container mysql -uroot -prootpassword < mysql_setup.sql
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Train model:**
```bash
python model/train_model.py
```

5. **Run consumer (Terminal 1):**
```bash
python kafka_consumer.py
```

6. **Send test transactions (Terminal 2):**
```bash
python kafka_producer.py
```

**Verify results:**
```bash
docker exec -it mysql-container mysql -uroot -prootpassword -e "
USE payment_scoring;
SELECT risk_level, COUNT(*) as count, AVG(fraud_score) as avg_score
FROM scored_transactions GROUP BY risk_level;"
```

---

## Project Structure

```
payment-scoring-system/
├── model/
│   ├── train_model.py          # Model training
│   ├── fraud_model.pkl         # Trained model
│   └── label_encoder.pkl       # Encoder
├── kafka_producer.py           # Transaction simulator
├── kafka_consumer.py           # Main scoring pipeline
├── mysql_setup.sql            # Database schema
├── docker-compose.yml         # Infrastructure
└── requirements.txt           # Dependencies
```

---

## Technical Details

**Why Random Forest?**
- Fast inference (<50ms)
- Handles tabular data well
- No feature scaling needed
- Interpretable feature importance

**Risk Thresholds:**
- HIGH: score ≥ 0.7
- MEDIUM: 0.3 ≤ score < 0.7
- LOW: score < 0.3

**Model Training Approach:**
- 10,000 synthetic transactions (80% legit, 20% fraud)
- Added 30% edge cases for realism (legit that looks suspicious, fraud that looks legit)
- This dropped accuracy from 99.98% to 92% - more realistic for production

---

## Key Implementation Decisions

1. **Synthetic Data with Edge Cases:**
   - Legitimate transactions with high amounts/unusual patterns
   - Fraudulent transactions disguised as normal
   - Result: 92% accuracy instead of unrealistic 99.98%

2. **Three-tier Risk System:**
   - HIGH (34%): Immediate review/block
   - MEDIUM (27%): Additional verification
   - LOW (39%): Auto-approve
   - Makes the system actionable for business

3. **Feature Engineering:**
   - Transaction velocity and distance account for 71% of decisions
   - Matches real-world fraud detection patterns

---

## Troubleshooting

**Kafka connection refused:**
```bash
# Wait 60 seconds for Kafka to start
docker logs kafka | grep "started"
```

**Model not found:**
```bash
python model/train_model.py
```

**MySQL connection error:**
```bash
docker ps | grep mysql
docker logs mysql-container
```

---

## System Performance

- **Throughput:** 25 trans/sec (single consumer)
- **Latency:** 40ms average
- **Scalability:** Can handle 500+ trans/sec with consumer group
- **Success Rate:** 100% (no message loss)

---

## Technologies Used

- Python 3.10
- Apache Kafka 7.4.0
- MySQL 8.0
- Docker & Docker Compose
- scikit-learn 1.3.0
- Random Forest Classifier

---

**Note:** This is a demonstration project using synthetic data. For production, additional security, monitoring, and compliance features would be required.
