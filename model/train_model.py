"""
Train a fraud detection model using Random Forest
This creates a production-ready model for real-time scoring
WITH MORE REALISTIC DATA (includes edge cases and noise)
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from sklearn.preprocessing import LabelEncoder
import joblib
import os

def generate_training_data(n_samples=10000):
    """
    Generate synthetic payment transaction data for training
    NOW WITH REALISTIC EDGE CASES AND NOISE
    """
    np.random.seed(42)
    
    # Base legitimate transactions (70% of legitimate)
    n_legitimate_base = int(n_samples * 0.8 * 0.7)
    legitimate_base = pd.DataFrame({
        'amount': np.random.lognormal(mean=3.5, sigma=1.2, size=n_legitimate_base),
        'merchant_category': np.random.choice(
            ['grocery', 'restaurant', 'gas', 'pharmacy', 'retail'],
            size=n_legitimate_base,
            p=[0.3, 0.25, 0.2, 0.15, 0.1]
        ),
        'hour': np.random.randint(6, 23, size=n_legitimate_base),
        'day_of_week': np.random.choice(range(7), size=n_legitimate_base),
        'card_present': np.random.choice([0, 1], size=n_legitimate_base, p=[0.3, 0.7]),
        'distance_from_home': np.abs(np.random.normal(5, 10, n_legitimate_base)),
        'transaction_velocity': np.random.poisson(3, n_legitimate_base),
        'is_fraud': 0
    })
    
    # Edge case: Legitimate but LOOKS suspicious (30% of legitimate)
    # These are real purchases but have fraud-like patterns
    n_legitimate_edge = int(n_samples * 0.8 * 0.3)
    legitimate_edge = pd.DataFrame({
        'amount': np.random.lognormal(mean=5.5, sigma=1.0, size=n_legitimate_edge),  # Larger amounts
        'merchant_category': np.random.choice(
            ['electronics', 'retail', 'travel', 'online', 'jewelry'],  # More "risky" categories
            size=n_legitimate_edge,
            p=[0.35, 0.25, 0.2, 0.15, 0.05]
        ),
        'hour': np.random.randint(0, 24, size=n_legitimate_edge),  # Any time
        'day_of_week': np.random.choice(range(7), size=n_legitimate_edge),
        'card_present': np.random.choice([0, 1], size=n_legitimate_edge, p=[0.6, 0.4]),  # More online
        'distance_from_home': np.abs(np.random.normal(80, 50, n_legitimate_edge)),  # Further from home
        'transaction_velocity': np.random.poisson(7, n_legitimate_edge),  # Higher velocity
        'is_fraud': 0
    })
    
    # Base fraudulent transactions (70% of fraud)
    n_fraud_base = int(n_samples * 0.2 * 0.7)
    fraud_base = pd.DataFrame({
        'amount': np.random.lognormal(mean=6.5, sigma=1.5, size=n_fraud_base),
        'merchant_category': np.random.choice(
            ['electronics', 'jewelry', 'online', 'wire_transfer', 'travel'],
            size=n_fraud_base,
            p=[0.3, 0.2, 0.25, 0.15, 0.1]
        ),
        'hour': np.random.randint(0, 24, size=n_fraud_base),
        'day_of_week': np.random.choice(range(7), size=n_fraud_base),
        'card_present': np.random.choice([0, 1], size=n_fraud_base, p=[0.9, 0.1]),
        'distance_from_home': np.abs(np.random.normal(250, 150, n_fraud_base)),
        'transaction_velocity': np.random.poisson(14, n_fraud_base),
        'is_fraud': 1
    })
    
    # Edge case: Fraud but LOOKS legitimate (30% of fraud)
    # These are frauds that try to blend in
    n_fraud_edge = n_samples - n_legitimate_base - n_legitimate_edge - n_fraud_base
    fraud_edge = pd.DataFrame({
        'amount': np.random.lognormal(mean=4.0, sigma=1.0, size=n_fraud_edge),  # Smaller amounts
        'merchant_category': np.random.choice(
            ['grocery', 'restaurant', 'gas', 'pharmacy', 'retail'],  # "Safe" categories
            size=n_fraud_edge,
            p=[0.25, 0.25, 0.2, 0.15, 0.15]
        ),
        'hour': np.random.randint(8, 22, size=n_fraud_edge),  # Normal hours
        'day_of_week': np.random.choice(range(7), size=n_fraud_edge),
        'card_present': np.random.choice([0, 1], size=n_fraud_edge, p=[0.5, 0.5]),  # Mixed
        'distance_from_home': np.abs(np.random.normal(25, 20, n_fraud_edge)),  # Closer to home
        'transaction_velocity': np.random.poisson(5, n_fraud_edge),  # Lower velocity
        'is_fraud': 1
    })
    
    # Combine all and shuffle
    data = pd.concat([legitimate_base, legitimate_edge, fraud_base, fraud_edge], ignore_index=True)
    data = data.sample(frac=1, random_state=42).reset_index(drop=True)
    
    # Clip values to realistic ranges
    data['amount'] = data['amount'].clip(0.01, 10000)
    data['distance_from_home'] = data['distance_from_home'].clip(0, 1000)
    data['transaction_velocity'] = data['transaction_velocity'].clip(0, 50)
    
    return data

def prepare_features(df):
    """
    Prepare features for model training
    """
    # Encode categorical variables
    le = LabelEncoder()
    df['merchant_category_encoded'] = le.fit_transform(df['merchant_category'])
    
    # Select features for model
    feature_cols = [
        'amount',
        'merchant_category_encoded',
        'hour',
        'day_of_week',
        'card_present',
        'distance_from_home',
        'transaction_velocity'
    ]
    
    X = df[feature_cols]
    y = df['is_fraud']
    
    return X, y, le

def train_model():
    """
    Train the fraud detection model
    """
    print("🔄 Generating training data (with realistic edge cases)...")
    data = generate_training_data(n_samples=10000)
    
    print(f"📊 Dataset size: {len(data)} transactions")
    print(f"   - Legitimate: {(data['is_fraud']==0).sum()} ({(data['is_fraud']==0).sum()/len(data)*100:.1f}%)")
    print(f"   - Fraudulent: {(data['is_fraud']==1).sum()} ({(data['is_fraud']==1).sum()/len(data)*100:.1f}%)")
    
    # Show edge case distribution
    legit = data[data['is_fraud'] == 0]
    fraud = data[data['is_fraud'] == 1]
    print(f"\n📊 Data Realism:")
    print(f"   - Legitimate with high amounts (>$500): {(legit['amount'] > 500).sum()}")
    print(f"   - Fraud with low amounts (<$200): {(fraud['amount'] < 200).sum()}")
    print(f"   - This overlap makes the problem more realistic!")
    
    print("\n🔄 Preparing features...")
    X, y, label_encoder = prepare_features(data)
    
    print("\n🔄 Splitting data (80% train, 20% test)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    print("\n🔄 Training Random Forest model...")
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        min_samples_split=20,
        min_samples_leaf=10,
        random_state=42,
        n_jobs=-1,
        class_weight='balanced'
    )
    
    model.fit(X_train, y_train)
    
    print("\n✅ Model trained successfully!")
    
    # Evaluate model
    print("\n📊 Model Performance:")
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=['Legitimate', 'Fraud']))
    
    print("\nConfusion Matrix:")
    cm = confusion_matrix(y_test, y_pred)
    print(cm)
    print(f"\nBreakdown:")
    print(f"  True Negatives (Correct Legitimate): {cm[0][0]}")
    print(f"  False Positives (Legit flagged as Fraud): {cm[0][1]}")
    print(f"  False Negatives (Fraud missed): {cm[1][0]}")
    print(f"  True Positives (Correct Fraud): {cm[1][1]}")
    
    roc_auc = roc_auc_score(y_test, y_pred_proba)
    print(f"\nROC AUC Score: {roc_auc:.4f}")
    
    if roc_auc > 0.95:
        print("⚠️  Note: Very high score - in production, expect 0.85-0.92 with real data")
    
    # Feature importance
    print("\n📊 Feature Importance:")
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    print(feature_importance.to_string(index=False))
    
    # Save model and encoder
    print("\n💾 Saving model...")
    os.makedirs('model', exist_ok=True)
    joblib.dump(model, 'model/fraud_model.pkl')
    joblib.dump(label_encoder, 'model/label_encoder.pkl')
    
    print("\n✅ Model saved successfully!")
    print("   📁 Location: model/fraud_model.pkl")
    print("   📁 Encoder: model/label_encoder.pkl")
    
    return model, label_encoder

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 FRAUD DETECTION MODEL TRAINING")
    print("   (With Realistic Edge Cases)")
    print("=" * 60)
    
    model, encoder = train_model()
    
    print("\n" + "=" * 60)
    print("✅ Training Complete!")
    print("=" * 60)
    print("\nYou can now run the Kafka consumer to start scoring transactions.")