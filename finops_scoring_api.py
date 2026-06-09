import joblib
import numpy as np
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="FinOps Scoring API")


data = joblib.load('finops_model.joblib')
model = data['model']
encoders = data['label_encoders']

def safe_transform(encoder, value):
    try:
        return encoder.transform([value])[0]
    except ValueError:
        return -1

class BillingEvent(BaseModel):
    billing_record_id: str
    project_id: str
    service: str
    sku: str
    usage_unit: str
    usage_amount: float
    cost_usd: float
    is_weekend: int
    is_anomaly: int

@app.post("/score")
async def score_event(event: BillingEvent):
    try:
        project_id_enc = safe_transform(encoders['project_id'], event.project_id)
        service_enc = safe_transform(encoders['service'], event.service)
        sku_enc = safe_transform(encoders['sku'], event.sku)
        usage_unit_enc = safe_transform(encoders['usage_unit'], event.usage_unit)
        
        features = pd.DataFrame([{
            'usage_amount': event.usage_amount, 
            'cost_usd': event.cost_usd, 
            'is_weekend': event.is_weekend, 
            'project_id_enc': project_id_enc, 
            'service_enc': service_enc, 
            'sku_enc': sku_enc, 
            'usage_unit_enc': usage_unit_enc
        }])
        
        prediction = model.predict(features)
        score = float(model.decision_function(features)[0])
        
        return {
            "is_fraud": bool(prediction[0] == -1),
            "fraud_probability": score
        }
    except Exception as e:
        print(f"❌ Błąd przetwarzania: {e}")
        return {"error": str(e), "is_fraud": False, "fraud_probability": 0.0}
