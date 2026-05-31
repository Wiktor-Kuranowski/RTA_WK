from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import numpy as np

app = FastAPI(title="FinOps Scoring API")

# Wczytanie paczki
data = joblib.load('finops_model.joblib')
model = data['model']
encoders = data['label_encoders'] # Upewnij się, że klucz to 'label_encoders'

# Funkcja zabezpieczająca przed błędem "unseen labels"
def safe_transform(encoder, value):
    try:
        return encoder.transform([value])[0]
    except ValueError:
        # Jeśli wartość jest nowa, zwracamy -1 (lub inną liczbę, która oznacza "inne")
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
        # Używamy safe_transform zamiast zwykłego transform
        project_id_enc = safe_transform(encoders['project_id'], event.project_id)
        service_enc = safe_transform(encoders['service'], event.service)
        sku_enc = safe_transform(encoders['sku'], event.sku)
        usage_unit_enc = safe_transform(encoders['usage_unit'], event.usage_unit)
        
        # Przygotowanie wektora cech
        features = np.array([[
            event.usage_amount, 
            event.cost_usd, 
            event.is_weekend, 
            project_id_enc, 
            service_enc, 
            sku_enc, 
            usage_unit_enc
        ]])
        
        prediction = model.predict(features)
        score = float(model.decision_function(features)[0])
        
        return {
            "is_fraud": bool(prediction[0] == -1),
            "fraud_probability": score
        }
    except Exception as e:
        # To zapobiegnie 500 Error i pokaże w logach, co się dzieje
        print(f"Błąd przetwarzania: {e}")
        return {"error": str(e), "is_fraud": False, "fraud_probability": 0.0}
