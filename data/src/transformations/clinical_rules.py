OBSERVATION_CODES = {
    "bmi": "39156-5",
    "systolic_bp": "8480-6",
    "diastolic_bp": "8462-4",
    "hba1c": "4548-4",
    "glucose": "2339-0",
    "creatinine": "38483-4",
    "egfr": "33914-3",
}

HIGH_RISK_MEDICATION_PATTERN = (
    "insulin|warfarin|heparin|fentanyl|morphine|oxycodone|hydrocodone|"
    "prednisone|amiodarone|clopidogrel|nitroglycerin|cisplatin"
)
ANTIMICROBIAL_PATTERN = "amoxicillin|azithromycin|ciprofloxacin|doxycycline|clindamycin"
MAJOR_PROCEDURE_PATTERN = "surgery|replacement|repair|insertion|removal|biopsy|dialysis|catheter|ventilation"
CHRONIC_CAREPLAN_PATTERN = "diabetes|respiratory|smoking|diet|exercise|therapy"
SEVERE_ALLERGY_PATTERN = "peanut|shellfish|bee|drug|latex"
PREVENTIVE_IMMUNIZATION_PATTERN = "influenza|pneumococcal|td|dtap|hepatitis|hep b"
