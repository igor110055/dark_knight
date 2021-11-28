from storm.services.profitability_service import record_arbitrages

count = record_arbitrages()
print(f"Recorded {count} arbitrages")
