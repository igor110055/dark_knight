from storm.services.profitability_service import record_arbitrages

if __name__ == '__main__':
    count = record_arbitrages()
    print(f'Recorded {count} arbitrages')
