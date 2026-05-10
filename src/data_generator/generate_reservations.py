import json
import random
from pathlib import Path
from datetime import datetime, timedelta
from faker import Faker

from src.common.config_reader import read_config
from src.common.logger import get_logger

logger = get_logger(__name__)
fake = Faker()

PROPERTY_CODES = ["BLR001","BLR002","DEL001","MUM001","HYD001"]
ROOM_TYPES = ["STD","DLX","STE","KING","QUEEN"]
RATE_PLAN=["BAR","CORP","MEMBER","PKG","PREPAY"]
PRICE_PLAN_MAPPING={
    "BAR":["BAR_STD","BAR_DLX"],
    "CORP":["CORP_STD","CORP_DLX"],
    "MEMBER": ["MEM_STD", "MEM_DLX"],
    "PKG": ["PKG_STD", "PKG_DLX"],
    "PREPAY": ["PPR_STD", "PPR_DLX"]
}
STATUSES = ["BOOKED","CANCELLED","MODIFIED"]

def random_date(base_date: datetime, min_offset: int =1, max_offset: int =60) -> datetime:
    return base_date + timedelta(days=random.randint(min_offset, max_offset))

def build_reservation_record(index: int, business_date: str) -> dict:
    base_date=datetime.strptime(business_date, "%Y-%m-%d")

    propertyCode = random.choice(PROPERTY_CODES)
    ratePlanCode = random.choice(RATE_PLAN)
    pricePlanCode = random.choice(PRICE_PLAN_MAPPING[ratePlanCode])
    roomtype = random.choice(ROOM_TYPES)

    arrivalDate = random_date(base_date)
    departureDate = arrivalDate + timedelta(days=random.randint(1,5))

    status = random.choice(STATUSES)
    room_count= random.randint(1,4)

    nightly_rate = random.randint(3500,15000)
    stayNights =(departureDate -arrivalDate).days

    if status=="CANCELLED":
        total_revenue = 0
    else:
        total_revenue= nightly_rate*stayNights*room_count

    created_ts= base_date + timedelta(hours=random.randint(0,23),minutes=random.randint(0,59),
                                      seconds=random.randint(0,59))
    updated_ts = created_ts + timedelta(minutes=random.randint(0,180))

    confirmation_number = f"CNF{100000 + index}"
    reservation_id = f"RES{100000 + index}"
    guest_id = f"GUEST{100000 + index}"

    return {
        "reservation_id": reservation_id,
        "property_code": propertyCode,
        "confirmation_number": confirmation_number,
        "guest_id": guest_id,
        "arrival_date": arrivalDate.strftime("%Y-%m-%d"),
        "departure_date": departureDate.strftime("%Y-%m-%d"),
        "room_type": roomtype,
        "rate_plan_code": ratePlanCode,
        "price_plan_code": pricePlanCode,
        "booking_status": status,
        "room_count":room_count,
        "total_revenue": total_revenue,
        "currency": "INR",
        "created_ts": created_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_ts": updated_ts.strftime("%Y-%m-%d %H:%M:%S")

    }

def build_reservation_event(record: dict) -> dict:
    return {
        "eventId": f"EVT_{record['reservation_id']}",
        "eventType": f"Reservation{record['booking_status'].title()}",
        "eventTimestamp": record["updated_ts"].replace(" ", "T"),
        "propertyCode": record["property_code"],
        "confirmationNumber": {
            "value": record["confirmation_number"]
        },
        "reservationDetails": {
            "reservationId": record["reservation_id"],
            "guestId": record["guest_id"],
            "arrivalDate": record["arrival_date"],
            "departureDate": record["departure_date"],
            "roomType": record["room_type"],
            "ratePlanCode": record["rate_plan_code"],
            "pricePlanCode": record["price_plan_code"],
            "status": record["booking_status"],
            "roomCount": record["room_count"],
            "totalRevenue": record["total_revenue"],
            "currency": record["currency"]
        }
    }

def write_csv(records: list, output_path: str, business_date: str) -> None:
    import csv

    Path(output_path).mkdir(parents=True, exist_ok=True)
    file_path = Path(output_path) / f"reservations_{business_date.replace('-', '')}.csv"

    with file_path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

    logger.info(f"CSV reservation data generated at: {file_path}")


def write_json(events: list, output_path: str, business_date: str) -> None:
    Path(output_path).mkdir(parents=True, exist_ok=True)
    file_path = Path(output_path) / f"reservation_events_{business_date.replace('-', '')}.json"

    with file_path.open("w") as file:
        for event in events:
            file.write(json.dumps(event) + "\n")

    logger.info(f"JSON reservation event data generated at: {file_path}")

def main():
    config = read_config()

    reservation_count = config["data_generation"]["reservation_count"]
    business_date = config["data_generation"]["business_date"]

    csv_output_path = config["paths"]["raw_reservations_csv"]
    json_output_path = config["paths"]["raw_reservations_json"]

    logger.info(f"Generating {reservation_count} reservation records for business_date={business_date}")

    records = [
        build_reservation_record(index=i, business_date=business_date)
        for i in range(1, reservation_count + 1)
    ]

    events = [build_reservation_event(record) for record in records]

    write_csv(records, csv_output_path, business_date)
    write_json(events, json_output_path, business_date)

    logger.info("Reservation dummy data generation completed successfully")


if __name__ == "__main__":
    main()