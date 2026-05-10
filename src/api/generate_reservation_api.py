import csv
import json
import random
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from faker import Faker

from src.common.config_reader import read_config
from src.common.logger import get_logger

logger = get_logger(__name__)
fake = Faker()

PROPERTY_CODES = [
    "BLR001", "BLR002", "DEL001", "MUM001", "HYD001", "CHN001",
    "DEL002", "MUM002", "KOL001", "KER001", "CHA001"
]

ROOM_TYPES = ["STD", "DLX", "STE", "KING", "QUEEN"]

RATE_PLAN = ["BAR", "CORP", "MEMBER", "PKG", "PREPAY"]

PRICE_PLAN_MAPPING = {
    "BAR": ["BAR_STD", "BAR_DLX"],
    "CORP": ["CORP_STD", "CORP_DLX"],
    "MEMBER": ["MEM_STD", "MEM_DLX"],
    "PKG": ["PKG_STD", "PKG_DLX"],
    "PREPAY": ["PPR_STD", "PPR_DLX"]
}

STATUSES = ["BOOKED", "CANCELLED", "MODIFIED"]


def random_date(base_date: datetime, min_offset: int = 1, max_offset: int = 60) -> datetime:
    return base_date + timedelta(days=random.randint(min_offset, max_offset))


def build_reservation_record(
        index: int,
        business_date: str,
        property_code: Optional[str] = None
) -> dict:
    """
    Builds one flat reservation record.
    This record is useful for CSV/raw tabular ingestion.
    """

    base_date = datetime.strptime(business_date, "%Y-%m-%d")

    selected_property_code = property_code or random.choice(PROPERTY_CODES)
    rate_plan_code = random.choice(RATE_PLAN)
    price_plan_code = random.choice(PRICE_PLAN_MAPPING[rate_plan_code])
    room_type = random.choice(ROOM_TYPES)

    arrival_date = random_date(base_date)
    departure_date = arrival_date + timedelta(days=random.randint(1, 5))

    status = random.choice(STATUSES)
    room_count = random.randint(1, 4)
    nightly_rate = random.randint(3500, 15000)

    stay_nights = (departure_date - arrival_date).days

    if status == "CANCELLED":
        total_revenue = 0
    else:
        total_revenue = nightly_rate * stay_nights * room_count

    created_ts = base_date + timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )

    updated_ts = created_ts + timedelta(minutes=random.randint(0, 180))

    confirmation_number = f"CNF{100000 + index}"
    reservation_id = f"RES{100000 + index}"
    guest_id = f"GUEST{100000 + index}"

    return {
        "reservation_id": reservation_id,
        "property_code": selected_property_code,
        "confirmation_number": confirmation_number,
        "guest_id": guest_id,
        "arrival_date": arrival_date.strftime("%Y-%m-%d"),
        "departure_date": departure_date.strftime("%Y-%m-%d"),
        "room_type": room_type,
        "rate_plan_code": rate_plan_code,
        "price_plan_code": price_plan_code,
        "booking_status": status,
        "room_count": room_count,
        "total_revenue": total_revenue,
        "currency": "INR",
        "created_ts": created_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_ts": updated_ts.strftime("%Y-%m-%d %H:%M:%S")
    }


def build_reservation_event(record: dict) -> dict:
    """
    Converts flat reservation record into nested event-style JSON.
    This is useful for simulating upstream event payloads.
    """

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


def generate_reservation_data(
        reservation_count: int,
        business_date: str,
        property_code: Optional[str] = None
) -> tuple[list[dict], list[dict]]:
    """
    Main reusable function.

    This can be called from:
    1. CLI script
    2. FastAPI endpoint
    3. Future Airflow/Dagster job
    4. Unit tests
    """

    if reservation_count <= 0:
        raise ValueError("reservation_count must be greater than 0")

    datetime.strptime(business_date, "%Y-%m-%d")

    records = [
        build_reservation_record(
            index=i,
            business_date=business_date,
            property_code=property_code
        )
        for i in range(1, reservation_count + 1)
    ]

    events = [build_reservation_event(record) for record in records]

    return records, events


def write_csv(records: list[dict], output_path: str, business_date: str) -> Path:
    """
    Writes flat reservation records to CSV.
    """

    if not records:
        raise ValueError("No records available to write into CSV")

    Path(output_path).mkdir(parents=True, exist_ok=True)

    file_path = Path(output_path) / f"reservations_{business_date.replace('-', '')}.csv"

    with file_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

    logger.info(f"CSV reservation data generated at: {file_path}")

    return file_path


def write_json(events: list[dict], output_path: str, business_date: str) -> Path:
    """
    Writes reservation event JSON in JSONL format.
    One JSON event per line.
    """

    if not events:
        raise ValueError("No events available to write into JSON")

    Path(output_path).mkdir(parents=True, exist_ok=True)

    file_path = Path(output_path) / f"reservation_events_{business_date.replace('-', '')}.json"

    with file_path.open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event) + "\n")

    logger.info(f"JSON reservation event data generated at: {file_path}")

    return file_path


def generate_and_write_reservation_files(
        reservation_count: int,
        business_date: str,
        csv_output_path: str,
        json_output_path: str,
        property_code: Optional[str] = None
) -> dict:
    """
    Generates reservation data and writes both CSV and JSON files.
    This is ideal for API endpoint when you want API call to create files.
    """

    logger.info(
        f"Generating {reservation_count} reservation records "
        f"for business_date={business_date}, property_code={property_code or 'RANDOM'}"
    )

    records, events = generate_reservation_data(
        reservation_count=reservation_count,
        business_date=business_date,
        property_code=property_code
    )

    csv_file_path = write_csv(
        records=records,
        output_path=csv_output_path,
        business_date=business_date
    )

    json_file_path = write_json(
        events=events,
        output_path=json_output_path,
        business_date=business_date
    )

    logger.info("Reservation dummy data generation completed successfully")

    return {
        "status": "success",
        "business_date": business_date,
        "property_code": property_code or "RANDOM",
        "reservation_count": reservation_count,
        "csv_file_path": str(csv_file_path),
        "json_file_path": str(json_file_path),
        "sample_record": records[0],
        "sample_event": events[0]
    }


def main():
    """
    CLI entry point.
    Existing behavior is preserved.
    Reads values from config and writes CSV + JSON.
    """

    config = read_config()

    reservation_count = config["data_generation"]["reservation_count"]
    business_date = config["data_generation"]["business_date"]

    csv_output_path = config["paths"]["raw_reservations_csv"]
    json_output_path = config["paths"]["raw_reservations_json"]

    generate_and_write_reservation_files(
        reservation_count=reservation_count,
        business_date=business_date,
        csv_output_path=csv_output_path,
        json_output_path=json_output_path
    )


if __name__ == "__main__":
    main()