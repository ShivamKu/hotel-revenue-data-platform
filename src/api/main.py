from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel, Field

from src.common.config_reader import read_config
from src.api.generate_reservation_api import (
    generate_reservation_data,
    generate_and_write_reservation_files
)

app = FastAPI(
    title="Hotel Revenue Data Platform API",
    description="API layer for synthetic hotel reservation data generation",
    version="1.0.0"
)


class GenerateReservationRequest(BaseModel):
    reservation_count: int = Field(..., gt=0, le=100000)
    business_date: str = Field(..., examples=["2026-05-10"])
    property_code: Optional[str] = Field(default=None, examples=["BLR001"])
    write_files: bool = Field(default=True)


@app.get("/health")
def health_check():
    return {
        "status": "success",
        "message": "Hotel Revenue Data Platform API is running"
    }


@app.post("/generate/reservations")
def generate_reservations(request: GenerateReservationRequest):
    """
    Generate reservation data.

    write_files = true:
        Generates CSV + JSON files using configured output paths.

    write_files = false:
        Only returns generated data in API response.
    """

    if request.write_files:
        config = read_config()

        csv_output_path = config["paths"]["raw_reservations_csv"]
        json_output_path = config["paths"]["raw_reservations_json"]

        result = generate_and_write_reservation_files(
            reservation_count=request.reservation_count,
            business_date=request.business_date,
            property_code=request.property_code,
            csv_output_path=csv_output_path,
            json_output_path=json_output_path
        )

        return result

    records, events = generate_reservation_data(
        reservation_count=request.reservation_count,
        business_date=request.business_date,
        property_code=request.property_code
    )

    return {
        "status": "success",
        "business_date": request.business_date,
        "property_code": request.property_code or "RANDOM",
        "reservation_count": request.reservation_count,
        "records": records,
        "events": events
    }