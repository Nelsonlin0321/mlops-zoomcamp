from datetime import datetime
from dateutil.relativedelta import relativedelta
from prefect import flow
import score
import dotenv
dotenv.load_dotenv(".env")


@flow
def ride_duration_prediction_backfill():
    start_date = datetime(year=2021, month=1, day=1)
    end_date = datetime(year=2023, month=3, day=1)

    d = start_date

    while d <= end_date:
        score.ride_duration_prediction(
            taxi_type='green',
            run_id='cc36795ca2fd48e8a176194f450c0ade',
            run_date=d
        )

        d = d + relativedelta(months=1)


if __name__ == '__main__':
    ride_duration_prediction_backfill()