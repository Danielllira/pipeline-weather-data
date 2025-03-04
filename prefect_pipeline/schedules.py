from datetime import timedelta
from prefect.client.schemas.schedules import IntervalSchedule

pipeline_schedule = IntervalSchedule(interval=timedelta(minutes=1))