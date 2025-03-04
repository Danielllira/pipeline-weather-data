from prefect import flow
from prefect_pipeline.tasks import fetch_weather_data, load_weather_data


@flow(name="pipeline_weather_data")
def pipeline_weather_data():
    data = fetch_weather_data()
    load_weather_data(data)


if __name__ == "__main__":
    pipeline_weather_data()
