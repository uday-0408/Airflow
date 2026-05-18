from airflow.decorators import dag, task
from datetime import datetime
import requests
from airflow.sensors.base import PokeReturnValue

@dag(start_date=datetime(2023, 1, 1), schedule="* * * * *", catchup=False)
def anime_sensor_dag():

    @task.sensor(poke_interval=30, timeout=3600, mode="poke")
    def check_for_anime() -> PokeReturnValue:
        # Hits the random anime endpoint
        r = requests.get("https://api.jikan.moe/v4/random/anime")
        
        if r.status_code == 200:
            data = r.json()
            anime_title = data['data']['title']
            print(f"Found a random anime: {anime_title}")
            
            condition_met = True
            operator_return_value = data
        else:
            condition_met = False
            operator_return_value = None

        return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)

    check_for_anime()

anime_sensor_dag()