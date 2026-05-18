from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
import requests
import os

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False,
    tags=['image_processing']
)
def download_wallpaper_dag():

    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def check_api_availability() -> PokeReturnValue:
        """Checks if the image service is responding."""
        r = requests.get("https://picsum.photos/1920/1080")
        
        if r.status_code == 200:
            # We pass the final URL (after redirects) to the next task
            return PokeReturnValue(is_done=True, xcom_value=r.url)
        
        return PokeReturnValue(is_done=False)

    @task
    def download_image(image_url: str):
        """Downloads the image and saves it to the include folder."""
        # Define the save path
        folder = '/usr/local/airflow/include/wallpapers'
        if not os.path.exists(folder):
            os.makedirs(folder)
            
        file_name = f"wallpaper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
        save_path = os.path.join(folder, file_name)

        # Stream the download to handle the file efficiently
        r = requests.get(image_url, stream=True)
        if r.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)
            print(f"Successfully downloaded: {save_path}")
            return save_path
        else:
            raise Exception(f"Failed to download image from {image_url}")

    # Set the dependency
    img_url = check_api_availability()
    download_image(img_url)

download_wallpaper_dag()