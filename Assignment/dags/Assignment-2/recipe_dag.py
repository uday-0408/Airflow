from airflow.decorators import dag, task
from datetime import datetime
import random

@dag(
    dag_id="recipe_pipeline",
    schedule="@daily",
    catchup=False,
    tags=["recipe"]
)
def recipe_pipeline():

    @task
    def get_ingredients(ds=None) -> dict:
        print(f"Getting ingredients for {ds}")
        return {
            "dish": "Pasta",
            "ingredients": ["pasta", "tomato sauce", "cheese", "garlic"]
        }

    @task
    def prepare_dish(recipe: dict) -> dict:
        dish = recipe["dish"]
        ingredients = recipe["ingredients"]

        print(f"Preparing {dish}...")
        for i, item in enumerate(ingredients, start=1):
            print(f"{i}. {item}")

        return {
            "dish": dish,
            "ingredient_count": len(ingredients),
            "status": "ready"
        }

    @task
    def serve_dish(result: dict) -> str:
        print(f"Serving {result['dish']} — {result['ingredient_count']} ingredients — Status: {result['status']}")
        return f"Served: {result['dish']}"

    @task.branch
    def check_rating(result: dict) -> str:
        rating = random.randint(1, 10)
        print(f"Customer rating: {rating}/10")

        if  rating >= 7:
            return "positive_review"
        else:
            return "needs_improvement"

    @task
    def positive_review() -> None:
        print("Great dish! Marking as customer favourite.")

    @task
    def needs_improvement() -> None:
        print("Recipe needs work. Adding to the improvement list.")

    @task(trigger_rule="none_failed")
    def end_of_service(served: str, ds=None) -> None:
        print(served)
        print(f"Kitchen closed for {ds}. All done!")

    ingredients = get_ingredients()
    prepared = prepare_dish(ingredients)
    served = serve_dish(prepared)

    review = check_rating(prepared)

    review >> [positive_review(), needs_improvement()]>>end_of_service(served)

dag_run = recipe_pipeline()