import dlt
import os
from dotenv import load_dotenv
from github import github_reactions, github_repo_events

# Load environment variables from .env file
load_dotenv()

def load_github_data():
    """
    Loads data from GitHub using dlt.
    Configured to load issues, pull requests, and comments.
    """
    # Initialize the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="duckdb",
        dataset_name="github_data",
        dev_mode=True # Enable dev mode for easier debugging/development
    )

    # Get repository details from environment variables
    repo_owner = os.getenv("REPO_OWNER", "dlt-hub")
    repo_name = os.getenv("REPO_NAME", "dlt")

    print(f"Loading data for {repo_owner}/{repo_name}...")

    # Define the source
    source = github_reactions(
        owner=repo_owner, 
        name=repo_name,
        items_per_page=100,
        max_items=500 # Limit for testing
    )

    # Run the pipeline
    info = pipeline.run(source)
    print(info)

if __name__ == "__main__":
    load_github_data()
