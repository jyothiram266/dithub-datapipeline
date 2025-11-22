import tomlkit
import os

SECRETS_PATH = ".dlt/secrets.toml"

def setup_token():
    print(f"Configuring secrets at {SECRETS_PATH}")
    
    if not os.path.exists(SECRETS_PATH):
        print(f"Error: {SECRETS_PATH} not found. Please run 'dlt init github duckdb' first.")
        return

    token = input("Please paste your GitHub Personal Access Token: ").strip()
    
    if not token:
        print("Token cannot be empty.")
        return

    if token == "<configure me>":
        print("You entered the placeholder. Please enter a valid token.")
        return

    try:
        with open(SECRETS_PATH, "r") as f:
            secrets = tomlkit.load(f)
        
        if "sources" not in secrets:
            secrets["sources"] = {}
        if "github" not in secrets["sources"]:
            secrets["sources"]["github"] = {}
            
        secrets["sources"]["github"]["access_token"] = token
        
        with open(SECRETS_PATH, "w") as f:
            tomlkit.dump(secrets, f)
            
        print(f"Successfully updated {SECRETS_PATH} with your token.")
        print("You can now run 'python dlt_pipeline.py'")
        
    except Exception as e:
        print(f"Failed to update secrets file: {e}")

if __name__ == "__main__":
    setup_token()
