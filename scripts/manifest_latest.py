import os
import json

ART = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "_artifacts")


def latest_manifest():
    files = sorted(
        [os.path.join(ART, f) for f in os.listdir(ART) if f.endswith(".json")]
    )
    if not files:
        print("No manifests found")
        return
    path = files[-1]
    print("Latest manifest:", path)
    with open(path, encoding="utf-8") as fh:
        j = json.load(fh)
    print("Keys:", list(j.keys()))
    print("Env hash:", j.get("environment", {}).get("env_hash"))
    print("Fetches:", len(j.get("fetches", [])))


if __name__ == "__main__":
    latest_manifest()
