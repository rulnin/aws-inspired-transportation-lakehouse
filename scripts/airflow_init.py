"""Initialize Airflow metadata and local SimpleAuthManager credentials."""

from __future__ import annotations

import json
import os
import pwd
import subprocess
from pathlib import Path


def main() -> None:
    subprocess.run(["airflow", "db", "migrate"], check=True)

    config_dir = Path("/opt/airflow/config")
    config_dir.mkdir(parents=True, exist_ok=True)
    airflow_user = pwd.getpwnam("airflow")
    os.chown(config_dir, airflow_user.pw_uid, airflow_user.pw_gid)
    password_file = config_dir / "simple_auth_manager_passwords.json"
    temporary_file = password_file.with_suffix(".tmp")
    credentials = {os.environ["AIRFLOW_UI_USERNAME"]: os.environ["AIRFLOW_UI_PASSWORD"]}
    temporary_file.write_text(json.dumps(credentials), encoding="utf-8")
    temporary_file.chmod(0o600)
    os.chown(temporary_file, airflow_user.pw_uid, airflow_user.pw_gid)
    temporary_file.replace(password_file)
    username = os.environ["AIRFLOW_UI_USERNAME"]
    print(f"Created local Airflow SimpleAuthManager credentials for {username}")


if __name__ == "__main__":
    main()
