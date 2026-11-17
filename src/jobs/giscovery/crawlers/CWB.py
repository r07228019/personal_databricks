"""中央氣象署 SFTP：下載指定資料夾內的所有檔案（已存在於 Volume 的略過）。

SFTP 帳密原本放 AWS Secrets Manager，這裡改讀 Databricks secret scope（由 runner 傳入 secrets(scope, key)）：
    scope：params["secret_scope"]（預設 giscovery）
    keys：cwb_sftp_hostname / cwb_sftp_username / cwb_sftp_password
注意：這台 SFTP 在中央氣象署內部，Databricks 連不到，所以這支在 Databricks 上無法實際執行，
排程固定暫停（見 scripts/gen_crawler_jobs.py 的 PAUSED_TASKS）。
"""

import os
import tempfile

import paramiko

# CWB SFTP 上有很多資料夾，只有這幾個放我們要的資料
TARGET_FOLDERS = {"1059", "1060", "1061", "958"}
DEFAULT_SECRET_SCOPE = "giscovery"


def get_sftp_credentials(secrets, scope: str) -> dict[str, str]:
    """回傳可直接 splat 進 paramiko SSHClient.connect() 的 kwargs。"""
    return {
        "hostname": secrets(scope, "cwb_sftp_hostname"),
        "username": secrets(scope, "cwb_sftp_username"),
        "password": secrets(scope, "cwb_sftp_password"),
    }


def download_folders(sftp_client, sink, tmpdir: str) -> list[str]:
    written = []
    for folder in sftp_client.listdir("/"):
        if folder not in TARGET_FOLDERS:
            continue
        for file_name in sftp_client.listdir(f"/{folder}"):
            # 把資料夾名攤平進檔名，與原本 S3 key 的 {folder}_{file} 規則一致
            target_name = f"{folder}_{file_name}"
            if sink.exists(target_name):
                print(f"* Skip existing {sink.path_for(target_name)}")
                continue
            local_path = os.path.join(tmpdir, file_name)
            sftp_client.get(f"/{folder}/{file_name}", local_path)
            with open(local_path, "rb") as f:
                written.append(sink.write(target_name, f.read()))
    return written


def main(sink, params, secrets):
    scope = params.get("secret_scope", DEFAULT_SECRET_SCOPE)

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh_client.connect(**get_sftp_credentials(secrets, scope))
        sftp_client = ssh_client.open_sftp()
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                written = download_folders(sftp_client, sink, tmpdir)
        finally:
            sftp_client.close()
    finally:
        ssh_client.close()

    print(f"* {len(written)} file(s) written")
    return written
