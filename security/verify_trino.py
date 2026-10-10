import os
from pathlib import Path

import requests
from trino.auth import BasicAuthentication
from trino.dbapi import connect
from trino.exceptions import TrinoUserError


def main() -> None:
    credentials = Path(os.environ.get("TRINO_SECURITY_CREDENTIALS", "/credentials"))
    host = os.environ.get("TRINO_SECURITY_HOST", "trino-secure")
    port = int(os.environ.get("TRINO_SECURITY_PORT", "8443"))
    endpoint = f"https://{host}:{port}"
    ca = str(credentials / "ca.crt")
    writer = "customer-profile-reference"
    password = (credentials / f"{writer}.password").read_text().strip()
    reader_password = (credentials / "showcase.password").read_text().strip()
    with requests.Session() as session:
        health = session.get(f"{endpoint}/v1/info", verify=ca, timeout=10)
        health.raise_for_status()
        if health.json().get("starting") is not False:
            raise RuntimeError("Trino has not finished starting")
        for authentication in (None, (writer, "invalid-password")):
            result = session.post(
                f"{endpoint}/v1/statement", data="SELECT 1",
                headers={"X-Trino-User": writer}, auth=authentication,
                verify=ca, timeout=10,
            )
            if result.status_code != 401:
                raise RuntimeError(f"Missing/invalid SQL credentials returned HTTP {result.status_code}, expected 401")
        try:
            requests.get(f"{endpoint}/v1/info", timeout=10)
        except requests.exceptions.SSLError:
            pass
        else:
            raise RuntimeError("Untrusted local certificate was not rejected")
    with connect(
        host=host, port=port, user=writer, http_scheme="https",
        auth=BasicAuthentication("showcase", reader_password), verify=ca,
        request_timeout=10, max_attempts=1,
    ) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT current_user")
            cursor.fetchall()
        except TrinoUserError as error:
            if error.error_name != "PERMISSION_DENIED" or "impersonate" not in error.message.lower():
                raise
        else:
            raise RuntimeError("Authenticated reader could impersonate another identity")
    with connect(
        host=host, port=port, user=writer, http_scheme="https",
        auth=BasicAuthentication(writer, password), verify=ca,
        request_timeout=10, max_attempts=1,
    ) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT current_user")
        if cursor.fetchall() != [[writer]]:
            raise RuntimeError("SQL identity differs from authenticated identity")
        cursor.execute("SELECT count(*) FROM iceberg.silver.customer_profile_360")
        if cursor.fetchall()[0][0] <= 0:
            raise RuntimeError("TLS query did not read the existing synthetic profile")
        try:
            cursor.execute("INSERT INTO iceberg.silver.customer_profile_360 SELECT * FROM iceberg.silver.customer_profile_360 WHERE false")
            cursor.fetchall()
        except Exception as error:
            if "Access Denied" not in str(error):
                raise
        else:
            raise RuntimeError("Read-only candidate allowed a write")
    print("PASS: trusted TLS SQL/read; missing/bad credentials, impersonation, untrusted CA and write rejected.")


if __name__ == "__main__":
    main()