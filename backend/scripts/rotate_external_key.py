"""Atomically re-encrypt external connection passwords with a new Fernet key.

Run during maintenance with all API processes stopped. Set DATABASE_URL,
EXTERNAL_OLD_KEY and EXTERNAL_NEW_KEY in a private environment.
"""

import os

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import create_engine, text


def main() -> None:
    try:
        old = Fernet(os.environ["EXTERNAL_OLD_KEY"].encode("ascii"))
        new = Fernet(os.environ["EXTERNAL_NEW_KEY"].encode("ascii"))
        database_url = os.environ["DATABASE_URL"]
    except (KeyError, ValueError, UnicodeError) as error:
        raise SystemExit("Valid DATABASE_URL, EXTERNAL_OLD_KEY and EXTERNAL_NEW_KEY are required") from error
    engine = create_engine(database_url)
    try:
        with engine.begin() as connection:
            rows = connection.execute(text(
                "SELECT id, encrypted_password FROM app.external_connections FOR UPDATE"
            )).all()
            for identity, encrypted in rows:
                try:
                    clear = old.decrypt(encrypted.encode("ascii"))
                except (InvalidToken, UnicodeError) as error:
                    raise RuntimeError("Cannot decrypt all existing credentials; rotation rolled back") from error
                connection.execute(text(
                    "UPDATE app.external_connections SET encrypted_password = :value, "
                    "updated_at = CURRENT_TIMESTAMP WHERE id = :identity"
                ), {"identity": identity, "value": new.encrypt(clear).decode("ascii")})
        print(f"Rotated {len(rows)} external credentials")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
