from datetime import UTC, datetime, timedelta
from ipaddress import ip_address
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "runtime" / "certs"
AUTHORITY = ROOT / "runtime" / "authority"
SUBJECT_TAIL = [
    x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Wazuh"),
    x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Wazuh"),
    x509.NameAttribute(NameOID.LOCALITY_NAME, "California"),
    x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
]


def write_private_key(path: Path, key) -> None:
    path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ))
    path.chmod(0o600)


def issue_leaf(ca_cert, ca_key, common_name: str, filename: str, usages, san=None):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name), *SUBJECT_TAIL])
    now = datetime.now(UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage(usages), critical=False)
    )
    if san:
        builder = builder.add_extension(x509.SubjectAlternativeName(san), critical=False)
    cert = builder.sign(ca_key, hashes.SHA256())
    (OUTPUT / f"{filename}.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    write_private_key(OUTPUT / f"{filename}-key.pem", key)


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    AUTHORITY.mkdir(parents=True, exist_ok=True)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "Diploma TASK-12 Test CA"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Diploma Local Tests"),
    ])
    now = datetime.now(UTC)
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=90))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(x509.KeyUsage(
            digital_signature=True, content_commitment=False, key_encipherment=False,
            data_encipherment=False, key_agreement=False, key_cert_sign=True,
            crl_sign=True, encipher_only=False, decipher_only=False,
        ), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    (OUTPUT / "root-ca.pem").write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    write_private_key(AUTHORITY / "root-ca-key.pem", ca_key)
    issue_leaf(
        ca_cert, ca_key, "node-1", "indexer",
        [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH],
        [x509.DNSName("localhost"), x509.IPAddress(ip_address("127.0.0.1"))],
    )
    for stale_file in OUTPUT.glob("admin*"):
        stale_file.unlink()
    for stale_file in OUTPUT.glob("root-ca-key*"):
        stale_file.unlink()
    print(f"Generated local test certificates in {OUTPUT}")


if __name__ == "__main__":
    main()
