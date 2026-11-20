"""Identity of the checked-in ML lock, also available in the JVM-free images.

The manifest parity regression must change together with an intentional runtime
upgrade. Reading an optional local manifest must never make an unknown model
eligible for history collection.
"""

from .contracts import DetectionError


def expected_runtime():
    return {"manifest": "92208724157eaebae035a4d8b50bc0b83df243918f5cc0bac7c7e12a268ea169",
            "jre": "17.0.17+10", "jpype": "1.6.0", "aws": "4.4.0", "format": 1}


def validate_runtime(identity):
    if (not isinstance(identity, dict) or type(identity.get("format")) is not int
            or identity != expected_runtime()):
        raise DetectionError("checkpoint_runtime_mismatch")
