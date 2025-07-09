import hashlib
import random
import string

def make_random_key(key_length: int = 10) -> str:
    # Make a random key of len 'key_length' out of characters and digits
    random_key = "".join(
        random.choices(string.ascii_letters + string.digits, k=key_length)
    )
    return random_key


def make_deterministic_val(key: str, value_length: int) -> str:
    if value_length <= 0:
        return ""

    # Start with the SHA256 hash of the key as bytes
    current_hash_bytes = hashlib.sha256(key.encode("utf-8")).digest()

    # We will build the full value string by appending hex representations of hashes
    value_parts = []

    # Keep generating and appending hashes until we have enough content
    while len("".join(value_parts)) < value_length:
        # Convert the current hash bytes to a hexadecimal string and append
        value_parts.append(current_hash_bytes.hex())

        # Calculate the next hash by hashing the current hash's bytes
        current_hash_bytes = hashlib.sha256(current_hash_bytes).digest()

    # Join all parts and truncate to the desired length
    full_deterministic_string = "".join(value_parts)
    value = full_deterministic_string[:value_length]

    # Sanity check
    assert len(value) == value_length, (
        f"Value length mismatch: {len(value)} != {value_length}"
    )
    return str(value)  # Return as string (hex characters)
