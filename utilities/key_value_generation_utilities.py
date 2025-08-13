import hashlib
import random
import string
import random
import uuid
import datetime
from faker import Faker

fake = Faker()

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


# --- JSON Generation Functions ---

def generate_user_data():
    """Generates a JSON object for user data."""
    return {
        "user_id": fake.uuid4(),
        "username": fake.user_name(),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "membership_level": random.choice(["bronze", "silver", "gold", "platinum"]),
        "last_login": fake.date_time_this_year().isoformat(),
        "preferences": {
            "theme": random.choice(["dark", "light"]),
            "notifications": {
                "email": random.choice([True, False]),
                "sms": random.choice([True, False])
            }
        }
    }

def generate_session_data():
    """Generates a JSON object representing a user's session data."""
    return {
        "session_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "login_time": fake.date_time_this_month().isoformat(),
        "last_active": fake.date_time_this_day().isoformat(),
        "cart_items": [
            {"product_id": fake.uuid4(), "quantity": random.randint(1, 5), "price": fake.random_int(10, 1000)}
            for _ in range(random.randint(0, 3))
        ],
        "csrf_token": fake.md5()
    }

def generate_product_data():
    """Generates a JSON object for a product in an e-commerce catalog."""
    
    # Generate a Decimal object for the price
    price_decimal = fake.pydecimal(left_digits=3, right_digits=2, positive=True)

    return {
        "product_id": fake.uuid4(),
        "name": fake.catch_phrase(),
        "description": fake.paragraph(nb_sentences=3),
        "price": str(price_decimal), # CONVERT DECIMAL TO STRING HERE
        "currency": "USD",
        "stock": random.randint(0, 500),
        "category": random.choice(["electronics", "clothing", "home_goods", "books", "toys"]),
        "features": {
            "dimensions": f"{random.uniform(5, 50):.1f}x{random.uniform(5, 50):.1f}x{random.uniform(5, 50):.1f} cm",
            "weight_kg": f"{random.uniform(0.1, 10):.2f}"
        },
        "reviews": [
            {
                "user_id": fake.uuid4(),
                "rating": random.randint(1, 5),
                "comment": fake.sentence(nb_words=10),
                "timestamp": fake.date_time_this_year().isoformat()
            } for _ in range(random.randint(0, 5))
        ]
    }

def generate_analytics_data():
    """Generates a JSON object for real-time analytics metrics."""
    return {
        "event_id": fake.uuid4(),
        "timestamp": fake.date_time_this_hour().isoformat(),
        "source_ip": fake.ipv4(),
        "user_id": random.choice([fake.uuid4() for _ in range(100)] + [None]),
        "event_type": random.choice(["page_view", "click", "add_to_cart", "login"]),
        "page_url": fake.uri(),
        "duration_ms": random.randint(50, 5000),
        "device_info": {
            "os": random.choice(["Windows", "macOS", "Linux", "iOS", "Android"]),
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"])
        }
    }