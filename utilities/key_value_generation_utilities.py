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
                "sms": random.choice([True, False]),
            },
        },
    }


def generate_session_data():
    """Generates a JSON object representing a user's session data."""
    return {
        "session_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "login_time": fake.date_time_this_month().isoformat(),
        # Use a provider that is known to work in your version
        "last_active": fake.date_time_this_month().isoformat(),
        "cart_items": [
            {
                "product_id": fake.uuid4(),
                "quantity": f"{random.randint(1, 5)}",
                "price": f"{fake.random_int(10, 1000)}",
            }
            for _ in range(random.randint(0, 3))
        ],
        "csrf_token": fake.md5(),
    }


def generate_product_data():
    """Generates a JSON object for a product in an e-commerce catalog."""

    # Generate a Decimal object for the price
    price_decimal = fake.pydecimal(left_digits=3, right_digits=2, positive=True)

    return {
        "product_id": fake.uuid4(),
        "name": fake.catch_phrase(),
        "description": fake.paragraph(nb_sentences=3),
        "price": str(price_decimal),
        "currency": "USD",
        "stock": random.randint(0, 500),
        "category": random.choice(
            ["electronics", "clothing", "home_goods", "books", "toys"]
        ),
        "features": {
            "dimensions": f"{random.uniform(5, 50):.1f}x{random.uniform(5, 50):.1f}x{random.uniform(5, 50):.1f} cm",
            "weight_kg": f"{random.uniform(0.1, 10):.2f}",
        },
        "reviews": [
            {
                "user_id": fake.uuid4(),
                "rating": random.randint(1, 5),
                "comment": fake.sentence(nb_words=10),
                "timestamp": fake.date_time_this_year().isoformat(),
            }
            for _ in range(random.randint(0, 5))
        ],
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
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
        },
    }


def generate_heavy_product_data() -> dict[str, any]:
    """
    Generates a heavy JSON object for a detailed product listing with
    a random number of variations and reviews.
    
    This object is designed to be complex, with nested arrays and objects, 
    to simulate a challenging real-world workload.

    Returns:
        Dict[str, Any]: A dictionary representing the heavy JSON product data.
    """
    # Generate random counts for variations and reviews
    num_variations = random.randint(1, 5) # A random number of variations
    num_reviews = random.randint(20, 100) # A random number of reviews

    # Generate the base product information
    product_data = {
        "product_id": fake.uuid4(),
        "sku": f"SKU-{fake.unique.random_int(10000, 99999)}",
        "name": fake.catch_phrase(),
        "description": fake.paragraph(nb_sentences=5),
        "category": random.sample(["electronics", "clothing", "home_goods", "books", "toys", "groceries", "sports"], k=random.randint(1, 3)),
        "manufacturer": fake.company(),
        "price_usd": str(fake.pydecimal(left_digits=3, right_digits=2, positive=True)),
        "stock_quantity": fake.random_int(0, 100),
        "images": [f"https://example.com/images/{fake.slug()}.jpg" for _ in range(random.randint(1, 4))],
        "specifications": {
            "material": random.choice(["plastic", "metal", "wood", "fabric", "glass"]),
            "care_instructions": fake.sentence(nb_words=10),
            "weight_kg": str(fake.pydecimal(left_digits=1, right_digits=2, positive=True))
        }
    }

    # Generate nested product variations
    variations = []
    for _ in range(num_variations):
        variations.append({
            "color": fake.color_name(),
            "size": random.choice(["XS", "S", "M", "L", "XL"]),
            "stock": fake.random_int(0, 50),
            "images": [f"https://example.com/images/{fake.slug()}.jpg"]
        })
    product_data["variations"] = variations
    
    # Generate a large list of reviews
    reviews = []
    for _ in range(num_reviews):
        reviews.append({
            "review_id": fake.uuid4(),
            "user_id": fake.uuid4(),
            "rating": fake.random_int(1, 5),
            "comment": fake.paragraph(nb_sentences=2),
            "timestamp": fake.date_time_this_year().isoformat()
        })
    product_data["reviews"] = reviews

    return product_data
