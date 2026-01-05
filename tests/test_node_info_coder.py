# tests/test_coder.py
from typing import Optional
import random
import string
import pytest
from pyzlc.utils.node_info import encode_node_info, decode_node_info, NodeInfo
from pyzlc.utils.msg import create_hash_identifier

def generate_random_string(length: int) -> str:
    """Helper to generate random alphanumeric strings."""
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for _ in range(length))

def generate_random_ip() -> str:
    """Generates a random IPv4 address."""
    return f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"

def generate_random_node_info(num_topics: Optional[int] = None, num_services: Optional[int] = None) -> NodeInfo:
    """Generates a randomized NodeInfo structure for fuzz testing."""
    if num_topics is None:
        num_topics = random.randint(1, 10)
    if num_services is None:
        num_services = random.randint(1, 10)
    
    node_ip = generate_random_ip()
    
    return {
        "nodeID": create_hash_identifier(),
        "infoID": random.randint(0, 0xFFFFFFFF), # Max U32
        "name": f"node_{generate_random_string(8)}",
        "ip": node_ip,
        "topics": [
            {"name": f"topic_{i}", "ip": node_ip, "port": random.randint(1024, 65535)}
            for i in range(num_topics)
        ],
        "services": [
            {"name": f"svc_{i}", "ip": node_ip, "port": random.randint(1024, 65535)}
            for i in range(num_services)
        ]
    }

# --- Tests ---

def test_encode_decode_identity():
    """Verify that encoding and then decoding returns the original object."""
    for _ in range(100):  # Run 100 random trials
        original_data = generate_random_node_info()
        
        encoded = encode_node_info(original_data)
        decoded = decode_node_info(encoded)
        
        assert decoded == original_data


@pytest.mark.parametrize("list_size", [0, 1, 50])
def test_variable_list_sizes(list_size):
    """Test empty lists and larger-than-average lists."""
    data = generate_random_node_info()
    data["topics"] = [
        {"name": "t", "ip": "1.1.1.1", "port": 80} for _ in range(list_size)
    ]
    
    encoded = encode_node_info(data)
    decoded = decode_node_info(encoded)
    assert len(decoded["topics"]) == list_size
    
@pytest.mark.benchmark
def test_benchmark_encode(benchmark):
    """Benchmark ONLY the encoding process."""
    # 1. Setup phase: Generate data OUTSIDE the timed loop
    data = generate_random_node_info(10, 10)
    
    # 2. Timing phase: 
    # benchmark(function, *args) calls the function with args
    # and ONLY times the 'encode_node_info' execution.
    benchmark(encode_node_info, data)

@pytest.mark.benchmark
def test_benchmark_decode(benchmark):
    """Benchmark ONLY the decoding process."""
    # 1. Setup phase: Prepare the bytes beforehand
    data = generate_random_node_info(10, 10)
    encoded_bytes = encode_node_info(data)
    
    # 2. Timing phase:
    # Only the decoding is measured.
    benchmark(decode_node_info, encoded_bytes)