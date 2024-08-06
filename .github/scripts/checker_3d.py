import re
from edn_format import Keyword, loads as edn_loads

# Function to preprocess the EDN data
def preprocess_edn(edn_data):
    # Replace `#jepsen.history.Op` with a dictionary-like structure
    processed_data = re.sub(r'#jepsen\.history\.Op\{', '{', edn_data)
    return processed_data

# Load and preprocess the EDN data
with open('store/latest/results.edn', 'r') as file:
    edn_data = file.read()
    processed_data = preprocess_edn(edn_data)
    data = edn_loads(processed_data)

# Navigate to :net:servers:msgs-per-op
msgs_per_op = data[Keyword("net")][Keyword('servers')][Keyword('msgs-per-op')]
print("Message per operation: ", msgs_per_op)
# Check if msgs-per-op is less than 30
stable_latencies = data[Keyword("workload")][Keyword("stable-latencies")]
print("Median Latency: ", stable_latencies[0.5])
print("Maximum Latency: ", stable_latencies[1])

assert msgs_per_op < 30, f"msgs-per-op is {msgs_per_op}, which is not less than 30."
assert stable_latencies[0.5] < 400, f"Median latency is {stable_latencies[0.5]}, which is not less than 400ms."
assert stable_latencies[1] < 600, f"Maximum latency is {stable_latencies[1]}, which is not less than 600ms."