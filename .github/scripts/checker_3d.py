import re
from edn_format import Keyword, loads as edn_loads
import argparse

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


parser = argparse.ArgumentParser(description='Process some EDN data.')
parser.add_argument('--msgs-per-op', type=int, required=True, help='Number of messages per operation')
parser.add_argument('--median-latency', type=int, required=True, help='Median latency in ms')
parser.add_argument('--max-latency', type=int, required=True, help='Maximum latency in ms')
args = parser.parse_args()
msgs_per_op_threshold = args.msgs_per_op
median_latency_threshold = args.median_latency
max_latency_threshold = args.max_latency

assert msgs_per_op < median_latency_threshold, f"msgs-per-op is {msgs_per_op}, which is not less than {msgs_per_op_threshold}."
assert stable_latencies[0.5] < median_latency_threshold, f"Median latency is {stable_latencies[0.5]}, which is not less than {median_latency_threshold}ms."
assert stable_latencies[1] < max_latency_threshold, f"Maximum latency is {stable_latencies[1]}, which is not less than {max_latency_threshold}ms."