# How to run?
- Install prerequisite for maelstrom server [Link](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md)
- Download maelstrom server tar file [Link] (https://github.com/jepsen-io/maelstrom/releases/tag/v0.2.3)
- Extract tar file
- Modify ./Echo/test.sh and set correct path for maelstrom server
- Run ./Echo/test.sh

# Challenge #1: Echo
Simple echo node, reads echo request and sends echo_ok response.

# Challenge #2: UniqueIdGen
One solution can be to generate Guid.
But in my case, we already have node id as distinct number. So for each node we can create a unique id by appending a counter with the node id. For subsequent requests, we can increment the counter.

# Challenge #3a: Single node broadcast
Simple container node. For broadcast rpc, we store new messages in the node. For read rpc, we just return the stored messages.

# Challenge #3b: Multi node broadcast
Every node maintains its current message list. For every broadcast rpc, if this node is seeing this message for the first time, it will notify this message to every neighbour node. For notify rpc from peers, it will do the same as broadcast rpc.

# Challenge #3c: Fault tolerant multi node broadcast
We will develop this solution on top of Challenge #3b. Here nodes will face netwrok failure. So nodes need to reliably send the message to the neighbours. For this each node keeps track of the followin information:
- list of messages it has seen
- neighbour nodes for this node
- message counter to generate unique message id for outgoing gossip rpc
- list of pending ack gossip rpc messages
- for each neighbours it maintains a list of messages it has reliably sent to that neighbour along with the messages it has seen from that neighbour

We introduce `gossip` rpc in this solution. A gossip rpc contains list of message to send from a sender node to neighbour node. For each `gossip` received by the node, it will send `gossip_ok` response. So the sender will know whether the request reached to the neighbour or not. Now sender will resend `gossip` rpc if it desn't receive `gossip_ok` response within a timeout (100ms).

One optimization is that gossip will not include messages that are already reliable sent to the neighbour.

