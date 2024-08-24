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
