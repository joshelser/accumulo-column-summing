accumulo-column-summing
=======================

Accumulo iterator that will perform a server-side summation over a set of column families.

This is an improvement, at the cost of code-complexity, over the SummingCombiner as it reduces the amount of data sent back to the client and also reduces the magnitude of the final client-side summation. While this is typically not very impactful in single machine setups, when real network latency is added to the underlying RPC invocations, this reduction in data can be a significant portion of the total execution time.

Summing a single column over 1 million rows will result in 1 million partial-sums being returned to the client by the SummingCombiner. Using the included SortedKeyValueIterator will result in approximately number of split points for the given table results sent to the client. In practice, the number of results returned with the iterator will be dependent on the table.scan.max.memory parameter set on the Accumulo table, but the number of results returned should always be orders of magnitude fewer results than the SummingCombiner.
