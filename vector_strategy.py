# Simple key-value store with multiple replicas storing the same data
class DataStore:
    def __init__(self, kv_dict, name):
        self._kv_dict = kv_dict
        self._name = name
        self._replica_list = []
        self._vector_clock = {name: 0}

    @property
    def kv_dict(self):
        return self._kv_dict

    @property
    def name(self):
        return self._name
        
    @property
    def vector_clock(self):
        return self._vector_clock

    # Add replicas based on addition of new nodes
    # This will also update the vector clock dict
    def add_replicas(self, replica_list):
        self._replica_list.extend(replica_list)
        for replica in replica_list:
            self._vector_clock[replica.name] = 0

    def reset_vector_clock(self):
        for replica in self._replica_list:
            self._vector_clock[replica.name] = 0
        self._vector_clock[self.name] = 0

    def get_data(self, key):
        return self.kv_dict.get(key)

    def set_data(self, key, value):
        self.kv_dict[key] = value
        self._vector_clock[self.name] += 1

    # Simple add operation on a particular key's value
    def add(self, key, value):
        if self.get_data(key):
            self.set_data(key, self.get_data(key) + value)
        else:
            self.set_data(key, value)
    
    # Simple multiply operation on a particular key's value
    def multiply(self, key, value):
        if self.get_data(key):
            self.set_data(key, self.get_data(key) * value)
        else:
            self.set_data(key, 0)
    
    # Event receiver from another replica to update value based on a change done on the caller
    # Normally, this would be called internally from the update on the other store
    def receive_event(self, key, value, caller_vector_clock):
        # Compare clocks
        self_clock_ahead = True
        caller_clock_ahead = True
        for node in self.vector_clock:
            if self.vector_clock[node] < caller_vector_clock[node]:
                self_clock_ahead = False
            if caller_vector_clock[node] < self.vector_clock[node]:
                caller_clock_ahead = False
        
        if not self_clock_ahead and not caller_clock_ahead:
            print(f'No clock is strictly coming later than others - CONCURRENT WRITES DETECTED for key {key} and values {value} and {self.get_data(key)} in receive_event of self.name')
        
        self.set_data(key, value)
        for replica in self._replica_list:
            self.vector_clock[replica.name] = max(self.vector_clock[replica.name], caller_vector_clock[replica.name])


# Client processing

# Setting up the stores
node_a = DataStore({}, 'node_a')
node_a.set_data('account_balance', 1000)
node_b = DataStore({}, 'node_b')
node_b.set_data('account_balance', 1000)
node_a.add_replicas([node_b])
node_b.add_replicas([node_a])
node_a.reset_vector_clock()
node_b.reset_vector_clock()

# Run without overlap
print(f'Initial: \n node_a vector clock: {node_a.vector_clock} \n node_b vector clock: {node_b.vector_clock}')
node_a.add('account_balance', 100)
print(f'After add in node_a: \n node_a vector clock: {node_a.vector_clock} \n node_b vector clock: {node_b.vector_clock}')
node_b.receive_event('account_balance', node_a.get_data('account_balance'), node_a.vector_clock)
print(f'After update in node_b for the add event: \n node_a vector clock: {node_a.vector_clock} \n node_b vector clock: {node_b.vector_clock}')
node_b.multiply('account_balance', 50)
print(f'After multiply in node_b: \n node_a vector clock: {node_a.vector_clock} \n node_b vector clock: {node_b.vector_clock}')
node_a.receive_event('account_balance', node_b.get_data('account_balance'), node_b.vector_clock)
print(f'After update in node_a for the multiply event: \n node_a vector clock: {node_a.vector_clock} \n node_b vector clock: {node_b.vector_clock}')
node_a_value = node_a.get_data('account_balance')
node_b_value = node_b.get_data('account_balance')
# Everything would work fine in this case since all replicas received an update before a new write
print(f'Correct Run: \n Value of account_balance in node_a = {node_a_value} \n Value of account_balance in node_b = {node_b_value}\n')

# Resetting the value of 'account_balance' key to 1000 and resetting the clocks
node_a.set_data('account_balance', 1000)
node_b.set_data('account_balance', 1000)
node_a.reset_vector_clock()
node_b.reset_vector_clock()

# Run with overlap
print(f'Initial: \n node_a vector clock: {node_a.vector_clock} \n node_b vector clock: {node_b.vector_clock}')
node_a.add('account_balance', 100)
print(f'After add in node_a: \n node_a vector clock: {node_a.vector_clock} \n node_b vector clock: {node_b.vector_clock}')
node_b.multiply('account_balance', 50)
print(f'After multiply in node_b: \n node_a vector clock: {node_a.vector_clock} \n node_b vector clock: {node_b.vector_clock}')
node_b.receive_event('account_balance', node_a.get_data('account_balance'), node_a.vector_clock)
print(f'After update in node_b for the add event: \n node_a vector clock: {node_a.vector_clock} \n node_b vector clock: {node_b.vector_clock}')
node_a.receive_event('account_balance', node_b.get_data('account_balance'), node_b.vector_clock)
print(f'After update in node_a for the multiply event: \n node_a vector clock: {node_a.vector_clock} \n node_b vector clock: {node_b.vector_clock}')
node_a_value = node_a.get_data('account_balance')
node_b_value = node_b.get_data('account_balance')
# One of the writes would be lost in this case as there are concurrent writes.
# However due to clash in vector clocks, the system can flag this to the application or take a predefined decision
# In this case, it chose to implement the write based on the first receiver event
# The final value would also depend on the order of events received after the concurrent writes
print(f'Incorrect Run: \n Value of account_balance in node_a = {node_a_value} \n Value of account_balance in node_b = {node_b_value}')
