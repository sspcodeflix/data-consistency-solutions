class DataStore:
    def __init__(self, kv_dict):
        self._kv_dict = kv_dict
        self._replica_list = []
        self._lock_dict = {}

    @property
    def kv_dict(self):
        return self._kv_dict

    @property
    def replica_list(self):
        return self._replica_list

    @property
    def lock_dict(self):
        return self._lock_dict

    def add_replicas(self, replica_list):
        self._replica_list.extend(replica_list)
    
    def add_lock(self, key):
        if self.lock_dict.get(key) and self.lock_dict.get(key) == 1:
            return -1
        
        self.lock_dict[key] = 1
        for replica in self._replica_list:
            replica.add_remote_lock(key)
        return 1

    def add_remote_lock(self, key):
        if self.lock_dict.get(key) and self.lock_dict.get(key) == 1:
            return -1
        
        self.lock_dict[key] = 1
        return 1

    def remove_lock(self, key):
        self.lock_dict[key] = 0
        for replica in self._replica_list:
            replica.remove_remote_lock(key)

    def remove_remote_lock(self, key):
        self.lock_dict[key] = 0

    def get_data(self, key):
        return self.kv_dict.get(key)

    def set_data(self, key, value):
        if self.add_lock(key) == -1:
            return -1

        self.kv_dict[key] = value
        for replica in self._replica_list:
            replica.set_remote_data(key, value)
        self.remove_lock(key)
        return 1

    def set_remote_data(self, key, value):
        self.kv_dict[key] = value

    def add(self, key, value):
        if self.get_data(key):
            result = self.set_data(key, self.get_data(key) + value)
        else:
            result = self.set_data(key, value)
        return result
    
    def multiply(self, key, value):
        if self.get_data(key):
            result = self.set_data(key, self.get_data(key) * value)
        else:
            result = self.set_data(key, 0)
        return result

# Client processing

# Setting up the stores
node_a = DataStore({})
node_a.set_data('account_balance', 1000)
node_b = DataStore({})
node_b.set_data('account_balance', 1000)
node_a.add_replicas([node_b])
node_b.add_replicas([node_a])

# In case of concurrent writes, the second one will not be able to lock the data to write to it
# It'll return failure. In normal cases, there is a wait mechanism on the lock, and failure after a wait timeout
add_result = node_a.add('account_balance', 100)
if add_result == -1:
    add_message = 'Failed to acquire locks, please retry'
else:
    add_message = 'Add operation successful'

mul_result = node_b.multiply('account_balance', 50)
if mul_result == -1:
    mul_message = 'Failed to acquire locks, please retry'
else:
    mul_message = 'Multiply operation successful'

node_a_value = node_a.get_data('account_balance')
node_b_value = node_b.get_data('account_balance')

print(f'Correct Run: Value of account balance in node_a = {node_a_value}, and in node_b = {node_b_value}')