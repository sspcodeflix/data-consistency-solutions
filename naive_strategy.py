import threading
class DataStore:
    """
    Simple key-value store with multiple replicas storing the same data.
    """

    def __init__(self, kv_dict=None):
        """
        Initialize the DataStore with an optional key-value dictionary.
        """
        self._kv_dict = kv_dict if kv_dict else {}
        self.lock = threading.Lock()
    @property
    def kv_dict(self):
        """
        Get the key-value dictionary.
        """
        return self._kv_dict
    
    def get_data(self, key):
            """
            Retrieve the value associated with the given key.
            """
            with self.lock:
                return self.kv_dict.get(key)

    def set_data(self, key, value):
        """
        Set the value for the given key.
        """
        with self.lock:
            self.kv_dict[key] = value

    def add(self, key, value):
            """
            Add the specified value to the existing value for the given key.
            """
            with self.lock:
                if key in self.kv_dict:
                    self.kv_dict[key] += value
                else:
                    self.kv_dict[key] = value
        
    def multiply(self, key, value):
        """
        Multiply the existing value for the given key by the specified value.
        """
        with self.lock:
            if key in self.kv_dict:
                self.kv_dict[key] *= value
            else:
                self.kv_dict[key] = 0

    def receive_event(self, key, value):
        """
        Update the value for the given key based on an event from another replica.
        """
        self.set_data(key, value)

def replicate_data(store_from, store_to, key):
    """
    Replicate data from one store(node) to another for the given key.
    """
    value = store_from.get_data(key)
    store_to.receive_event(key, value)

def run_correct_scenario():
    """
    Run the scenario without overlap.
    """
    node_a = DataStore({'account_balance': 1000})
    node_b = DataStore({'account_balance': 1000})

    node_a.add('account_balance', 100)
    replicate_data(node_a, node_b, 'account_balance')
    node_b.multiply('account_balance', 50)
    replicate_data(node_b, node_a, 'account_balance')

    node_a_value = node_a.get_data('account_balance')
    node_b_value = node_b.get_data('account_balance')
    print(f'Correct Run: Value of account_balance in node_a = {node_a_value}, and in node_b = {node_b_value}')

def run_incorrect_scenario():
    """
    Run the scenario with overlap to demonstrate concurrency issues.
    """
    node_a = DataStore({'account_balance': 1000})
    node_b = DataStore({'account_balance': 1000})

    thread1 = threading.Thread(target=node_a.add, args=('account_balance', 100))
    thread2 = threading.Thread(target=node_b.multiply, args=('account_balance', 50))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    replicate_data(node_a, node_b, 'account_balance')
    replicate_data(node_b, node_a, 'account_balance')

    node_a_value = node_a.get_data('account_balance')
    node_b_value = node_b.get_data('account_balance')
    print(f'Incorrect Run: Value of account_balance in node_a = {node_a_value}, and in node_b = {node_b_value}')

if __name__ == "__main__":
    run_correct_scenario()
    run_incorrect_scenario()