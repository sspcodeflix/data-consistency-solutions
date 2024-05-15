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
    Replicate data from one store to another for the given key.
    """
    value = store_from.get_data(key)
    store_to.receive_event(key, value)

def run_correct_scenario():
    """
    Run the scenario without overlap.
    """
    s1 = DataStore({'x': 5})
    s2 = DataStore({'x': 5})

    s1.add('x', 2)
    replicate_data(s1, s2, 'x')
    s2.multiply('x', 3)
    replicate_data(s2, s1, 'x')

    s1_value = s1.get_data('x')
    s2_value = s2.get_data('x')
    print(f'Correct Run: Value of x in s1 = {s1_value}, and in s2 = {s2_value}')

def run_incorrect_scenario():
    """
    Run the scenario with overlap to demonstrate concurrency issues.
    """
    s1 = DataStore({'x': 5})
    s2 = DataStore({'x': 5})

    thread1 = threading.Thread(target=s1.add, args=('x', 2))
    thread2 = threading.Thread(target=s2.multiply, args=('x', 3))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    replicate_data(s1, s2, 'x')
    replicate_data(s2, s1, 'x')

    s1_value = s1.get_data('x')
    s2_value = s2.get_data('x')
    print(f'Incorrect Run: Value of x in s1 = {s1_value}, and in s2 = {s2_value}')

if __name__ == "__main__":
    run_correct_scenario()
    run_incorrect_scenario()
