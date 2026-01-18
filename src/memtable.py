from typing import Dict, List, Optional
from datetime import datetime
from .types import Entry

class Memtable:
    """In-memory sorted key-value store"""
    
    def __init__(self, max_size_bytes: int = 4 * 1024 * 1024):
        self.storage: Dict[str, Entry] = {}
        self.current_size_bytes = 0
        self.max_size_bytes = max_size_bytes
    
    def set(self, key: str, value: Optional[str], ) -> None:
        """Set a key-value pair in the memtable."""
        
        if key in self.storage:
            old_entry = self.storage[key]
            self.current_size_bytes -= old_entry.size_bytes()
        
        new_entry = Entry(
            key=key,
            value=value,
            timestamp=datetime.now().timestamp()
        )
        
        self.storage[key] = new_entry
        
        self.current_size_bytes += new_entry.size_bytes()
    
    def get(self, key: str) -> Optional[str]:
        """Get value by key"""
        if key in self.storage:
            return self.storage[key].value
        return None
    
    def delete(self, key: str) -> None:
        """Delete a key from the memtable (marks it as deleted)"""
        self.set(key, None)
    
    def get_all(self) -> List[Entry]:
        """Get all entries in sorted order by key"""
        data = self.storage.values()
        return sorted(list(data), key=lambda k: k.key)
    
    def clear(self) -> None:
        """Clear the memtable"""
        self.storage.clear()
        self.current_size_bytes = 0

    @property
    def size(self) -> int:
        """Get the actual size in bytes of the memtable"""
        return self.current_size_bytes
    
    @property
    def should_flush(self) -> bool:
        """Check if the memtable has exceeded its max size"""
        return self.current_size_bytes >= self.max_size_bytes
    



if __name__ == '__main__':
    print("=== Testing Memtable ===\n")
    
    m = Memtable(max_size_bytes=1000)
    
    # Test 1: Set and Get
    print("Test 1: Set and Get")
    m.set('name', 'Alice')
    print(f"  get('name') = {m.get('name')}")
    assert m.get('name') == 'Alice'
    print("  ✓ Passed\n")

    # Test 2: Update
    print("Test 2: Update")
    size_before = m.size
    m.set('name', 'Bob')
    size_after = m.size
    print(f"  Updated 'name' to 'Bob'")
    print(f"  Size before: {size_before}, after: {size_after}")
    assert m.get('name') == 'Bob'
    print("  ✓ Passed\n")

    # Test 3: Delete
    print("Test 3: Delete")
    m.delete('name')
    print(f"  get('name') after delete = {m.get('name')}")
    assert m.get('name') is None
    print("  ✓ Passed\n")
    
    # Test 4: Sorting
    print("Test 4: Sorting")
    m.set('zebra', 'last')
    m.set('apple', 'first')
    m.set('mango', 'middle')
    entries = m.get_all()
    keys = [e.key for e in entries]
    print(f"  Keys in order: {keys}")
    assert keys == ['apple', 'mango', 'name', 'zebra']
    print("  ✓ Passed\n")
    
    # Test 5: Should flush
    print("Test 5: Should flush")
    m.clear()
    for i in range(100):
        m.set(f'key{i}', 'x' * 50)
    print(f"  Size: {m.size} bytes")
    print(f"  Should flush: {m.should_flush}")
    assert m.should_flush == True
    print("  ✓ Passed\n")
    
    # Test 6: Clear
    print("Test 6: Clear")
    print(f"  Size before clear: {m.size}")
    m.clear()
    print(f"  Size after clear: {m.size}")
    assert m.size == 0
    assert m.get('key0') is None
    print("  ✓ Passed\n")
    
    print("✅ All tests passed!")