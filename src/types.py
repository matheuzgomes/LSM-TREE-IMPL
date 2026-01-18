from dataclasses import dataclass
from typing import Optional



@dataclass
class Entry:
     """
    Represents a single key-value entry in our LSM-Tree
    
    Attributes:
        key: The key (string)
        value: The value (string or None for deletions)
        timestamp: When this entry was created (for conflict resolution)
    """
     key: str
     value: Optional[str]
     timestamp: float


     def size_bytes(self) -> int:
        """
        Calculate the approximate size of this entry in bytes
        
        This is used to track how much memory the Memtable is using
        """
        key_size = len(self.key.encode('utf-8'))
        value_size = len(self.value.encode('utf-8')) if self.value else 0
        timestamp_size = 8
        return key_size + value_size + timestamp_size