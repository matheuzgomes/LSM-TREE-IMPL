from typing import Optional
from datetime import datetime, timezone

from .types import Entry
from .memtable import Memtable
from .wal import WAL

class LSMEngine:
    """
    Simple LSM-Tree engine that coordinates WAL and Memtable
    
    Write path: WAL → Memtable
    Read path: Memtable only (for now)
    """
    
    def __init__(self, wal_path: str = 'data/wal/wal.log'):
        """
        Initialize the LSM engine
        
        YOUR TASK:
        1. Create WAL instance
        2. Create Memtable instance
        3. Call _recover() to rebuild from WAL if needed
        """
        self.wal = WAL(wal_path)
        self.memtable = Memtable()
        self._recover()
    
    def _recover(self):
        """
        Recover data from WAL (crash recovery)
        
        YOUR TASK:
        1. Call self.wal.replay() to get all entries
        2. For each entry:
           - Put it DIRECTLY into memtable.storage (bypass set())
           - Update memtable.current_size_bytes
        
        Why bypass set()? Because set() calls WAL again!
        We're just rebuilding from existing WAL.
        """
        entries = self.wal.replay()

        if len(entries) == 0:
            return

        for entry in entries:
            self.memtable.storage[entry.key] = Entry(
                key=entry.key,
                value=entry.value,
                timestamp=entry.timestamp
            )
            self.memtable.current_size_bytes += len(entry.key) + len(entry.value) if entry.value else 0 + 8
    
    def put(self, key: str, value: str) -> None:
        """
        Write a key-value pair
        """
        try:
            timestamp = datetime.now(timezone.utc).timestamp()
            self.wal.log_put(key, value, timestamp)
            self.memtable.set(key, value)
        except ValueError as e:
            print(f"Error in put operation: {e}")
        except Exception as e:
            print(f"Unexpected error in put operation: {e}")

    def get(self, key: str) -> Optional[str]:
        """
        Read a value by key
        
        YOUR TASK:
        Just read from memtable: return self.memtable.get(key)
        
        (Later we'll also check SSTables)
        """

        return self.memtable.get(key)
    
    def delete(self, key: str) -> None:
        """
        Delete a key (tombstone)
        
        YOUR TASK:
        1. Get current timestamp
        2. Write to WAL: self.wal.log_delete(key, timestamp)
        3. Write to Memtable: self.memtable.delete(key)
        """
        timestamp = datetime.now(timezone.utc).timestamp()
        self.wal.log_delete(key, timestamp)
        self.memtable.delete(key)

    def size(self) -> int:
        """Get current memtable size"""
        return self.memtable.size
    
    def should_flush(self) -> bool:
        """Check if memtable should be flushed"""
        return self.memtable.should_flush