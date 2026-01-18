from typing import List
import os
from .types import Entry

class WAL:
    """Write-Ahead Log for durability"""
    
    def __init__(self, filepath: str):
        """
        Initialize WAL
        
        Args:
            filepath: Path to the WAL file (e.g., 'data/wal/wal.log')
        """
        self.filepath = filepath

        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        if not os.path.exists(filepath):
            with open(filepath, 'w'):
                pass
    
    def log_put(self, key: str, value: str, timestamp: float) -> None:
        """
        Append a PUT operation to the WAL
        
        Format: PUT|key|value|timestamp\n
        
        Args:
            key: The key
            value: The value (should not be None for PUT)
            timestamp: When this happened
        """

        if value is None:
            raise ValueError("Value for PUT operation cannot be None")
        
        format_line = f"PUT|{key}|{value.replace('|', ' ')}|{timestamp}\n"
        with open(self.filepath, 'a') as f:
            f.write(format_line)

    def log_delete(self, key: str, timestamp: float) -> None:
        """
        Append a DELETE operation to the WAL
        
        Format: DEL|key|timestamp\n

        Args:
            key: The key to delete
            timestamp: When this happened
        """
        format_line = f"DEL|{key}|{timestamp}\n"
        with open(self.filepath, 'a') as f:
            f.write(format_line)
    
    def replay(self) -> List[Entry]:
        entries = []

        try:
            with open(self.filepath, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts: List[str] = line.split('|')
                    operation: str = parts[0]
                    
                    if operation == 'PUT':
                        key = parts[1]
                        value = parts[2]
                        timestamp = float(parts[3])
                        entries.append(Entry(key=key, value=value, timestamp=timestamp))
                        
                    elif operation == 'DEL':
                        key = parts[1]
                        timestamp = float(parts[2])
                        entries.append(Entry(key=key, value=None, timestamp=timestamp))
        except FileNotFoundError:
            pass
        except (IndexError, ValueError) as e:
            print(f"Malformed WAL entry: {e}")
        except Exception as e:
            print(f"Error reading WAL: {e}")

        return entries
    
    def clear(self) -> None:
        """
        Delete the WAL file
        Called after successfully flushing Memtable to SSTable
        """
        if os.path.exists(self.filepath):
            os.remove(self.filepath)
        open(self.filepath, 'w').close()