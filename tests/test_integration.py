import os
from src.lsm_engine import LSMEngine

def test_integration():
    print("=== Testing LSM Engine Integration ===\n")
    
    wal_path = 'data/wal/test_integration.log'
    
    if os.path.exists(wal_path):
        os.remove(wal_path)
    
    print("Test 1: Basic PUT and GET")
    engine = LSMEngine(wal_path)
    
    engine.put('name', 'Alice')
    engine.put('age', '25')
    engine.put('city', 'NYC')
    
    assert engine.get('name') == 'Alice', "Failed to get 'name'"
    assert engine.get('age') == '25', "Failed to get 'age'"
    assert engine.get('city') == 'NYC', "Failed to get 'city'"
    print("  ✓ PUT and GET work\n")
    
    # Test 2: Update
    print("Test 2: UPDATE")
    engine.put('name', 'Bob')
    assert engine.get('name') == 'Bob', "Failed to update 'name'"
    print("  ✓ UPDATE works\n")
    
    # Test 3: Delete
    print("Test 3: DELETE")
    engine.delete('age')
    assert engine.get('age') is None, "Delete didn't work"
    print("  ✓ DELETE works\n")
    
    # Test 4: Non-existent key
    print("Test 4: Non-existent key")
    assert engine.get('nonexistent') is None, "Should return None for missing key"
    print("  ✓ Non-existent key returns None\n")
    
    # Test 5: Crash recovery (THE MOST IMPORTANT TEST!)
    print("Test 5: CRASH RECOVERY")
    print("  Current state:")
    print(f"    name = {engine.get('name')}")
    print(f"    age = {engine.get('age')}")
    print(f"    city = {engine.get('city')}")
    
    print("\n  💥 Simulating crash (deleting engine)...")
    del engine  # Simulate crash - all in-memory data lost!
    
    print("  🔄 Restarting engine (should recover from WAL)...")
    engine = LSMEngine(wal_path)
    
    print("  Recovered state:")
    print(f"    name = {engine.get('name')}")
    print(f"    age = {engine.get('age')}")
    print(f"    city = {engine.get('city')}")
    
    # Verify recovery
    assert engine.get('name') == 'Bob', "Failed to recover 'name'"
    assert engine.get('age') is None, "Failed to recover deleted 'age'"
    assert engine.get('city') == 'NYC', "Failed to recover 'city'"
    
    print("  ✓ Recovery works! Data survived the crash! 🎉\n")
    
    # Test 6: Multiple crashes
    print("Test 6: Multiple crashes")
    engine.put('test', 'value1')
    del engine  # Crash 1
    
    engine = LSMEngine(wal_path)
    assert engine.get('test') == 'value1'
    
    engine.put('test', 'value2')
    del engine  # Crash 2
    
    engine = LSMEngine(wal_path)
    assert engine.get('test') == 'value2'
    print("  ✓ Multiple crashes handled correctly\n")
    
    # Test 7: Size tracking
    print("Test 7: Size tracking")
    engine.put('key1', 'x' * 100)
    engine.put('key2', 'y' * 100)
    size = engine.size()
    print(f"  Current size: {size} bytes")
    assert size > 200, "Size should be > 200 bytes"
    print("  ✓ Size tracking works\n")
    
    # Cleanup
    if os.path.exists(wal_path):
        os.remove(wal_path)
    
    print("All integration tests passed!")
    print("\nYou now have a crash-resistant key-value store!")

if __name__ == '__main__':
    test_integration()