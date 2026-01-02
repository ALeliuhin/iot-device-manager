"""Central controller for managing IoT devices with async updates, analytics, and storage."""
import asyncio
import json
import random
import threading
from collections import namedtuple
from datetime import datetime
from functools import reduce
from queue import Queue
from typing import List

from .devices import SmartDevice, SmartBulb, SmartThermostat, SmartCamera


# Named tuple for processed device data
DeviceUpdate = namedtuple("DeviceUpdate", ["device_id", "timestamp", "value"])


def storage_worker(queue: Queue, log_file: str = "history.log") -> None:
    """Daemon worker that writes device updates to history.log.
    
    Args:
        queue: Queue containing device update dictionaries
        log_file: Path to the log file
    """
    while True:
        try:
            update = queue.get()
            if update is None: 
                break
            
            timestamp = datetime.now().isoformat()
            log_entry = {
                "timestamp": timestamp,
                "update": update
            }
            
            with open(log_file, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
            
            queue.task_done()
        except Exception as e:
            print(f"Error writing to log: {e}")


async def device_update_stream(device: SmartDevice, update_queue: Queue) -> None:
    """Simulate a device sending updates asynchronously.
    
    Args:
        device: The device to simulate
        update_queue: Queue to put updates into
    """
    while True:
        await asyncio.sleep(random.uniform(1, 5))
        
        if device.is_connected:
            update = device.send_update()
            update["timestamp"] = datetime.now().isoformat()
            update_queue.put(update)


def map_to_device_update(raw_data: dict) -> DeviceUpdate:
    """Map raw JSON data to DeviceUpdate named tuple.
    
    Args:
        raw_data: Raw device update dictionary
        
    Returns:
        DeviceUpdate named tuple
    """
    device_id = raw_data.get("device_id", "")
    timestamp = raw_data.get("timestamp", "")
    
    # Extract value based on device type
    device_type = raw_data.get("device_type", "")
    if device_type == "BULB":
        value = raw_data.get("brightness", 0)
    elif device_type == "THERMOSTAT":
        value = raw_data.get("current_temp", 0.0)
    elif device_type == "CAMERA":
        value = raw_data.get("battery_level", 0)
    else:
        value = None
    
    return DeviceUpdate(device_id=device_id, timestamp=timestamp, value=value)


def filter_critical_events(update: DeviceUpdate) -> bool:
    """Filter for critical events (temp > 30 or battery < 10%).
    
    Args:
        update: DeviceUpdate to check
        
    Returns:
        True if event is critical, False otherwise
    """
    # Check for high temperature (> 30) or low battery (< 10)
    if isinstance(update.value, float) and update.value > 30:
        return True  # High temperature
    if isinstance(update.value, int) and update.value < 10:
        return True  # Low battery
    return False


def calculate_average_temperature(updates: List[DeviceUpdate]) -> float:
    """Calculate average temperature from thermostat updates using reduce.
    
    Args:
        updates: List of DeviceUpdate objects
        
    Returns:
        Average temperature
    """
    thermostat_updates = [u for u in updates if isinstance(u.value, float)]
    
    if not thermostat_updates:
        return 0.0
    
    def reducer(acc, update):
        count, total = acc
        return (count + 1, total + update.value)
    
    count, total = reduce(reducer, thermostat_updates, (0, 0.0))
    return total / count if count > 0 else 0.0


async def main():
    """Main controller function."""

    # 1. Define thread that saves data to history.log
    storage_queue = Queue()
    storage_thread = threading.Thread(
        target=storage_worker,
        args=(storage_queue,),
        daemon=True
    )
    storage_thread.start()
    
    # Queue for receiving device updates
    update_queue = Queue()
    
    # 2. Instantiate smart devices
    devices: List[SmartDevice] = [
        SmartBulb("bulb_01", "Living Room Light", "Living Room"),
        SmartBulb("bulb_02", "Bedroom Light", "Bedroom"),
        SmartThermostat("thermo_01", "Main Thermostat", "Living Room", 22.0, 20.0, 45.0),
        SmartThermostat("thermo_02", "Bedroom Thermostat", "Bedroom", 25.0, 22.0, 50.0),
        SmartCamera("cam_01", "Front Door Camera", "Entrance", 85),
        SmartCamera("cam_02", "Backyard Camera", "Garden", 5),
    ]
    
    # 3. Connect devices to the network
    for device in devices:
        device.connect()
    
    print(f"Connected {len(devices)} devices to the network")
    
    # 4. Create async tasks for each device to send updates
    tasks = [
        asyncio.create_task(device_update_stream(device, update_queue))
        for device in devices
    ]
    
    # Process updates in batches
    batch_size = 10
    update_batch = []
    
    try:
        while True:
            # Collect updates from queue (non-blocking)
            while not update_queue.empty() and len(update_batch) < batch_size:
                try:
                    update = update_queue.get_nowait()
                    update_batch.append(update)
                except:
                    break
            
            if update_batch:
                # 5. Process updates with functional programming pipeline
                # Map: Transform raw data to DeviceUpdate
                mapped_updates = list(map(map_to_device_update, update_batch))
                
                # Filter: Get critical events
                critical_events = list(filter(filter_critical_events, mapped_updates))
                
                # Execute commands for filtered devices (e.g., thermostats with temp > 30)
                for event in critical_events:
                    for device in devices:
                        if device.device_id == event.device_id:
                            if device.device_type == "THERMOSTAT" and isinstance(event.value, float) and event.value > 30:
                                device.execute_command("set_target_temp", temperature=event.value - 2)
                                print(f"Adjusted thermostat {device.device_id} target temp to {event.value - 2}")
                            elif device.device_type == "CAMERA" and isinstance(event.value, int) and event.value < 10:
                                print(f"Warning: Camera {device.device_id} battery low: {event.value}%")
                
                # Reduce: Calculate metrics
                avg_temp = calculate_average_temperature(mapped_updates)
                if avg_temp > 0:
                    print(f"Average house temperature: {avg_temp:.2f}°C")
                
                # 6. Put raw updates in storage queue
                for update in update_batch:
                    storage_queue.put(update)
                
                update_batch.clear()
            
            await asyncio.sleep(0.1)  # Small delay to prevent busy waiting
            
    except KeyboardInterrupt:
        print("\nShutting down...")
        # Signal storage thread to stop
        storage_queue.put(None)
        # Cancel all device tasks
        for task in tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())

