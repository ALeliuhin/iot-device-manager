"""Central controller for managing IoT devices with async updates, analytics, and storage."""
import asyncio
import json
import logging
import random
import threading
from collections import namedtuple
from datetime import datetime
from functools import reduce
from queue import Queue
from typing import List

from .devices import SmartDevice, SmartBulb, SmartThermostat, SmartCamera

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler("history.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Console handler for terminal output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Add both handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)


# Named tuple for processed device data
DeviceUpdate = namedtuple("DeviceUpdate", ["device_id", "timestamp", "value"])

# Temperature threshold for triggering cooling
TEMP_THRESHOLD = 30.0  # Celsius
COOLING_TARGET = 25.0  # Target temperature when cooling is triggered


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
            logger.error(f"Error writing to log: {e}")


async def device_update_stream(device: SmartDevice, update_queue: Queue) -> None:
    """Simulate a device sending updates asynchronously.
    
    Args:
        device: The device to simulate
        update_queue: Queue to put updates into
    """
    while True:
        await asyncio.sleep(random.uniform(1, 5))
        
        if device.is_connected:
            # Vary thermostat temperatures and humidity
            if isinstance(device, SmartThermostat):
                # Vary current temperature around target temperature
                current_temp = device.current_temp
                target_temp = device.target_temp
                # Random variation: move towards target with some randomness
                variation = random.uniform(-3.0, 5.0)  # Asymmetric: can spike higher
                new_temp = current_temp + (target_temp - current_temp) * 0.1 + variation
                # Occasionally add larger spikes to ensure threshold can be exceeded
                if random.random() < 0.15:  # 15% chance of a larger spike
                    spike = random.uniform(3.0, 8.0)
                    new_temp += spike
                device.execute_command("update_temp", temperature=new_temp)
                
                # Vary humidity slightly
                current_humidity = device.humidity
                humidity_variation = random.uniform(-3.0, 3.0)
                new_humidity = max(0.0, min(100.0, current_humidity + humidity_variation))
                device.execute_command("update_humidity", humidity=new_humidity)
            
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
    """Filter for critical events (temp > threshold or battery < 10%).
    
    Args:
        update: DeviceUpdate to check
        
    Returns:
        True if event is critical, False otherwise
    """
    # Check for high temperature (> threshold) or low battery (< 10)
    if isinstance(update.value, float) and update.value > TEMP_THRESHOLD:
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
        SmartThermostat("thermo_01", "Main Thermostat", "Living Room", 28.0, 24.0, 45.0),
        SmartThermostat("thermo_02", "Bedroom Thermostat", "Bedroom", 27.0, 25.0, 50.0),
        SmartCamera("cam_01", "Front Door Camera", "Entrance", 85),
        SmartCamera("cam_02", "Backyard Camera", "Garden", 5),
    ]
    
    # 3. Connect devices to the network
    for device in devices:
        device.connect()
    
    logger.info(f"Connected {len(devices)} devices to the network")
    
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
                
                # Execute commands for filtered devices
                for event in critical_events:
                    for device in devices:
                        if device.device_id == event.device_id:
                            if device.device_type == "THERMOSTAT" and isinstance(event.value, float) and event.value > TEMP_THRESHOLD:
                                # Cool down: reduce current temperature below threshold
                                current_temp = device.current_temp
                                # Cool down by at least 5°C, but ensure it goes below threshold
                                # Target cooling to COOLING_TARGET, but at minimum bring it below threshold
                                cooling_amount = max(5.0, current_temp - COOLING_TARGET)
                                new_temp = current_temp - cooling_amount
                                # Ensure temperature is below threshold after cooling
                                if new_temp >= TEMP_THRESHOLD:
                                    new_temp = TEMP_THRESHOLD - 2.0  # Cool to 2°C below threshold
                                device.execute_command("update_temp", temperature=new_temp)
                                device.execute_command("set_target_temp", temperature=COOLING_TARGET)
                                logger.warning(f"⚠ ALERT: High Temp detected! Triggering cooling...")
                                logger.info(f"Smart Thermostat command executed: Temperature adjusted.")
                            elif device.device_type == "CAMERA" and isinstance(event.value, int) and event.value < 10:
                                logger.warning(f"Warning: Camera {device.device_id} battery low: {event.value}%")
                
                # Reduce: Calculate metrics
                avg_temp = calculate_average_temperature(mapped_updates)
                if avg_temp > 0:
                    logger.info(f"Average house temperature: {avg_temp:.2f}°C")
                
                # 6. Put raw updates in storage queue
                for update in update_batch:
                    storage_queue.put(update)
                
                update_batch.clear()
            
            await asyncio.sleep(0.1)  # Small delay to prevent busy waiting
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        # Signal storage thread to stop
        storage_queue.put(None)
        # Cancel all device tasks
        for task in tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())

