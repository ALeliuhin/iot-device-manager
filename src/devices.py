"""Device Layer: Abstract base class with device implementations."""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional


class SmartDevice(ABC):
    """Abstract base class for all smart devices."""
    
    def __init__(self, device_id: str, name: str, location: str):
        """Initialize a smart device."""
        self._device_id = device_id
        self._name = name
        self._location = location
        self._device_type = "GENERIC"
        self._connected = False
    
    @property
    def device_id(self) -> str:
        """Get the device ID."""
        return self._device_id
    
    @property
    def name(self) -> str:
        """Get the device name."""
        return self._name
    
    @property
    def location(self) -> str:
        """Get the device location."""
        return self._location
    
    @property
    def device_type(self) -> str:
        """Get the device type."""
        return self._device_type
    
    @property
    def is_connected(self) -> bool:
        """Get the connection status."""
        return self._connected
    
    def connect(self) -> None:
        """Connect the device to the network."""
        self._connected = True
    
    def send_update(self) -> dict:
        """Send device status update.
        
        Returns:
            Dictionary containing device status information
        """
        return {
            "device_id": self._device_id,
            "name": self._name,
            "location": self._location,
            "device_type": self._device_type,
            "connected": self._connected
        }
    
    @abstractmethod
    def execute_command(self, command: str, **kwargs) -> bool:
        """Execute a command on the device.
        
        Args:
            command: The command to execute
            **kwargs: Additional command parameters
            
        Returns:
            True if command was executed successfully, False otherwise
        """
        pass


class SmartBulb(SmartDevice):
    """Smart bulb device with brightness control."""
    
    def __init__(self, device_id: str, name: str, location: str):
        """Initialize a smart bulb.
        
        Args:
            device_id: Unique identifier for the device
            name: Name of the device
            location: Location where the device is installed
        """
        super().__init__(device_id, name, location)
        self._device_type = "BULB"
        self._is_on = False
        self._brightness = 0
    
    @property
    def is_on(self) -> bool:
        """Get the on/off state of the bulb."""
        return self._is_on
    
    @property
    def brightness(self) -> int:
        """Get the brightness level (0-100)."""
        return self._brightness
    
    def _set_brightness(self, value: int) -> None:
        """Set brightness with validation (0-100).
        
        Args:
            value: Brightness value to set
        """
        if value < 0:
            self._brightness = 0
        elif value > 100:
            self._brightness = 100
        else:
            self._brightness = value
    
    def execute_command(self, command: str, **kwargs) -> bool:
        """Execute a command on the bulb.
        
        Supported commands:
            - "turn_on": Turn the bulb on
            - "turn_off": Turn the bulb off
            - "set_brightness": Set brightness (requires 'brightness' in kwargs)
            
        Args:
            command: The command to execute
            **kwargs: Additional command parameters
            
        Returns:
            True if command was executed successfully, False otherwise
        """
        if command == "turn_on":
            self._is_on = True
            return True
        elif command == "turn_off":
            self._is_on = False
            self._brightness = 0
            return True
        elif command == "set_brightness":
            if "brightness" in kwargs:
                brightness = kwargs["brightness"]
                self._set_brightness(brightness)
                if brightness > 0:
                    self._is_on = True
                return True
            return False
        return False
    
    def send_update(self) -> dict:
        """Send bulb status update."""
        update = super().send_update()
        update.update({
            "is_on": self._is_on,
            "brightness": self._brightness
        })
        return update


class SmartThermostat(SmartDevice):
    """Smart thermostat device with temperature and humidity control."""
    
    def __init__(self, device_id: str, name: str, location: str, 
                 current_temp: float = 20.0, target_temp: float = 20.0, 
                 humidity: float = 50.0):
        """Initialize a smart thermostat."""
        super().__init__(device_id, name, location)
        self._device_type = "THERMOSTAT"
        self._current_temp = current_temp
        self._target_temp = target_temp
        self._humidity = humidity
    
    @property
    def current_temp(self) -> float:
        """Get the current temperature."""
        return self._current_temp
    
    @property
    def target_temp(self) -> float:
        """Get the target temperature."""
        return self._target_temp
    
    @property
    def humidity(self) -> float:
        """Get the current humidity."""
        return self._humidity
    
    def execute_command(self, command: str, **kwargs) -> bool:
        """Execute a command on the thermostat.
        
        Supported commands:
            - "set_target_temp": Set target temperature (requires 'temperature' in kwargs)
            - "update_temp": Update current temperature (requires 'temperature' in kwargs)
            - "update_humidity": Update humidity (requires 'humidity' in kwargs)
            
        Args:
            command: The command to execute
            **kwargs: Additional command parameters
            
        Returns:
            True if command was executed successfully, False otherwise
        """
        if command == "set_target_temp":
            if "temperature" in kwargs:
                self._target_temp = kwargs["temperature"]
                return True
            return False
        elif command == "update_temp":
            if "temperature" in kwargs:
                self._current_temp = kwargs["temperature"]
                return True
            return False
        elif command == "update_humidity":
            if "humidity" in kwargs:
                self._humidity = kwargs["humidity"]
                return True
            return False
        return False
    
    def send_update(self) -> dict:
        """Send thermostat status update."""
        update = super().send_update()
        update.update({
            "current_temp": self._current_temp,
            "target_temp": self._target_temp,
            "humidity": self._humidity
        })
        return update


class SmartCamera(SmartDevice):
    """Smart camera device with motion detection and battery monitoring."""
    
    def __init__(self, device_id: str, name: str, location: str,
                 battery_level: int = 100):
        """Initialize a smart camera."""
        super().__init__(device_id, name, location)
        self._device_type = "CAMERA"
        self._motion_detected = False
        self._battery_level = max(0, min(100, battery_level))  # Clamp to 0-100
        self._last_snapshot: Optional[datetime] = None
    
    @property
    def motion_detected(self) -> bool:
        """Get the motion detection state."""
        return self._motion_detected
    
    @property
    def battery_level(self) -> int:
        """Get the battery level (0-100)."""
        return self._battery_level
    
    @property
    def last_snapshot(self) -> Optional[datetime]:
        """Get the timestamp of the last snapshot."""
        return self._last_snapshot
    
    def _set_battery_level(self, value: int) -> None:
        """Set battery level with validation (0-100).
        
        Args:
            value: Battery level to set
        """
        if value < 0:
            self._battery_level = 0
        elif value > 100:
            self._battery_level = 100
        else:
            self._battery_level = value
    
    def execute_command(self, command: str, **kwargs) -> bool:
        """Execute a command on the camera.
        
        Supported commands:
            - "take_snapshot": Take a snapshot
            - "set_motion_detected": Set motion detection state (requires 'motion' in kwargs)
            - "set_battery_level": Set battery level (requires 'battery_level' in kwargs)
            
        Args:
            command: The command to execute
            **kwargs: Additional command parameters
            
        Returns:
            True if command was executed successfully, False otherwise
        """
        if command == "take_snapshot":
            self._last_snapshot = datetime.now()
            return True
        elif command == "set_motion_detected":
            if "motion" in kwargs:
                self._motion_detected = bool(kwargs["motion"])
                return True
            return False
        elif command == "set_battery_level":
            if "battery_level" in kwargs:
                self._set_battery_level(kwargs["battery_level"])
                return True
            return False
        return False
    
    def send_update(self) -> dict:
        """Send camera status update."""
        update = super().send_update()
        update.update({
            "motion_detected": self._motion_detected,
            "battery_level": self._battery_level,
            "last_snapshot": self._last_snapshot.isoformat() if self._last_snapshot else None
        })
        return update

