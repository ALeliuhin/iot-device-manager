"""Main entry point for the EcoHub IoT Device Manager application."""
import asyncio
from src.controller import main


if __name__ == "__main__":
    asyncio.run(main())

