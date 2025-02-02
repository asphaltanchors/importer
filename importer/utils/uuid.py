"""UUID generation utilities."""

import uuid

def generate_uuid() -> str:
    """Generate a new UUID.
    
    Returns:
        String representation of UUID4
    """
    return str(uuid.uuid4())
