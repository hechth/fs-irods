def can_create(mode: str) -> bool:
    """Check if the mode implies creating a file."""
    if any(map(lambda x: x in mode, ["w", "a"])):
        return True
    return False
