from functools import wraps

def test_function(func, *args, **kwargs):
    """
        Test the function and prints its return output
        Returns True if the function runs without errors
    """

    try:
        original_func = func
        print(f"Testing {func.__name__}")
        data = func(*args, **kwargs)
        print(f"{original_func.__name__} test passed!")
        print(data)
        print()
        return True
    except Exception as e:
        print(f"{original_func.__name__} test failed: {e}")
        print()
        return False

def get_comma_separated_values(values):
    """Return the values as a comma-separated string"""

    # Make sure values is a list or tuple
    if not isinstance(values, list) and not isinstance(values, tuple):
        values = [values]

    return ','.join(values)