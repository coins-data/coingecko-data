from functools import wraps

def test_function(func, *args, **kwargs):
    """Test the function and print the output"""

    try:
        original_func = func
        print(f"Testing {func.__name__}")
        data = func(*args, **kwargs)
        print(f"{original_func.__name__} test passed!")
        print(data)
    except Exception as e:
        print(f"{original_func.__name__} test failed: {e}")
    print()

def get_comma_separated_values(values):
    """Return the values as a comma-separated string"""

    # Make sure values is a list or tuple
    if not isinstance(values, list) and not isinstance(values, tuple):
        values = [values]

    return ','.join(values)