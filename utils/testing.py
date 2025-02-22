from functools import wraps
from pprint import pprint
import io

def test_function(func, *args, **kwargs):
    """
        Test the function and prints its return output (truncate if too long)
        Returns True if the function runs without errors
    """

    try:
        print(f"Testing {func.__name__}")
        data = func(*args, **kwargs)
        print(f"{func.__name__} test passed!")
        output = io.StringIO()
        pprint(data, stream=output, depth=3, compact=True)
        formatted_data_str = output.getvalue()
        output.close()
        if len(formatted_data_str) > 1000:
            print(formatted_data_str[:1000] + '... (truncated)')
        else:
            print(formatted_data_str)
        print()
        return True
    except Exception as e:
        print(f"{func.__name__} test failed: {e}")
        print()
        return False

def get_comma_separated_values(values):
    """Return the values as a comma-separated string"""

    # Make sure values is a list or tuple
    if not isinstance(values, list) and not isinstance(values, tuple):
        values = [values]

    return ','.join(values)