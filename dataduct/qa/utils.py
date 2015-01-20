"""
Shared utility functions
"""

def render_output(data):
    """Print the formatted output for the list
    """
    output = ['[Dataduct]: ']
    output.extend(data)
    return '\n'.join(output)
