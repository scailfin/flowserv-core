import os
import sys

value = os.environ[sys.argv[1]]
if value == 'error':
    raise ValueError('there was an error')
print(value)
