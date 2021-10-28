"""Removes all `__pycache__` folders from the app

"""

import os
import shutil

path = f"{os.path.dirname(os.path.dirname(__file__))}"

for directories, subfolder, files in os.walk(path):
    if os.path.isdir(directories):
        if directories[::-1][:11][::-1] == "__pycache__":
            shutil.rmtree(directories)
