"""
an example implementation of the axolotl batch module\

to create a new module, make a new folder under "axolotl/batch", then in the __init__.py script,
implement these functions:

- run_module(input_file: str, target_root_folder: str, params: List={}) -> int
will be called by each spark executor on each input file (i.e., the mapped function)

- show_help()
module-specific help message
"""

from typing import List


def run_module(input_file: str, target_root_folder: str, params: List={}) -> int:
	print("example module successfully ran! input_file=%s target_folder=%s params=%s" % (input_file, target_root_folder, params))
	return 1


def show_help():
	print("example help text for a batch module")