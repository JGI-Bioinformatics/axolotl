import pkgutil
import importlib
from os import path

def get_module_data(name):
	""""""
	try:
		return importlib.import_module(f"axolotl.batch.{name}")
	except ModuleNotFoundError:
		return None