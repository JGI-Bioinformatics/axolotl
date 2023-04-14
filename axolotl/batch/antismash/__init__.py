"""
runner module to run antiSMASH on a set of genome files (fasta/gbk)
pass antismash parameters via params

[!!] as of current implementation, this module will create a subdirectory
with the basename of the input file (e.g., genome.fasta/) under the target
directory. Therefore, you need to be careful not to include genome files with
the same name from different directories.
"""

from typing import List
from os import path, makedirs
from subprocess import check_call, DEVNULL


def run_module(input_file: str, target_root_folder: str, params: List=[]) -> int:
    output_folder = path.join(target_root_folder, path.basename(input_file))
    if path.exists(output_folder) and path.exists(path.join(output_folder, "regions.js")):
        result = -1 # skipped
    else:
        result = 0
        try:
            command = ["antismash", "--output-dir", output_folder]

            # check additional parameters
            if len(params) < 1:
                command.extend(["--minimal", "--taxon", "bacteria", "--skip-sanitisation"])
            else:
                command.extend(params)

            command.append(input_file)
            check_call(command, stdout=DEVNULL)
            result = 1 # success
        except FileNotFoundError as err:
            print("FAILED: (%s) antismash is not found in the path" % (input_file))
        except Exception:
            print("FAILED: (%s) unknown error" % (input_file))
    return result


def show_help():
    print("example help text for a batch module")