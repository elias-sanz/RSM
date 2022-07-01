import os

import openmatrix


def open_skims(
    skims_filename="FromSANDAG-Files/traffic_skims_AM.omx",
    data_dir=None,
    mode="r"
):

    if data_dir is not None:
        data_dir = os.path.expanduser(data_dir)
        cwd = os.getcwd()
        os.chdir(data_dir)
    else:
        cwd = None

    try:
        s = openmatrix.open_file(
            skims_filename,
            mode=mode,
        )
        return s

    finally:
        # change back to original cwd
        os.chdir(cwd)
