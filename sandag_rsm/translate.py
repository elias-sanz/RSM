import os
import logging
import pandas as pd
from pathlib import Path

from .data_load.skims import open_skims

logger = logging.getLogger(__name__)


def translate_demand(
        skims_file, agg_map, matrix_list=None, data_dir=None, export_dir=None
):
    matrix, matrix_list = load_matrix(skims_file, matrix_list, data_dir)
    aggregate_matrix(matrix, matrix_list, agg_map)
    export_matrix(skims_file, matrix, matrix_list, export_dir)


def load_matrix(
        skims_file, matrix_list, data_dir
):
    logger.info(f'Loading Matrix: {skims_file}')

    # if isinstance(skims_file, emme_path):
    #     matrix_load = load_matrix_emme(skims_file, emme_path=emme_path)
    if isinstance(skims_file, (str, Path)):
        matrix_load = open_skims(skims_file, data_dir=data_dir)
        if matrix_list is None:
            matrix_list = matrix_load.list_matrices()
        print(f'Matrices in file: {matrix_list}')

    return {m: pd.DataFrame(matrix_load[m]) for m in matrix_list}, matrix_list


def aggregate_matrix(
        matrix, matrix_list, agg_map
):
    logger.info('Aggregating Matrix')
    for m in matrix_list:
        matrix[m] = matrix[m].rename(columns=(agg_map))
        matrix[m] = matrix[m].rename(index=(agg_map))

        matrix[m] = matrix[m].stack().groupby(level=[0,1]).sum().unstack()


def export_matrix(
        skims_file, matrix, matrix_list, export_dir, file_type='omx'
):
    logger.info('Exporting Matrix')
    if file_type == 'omx':
        matrix_save = open_skims(skims_file, data_dir=export_dir, mode="a")

        for m in matrix_list:
            matrix_save[m] = matrix[m].to_numpy()

        matrix_save.close()

    elif file_type == 'csv':
        for m in matrix_list:
            matrix[m].to_csv(os.path.join(export_dir, f'{m}.csv'))

    elif file_type == 'emme':
        # save_matrix_emme(emme_path, matrix_name)
        pass
