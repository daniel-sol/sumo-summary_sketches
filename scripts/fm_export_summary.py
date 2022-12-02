#!/usr/bin/env python
"""Author dbs: exports summary data with fmu-dataio"""
import argparse
import logging
from pathlib import Path
import yaml
import pandas as pd
import pyarrow as pa
from ecl2df import summary, EclFiles
from fmu.dataio import ExportData
from fmu.config.utilities import yaml_load


logging.basicConfig(level="DEBUG")


def parse_args():
    """Parses the arguments required
    returns:
    args: arguments as name space
    """
    form_class = argparse.ArgumentDefaultsHelpFormatter
    description = (
        "Exports a summary file as arrow"
    )
    parser = argparse.ArgumentParser(
        formatter_class=form_class, description=description
    )
    parser.add_argument(
        "Eclipse_path", type=str,
        help="path to eclipse datafile without suffix"
    )
    parser.add_argument(
        "Config_path", type=str,
        help="path to fmu config yaml file"
    )

    parser.add_argument("--vectors", nargs="*", default=["*"], help="vector filter")

    parser.add_argument("-d", help="debug mode", action="store_true")

    args = parser.parse_args()

    logging.debug(args)
    return args


def export_sum(eclipse_path, cfg_path, vectors):
    """Exports summary data with fmu-dataio
    args:
     eclipse_path (str): path to eclipse datafile
     cfg_path (str): path to fmu config yaml file
    """
    if not eclipse_path.endswith(".DATA"):
        eclipse_path += ".DATA"

    name = "summary"
    eclfiles = EclFiles(eclipse_path)
    extract_columns = vectors
    dframe = summary.df(eclfiles, column_keys=extract_columns)
    print(f"Exporting vectors {dframe.columns.tolist()}")
    exporter = ExportData(config=yaml_load(cfg_path))
    exporter.export(pa.Table.from_pandas(dframe), name=name, workflow="eclipse")


def main():
    """ fetches input, exports parquet file"""
    args = parse_args()
    export_sum(args.Eclipse_path, args.Config_path, args.vectors)


if __name__ == "__main__":
    main()
