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

    parser.add_argument("-d", help="debug mode", action="store_true")

    args = parser.parse_args()

    logging.debug(args)
    return args


def add_missing_keys(infile, datatype, name):
    """Adds missing keywords to metadata
    args:
    infile (str): path to the file to generate from
    datatype (str): name of folder in share
    name (str): name of file
    """
    missing_key = "workflow"
    runpath = Path(infile).parent.parent.parent
    sharepath = runpath / "share" / "results" / datatype
    print(f" Looking in {sharepath}")
    metadatafiles = sharepath.glob(f"?{name}*.*.yml")
    for metadatafile in metadatafiles:
        print(metadatafile)
        metadata = yaml_load(metadatafile)
        if missing_key not in metadata:

            metadata["fmu"][missing_key] = {"reference": "eclipse"}
            with open(metadatafile, "w", encoding="utf-8") as yamhandle:
                yaml.dump(metadata, yamhandle)
            print(f" Added {missing_key} to {metadatafile}")
        else:
            print(f" {missing_key} exists in {metadatafile}")


def export_sum(eclipse_path, cfg_path):
    """Exports summary data with fmu-dataio
    args:
     eclipse_path (str): path to eclipse datafile
     cfg_path (str): path to fmu config yaml file
    """
    if not eclipse_path.endswith(".DATA"):
        eclipse_path += ".DATA"

    name = "summary"
    eclfiles = EclFiles(eclipse_path)
    dframe = summary.df(eclfiles)

    exporter = ExportData(config=yaml_load(cfg_path))
    exporter.export(pa.Table.from_pandas(dframe), name=name, workflow="eclipse")
    # add_missing_keys(eclipse_path, "tables", name)


def export_volumes(cfg_path, runpath="."):
    """Exports any csv file with fmu-dataio
    args:
     cfg_path (str): path to fmu config yaml file
     runpath (str): path to eclipse datafile
    """

    table_path = Path(runpath) / "share" / "results" / "volumes"
    exporter = ExportData(config=yaml_load(cfg_path))
    csv_files = table_path.glob("*.csv")
    for csv_file in csv_files:
        name = csv_file.name
        print(f"Exporting {str(csv_file)}")
        exporter.export(pd.read_csv(csv_file), name=name)
        # add_missing_keys(csv_file.parent, "volumes", name)


def main():
    """ fetches input, exports parquet file"""
    args = parse_args()
    export_sum(args.Eclipse_path, args.Config_path)
    # export_volumes(args.Config_path)

if __name__ == "__main__":
    main()
