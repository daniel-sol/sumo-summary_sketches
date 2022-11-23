#!/usr/bin/env python
"""Author dbs: aggregates summary files, splits up by vector"""
import argparse
import logging
from pathlib import Path
from subprocess import Popen, PIPE, CalledProcessError
import pandas as pd
import pyarrow as pa
import yaml
from fmu.ensemble import EnsembleSet
# from ecl2df.summary import _df2pyarrow
from fmu.dataio import ExportData
from fmu.config.utilities import yaml_load
# from fmu.sumo.uploader.scripts.sumo_upload import sumo_upload_main
# from pyarrow.lib import ArrowNotImplementedError


logging.basicConfig(level="DEBUG")
LOGGER = logging.getLogger(__name__)


def remove_duplicates(frame):
    """removes duplicate columns in a pandas dataframe
    args:
      frame (pd.DataFrame): dataframe to analyse
      returns dup_free_frame (pd.DataFrame): dataframe without duplicates
     """
    frame_cols = sorted(frame.columns.values.tolist())
    frame = frame[sorted(frame_cols)]
    LOGGER.debug(frame_cols)
    LOGGER.debug("-------------")
    col_set = set(frame_cols)
    LOGGER.debug(sorted(list(col_set)))
    frame_col_len = len(frame_cols)
    col_set_len = len(col_set)
    LOGGER.debug("Length of all cols is %i ", frame_col_len)
    LOGGER.debug("Nr of uniques is %i", col_set_len)
    if frame_col_len > col_set_len:

        LOGGER.debug("A difference of %i in length", frame_col_len-col_set_len)
    dup_free_frame = frame.loc[:, ~frame.columns.duplicated()]
    return dup_free_frame


class AggExporter(ExportData):

    """Exporter of aggregated data"""
    def __init__(self, casepath, config_file):
        LOGGER.debug("Reading %s", config_file)
        self._real_ids = None

        config = yaml_load(config_file)
        del config["fmu"]["realization"]
        fmu_dict = config["fmu"]
        self.fmu_meta = config
        config.update(fmu_dict)
        del config["fmu"]
        # del config["realization"]
        LOGGER.debug(config.keys())
        for key in config:
            LOGGER.debug("%s : %s", key, config[key])
        # exit()
        super().__init__(config=config, content="timeseries",
                         casepath=casepath)

        self.case = casepath
        LOGGER.debug(self._metadata)

    @property
    def case(self):
        """Returns _case attribute
        """
        return self._case

    @case.setter
    def case(self, path):
        """Sets _case attribute"""
        self._case = Path(path).name

    @property
    def real_ids(self):
        """Returns _real_ids attribute"""
        return self._real_ids

    @real_ids.setter
    def real_ids(self, real_ids):
        """Sets the _real_ids attribute"""
        self._real_ids = real_ids
        self._fmu_meta["aggregation"]["realization_ids"] = real_ids

    @property
    def ens_tag(self):
        """returns the _ens_tag attribute"""
        return self._ens_tag

    @ens_tag.setter
    def ens_tag(self, ens_tag):
        """Sets the ens tag attribute"""
        self._ens_tag = ens_tag

    @property
    def fmu_meta(self):
        """Returns attribute _fmu_meta"""
        return self._fmu_meta

    @fmu_meta.setter
    def fmu_meta(self, model_meta):
        """Sets metadata to be used later for fmu"""

        self._fmu_meta = model_meta["fmu"]
        self._fmu_meta["aggregation"] = {"operation": "collection",
                                         "realization_ids": self.real_ids}
        self._fmu_meta["workflow"] = {"reference": "eclipse"}
        del self._fmu_meta["context"]

    def export_and_fix(self, obj, name):
        """Exports object, and adds what is missing in metadata
        args:
             obj (object): what is to be exported
             name (str): name of object to be exported
             workflow (str): name of workflow to be included
        """
        self.generate_metadata(obj, name=name)
        self._metadata["name"] = name
        # LOGGER.debug(self._metadata)
        # exit()
        exp_file = self.export(obj, name=name, tagname=self.ens_tag)
        LOGGER.debug("exported %s", exp_file)
        fix_meta(exp_file, self.fmu_meta)


def find_meta_file(path):

    """Finds all metadatafiles
    args:
    path (str) : path to ert case
    """
    LOGGER.debug("Finding meta files in %s", path)
    all_metas = Path(path).glob("realization-*/iter-*/share/results/tables/*.yml")
    LOGGER.debug(all_metas)
    first_meta = next(all_metas)
    LOGGER.debug(first_meta)
    return first_meta


def make_vectorframe(ensemble_path, save_path=None):
    """Makes pandas dataframe with vectors from eclipse
       args:
          case_path (str): path to existing ert case
          save_path (str): path to csv file, just used for debugging
       returns ens_vectors (pd.Dataframe): resulting vectors
    """

    ens_set = EnsembleSet("aggregation_case", frompath=ensemble_path)
    ens_vectors = remove_duplicates(ens_set.get_smry())
    LOGGER.debug(ens_vectors)
    if save_path is not None:
        ens_vectors.to_csv(save_path, index=False)
    return ens_vectors


def read_vectorframe(csv_path):
    """Reads pandas dataframe from file used for debugging

    args:
        csv_path (str): path to file
    """
    ens_vectors = pd.read_csv(csv_path)
    return ens_vectors


def export_aggregated(dframe, exporter):
    """exports a dataframe through arrow
        args:
        dframe (pd.DataFrame): the dataframe to export
        exporter (AggExporter): class to export with
    """
    export_arrow(dframe, "grand_summary", exporter)


def split_sum(dframe, exporter, aggregate=False):
    """splits a dataframe into separate columns
    args:
    dframe (pd.DataFrame): the dataframe to be split
    exporter (AggExporter): class to export with
    """
    neccessaries = ["REAL"]
    unneccessaries = ["YEARS", "SECONDS", "ENSEMBLE"]

    it_ids = dframe.ENSEMBLE.unique().tolist()
    for it_id in it_ids:
        it_frame = dframe.loc[dframe.ENSEMBLE == it_id]
        exporter.real_ids = it_frame.REAL.unique().tolist()
        exporter.ens_tag = it_id
        count = 0
        if aggregate:
            export_aggregated(it_frame, exporter)
        for col_name in dframe:
            if col_name in (neccessaries + unneccessaries):
                continue

            LOGGER.info("Creation of file for %s", col_name)
            keep_cols = neccessaries + [col_name]
            pd_frame = dframe[keep_cols]
            export_arrow(pd_frame, col_name, exporter)
            count += 1

        LOGGER.info("%s files produced", count)


def arrow_table(frame):
    """Exports arrow file from pandas dataframe
    args
    frame (pd.DataFrame):
    # file_name (str): name of file

    """
    schema = pa.Schema.from_pandas(frame)
    table = pa.Table.from_pandas(frame, schema=schema)
    return table


def export_arrow(frame, name, exporter):
    """Exports dataframe to arrow
    args:
    frame (pd.DataFrame): the dataframe with the results
    exporter (AggExporter): the vehicle for exporting
    tag (str): extra tag to include
    """

    LOGGER.debug(frame)
    LOGGER.debug(frame.dtypes)
    arr_table = arrow_table(frame)
    exporter.export_and_fix(arr_table, name=name)


def check_meta(metadata):
    """Checks components of dictionary, two levels
    args:
       metadata (dict): the metadata
    """
    LOGGER.debug(metadata.keys())
    for main_key in metadata:
        LOGGER.debug("%s: %s", main_key, metadata[main_key])


def fix_meta(export_path, fmu_dict):
    """Fixes metadata in file
    export_path (str): path to exported file
    fmu_dict (dict): dictionary with part that needs to be added
    """
    path = Path(export_path)

    meta_path = Path(path.parent) / ("." + path.name + ".yml")

    metadata = yaml_load(meta_path)
    metadata["fmu"] = fmu_dict
    # check_meta(metadata)

    with open(meta_path, "w", encoding="utf-8") as methandle:
        yaml.dump(metadata, methandle)

    LOGGER.debug("%s modified", meta_path)


def command_runner(command):
    """Runs unix command
    args:
    command (list): list where every part of a command is an entry
    background (bool): request for feedback, note that when this is
                       set to true it will also effectively block
                       the process, i.e. it can not run as a background
                       process
    """
    LOGGER.info('Call to Popen: %s', " ".join(command))
    try:
        with Popen(command, stdout=PIPE, stderr=PIPE) as process:

            stdout, stderr = process.communicate()
            if stdout:
                LOGGER.debug(stdout.decode("utf-8"))

            if stderr:
                LOGGER.warning(stderr.decode("utf-8"))

    except CalledProcessError:
        LOGGER.warning('Could not run command %s', ' '.join(command))


def upload_to_sumo(*command):
    """Runs command line commands
    args
    command (list): list of commands
    """
    command_runner(["sumo_upload"] + list(command))


def parse_args():
    """Parses the arguments required
    returns:
    args: arguments as name space
    """
    form_class = argparse.ArgumentDefaultsHelpFormatter
    description = (
        "Splits aggregated summary vectors into 'single files' "
    )
    parser = argparse.ArgumentParser(
        formatter_class=form_class, description=description
    )
    parser.add_argument(
        "scratch_path", type=str,
        help="path scratch ensemble"
    )
    parser.add_argument(
        "vector_filter", type=str,
        help="filter on vectors"
    )

    # parser.add_argument(
    #     "global_var_path", type=str,
    #     help="path to one global variables yaml file"
    # )
    parser.add_argument("-env", help="What sumo environment to upload to",
                        type=str, default="prod")
    parser.add_argument("-keep_aggregated", help="Keep large aggregated file",
                        action="store_true")

    parser.add_argument("-d", help="debug mode", action="store_true")

    args = parser.parse_args()

    LOGGER.debug(args)
    return args


def main():

    """Extracts the vectors, splits"""

    args = parse_args()

    exporter = AggExporter(args.scratch_path,
                           find_meta_file(args.scratch_path))
    if args.d:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    LOGGER.setLevel(log_level)
    ens_vectors = make_vectorframe(args.scratch_path)

    # ens_vectors.head().to_csv("delme.csv", index=False)
    # exit()
    split_sum(ens_vectors, exporter)
    if args.keep_aggregated:
        export_aggregated(ens_vectors, exporter)

    # sumo_upload_main(args.scratch_path, "share/results/tables/*.arrow", "dev",
    #                  "share/results/tables/", 6)

    upload_to_sumo(args.scratch_path,
                   f"{args.scratch_path}/share/results/tables/*.arrow", args.env)

    LOGGER.info("All done splitting!")


if __name__ == "__main__":
    main()
