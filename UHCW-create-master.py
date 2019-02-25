#!/usr/bin/env python3
"""
Generate UHCW dataset by aggregating separate run files.

Generate appointments.csv file containing the data from all .csv files
(corresponding to all the runs).  The data consists of the appointment
times for the UHCW project.

Save a backup file with timestamp in ~/Data/UHCW/MASTER_BACKUPS.

"""
import os
import glob
import logging
import datetime
import pytz
import shutil
import re

SCRIPT_NAME = os.path.basename(__file__)
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

HOME_DIR = os.path.expanduser("~")
PROJECT_DIR = os.path.join(
    HOME_DIR,
    "Projects/UHCW"
)
DATA_DIR = os.path.join(
    HOME_DIR,
    "Data",
    "UHCW"
)
RUNS_DIR = os.path.join(
    DATA_DIR,
    "RUNS"
)
RUN_DIR_PREFIX = "RUN-"

MASTER_BACKUP_DIR = os.path.join(
    DATA_DIR,
    "MASTER_BACKUPS"
)

# LOGGING
LOGFILE = re.sub(".py$", ".log", SCRIPT_NAME)
LOGFILE = os.path.join(SCRIPT_DIR, LOGFILE)
# Logger to record when this script is run
LOGGER = logging.getLogger("script_logger")
LOGGER.setLevel(logging.DEBUG)

CONSOLE_HANDLER = logging.StreamHandler()
CONSOLE_HANDLER.setLevel(logging.INFO)
CONSOLE_FORMATTER = logging.Formatter()
CONSOLE_HANDLER.setFormatter(CONSOLE_FORMATTER)
LOGGER.addHandler(CONSOLE_HANDLER)


FILE_HANDLER = logging.FileHandler(LOGFILE)
FILE_FORMATTER = logging.Formatter(
    '%(asctime)s - %(levelname)-10s: %(message)s'
)
FILE_HANDLER.setFormatter(FILE_FORMATTER)
LOGGER.addHandler(FILE_HANDLER)


# DATETIME FORMATS
FMT_TS = '%Y-%m-%d %H:%M'    # for timestamps
FMT_FN = '%Y-%m-%d-%H-%M-%S'    # for filenames
FMT_LOG = '%a %d %b %Y %H:%M:%S %Z%z'  # for logging

# TIMEZONES
TZ_UTC = pytz.timezone("UTC")
TZ_LONDON = pytz.timezone("Europe/London")


def main():
    """Aggregate data from runs into a single CSV file.

    - Check whether this script is located in expected folder.
    - Generate list of all run directories.
    - Generate list of all .csv files.
    - Generate list of all .csv files with appointment details.
    - Create subdirectory MASTER_BACKUPS if not existing.
    - Read all files and copy into appointments.csv.
    - Saves a backup of appointments.csv (if exists) into
      MASTER_BACKUPS by appending timestamp to name.

    """
    run_time = TZ_UTC.localize(datetime.datetime.now())

    LOGGER.info("-"*60)
    LOGGER.info("New run of script: {}".format(SCRIPT_NAME))
    LOGGER.info(
        "Timestamp: {}".format(
            run_time.astimezone(TZ_LONDON).strftime(FMT_LOG)
        )
    )

    if not (SCRIPT_DIR == PROJECT_DIR):
        LOGGER.warning(
            "This script is NOT in the correct directory, "
            + "should be in {}".format(PROJECT_DIR)
        )

    run_dirs = [
        item + "/"
        for item
        in sorted(
            glob.glob(os.path.join(
                RUNS_DIR,
                ''.join([RUN_DIR_PREFIX, '*'])))
        )
    ]
    LOGGER.info("There are {0} runs.".format(len(run_dirs)))

    csv_filenames = [
        csv_filename
        for run_dir in run_dirs
        for csv_filename
        in (glob.glob(os.path.join(run_dir, "*.csv")))
    ]
    LOGGER.info("There are {} CSV files.".format(len(csv_filenames)))

    appt_filenames = [
        csv_filename
        for csv_filename
        in csv_filenames
        if "appointments" in csv_filename.split("/")[-1]
    ]
    LOGGER.info(
        "There are {} CSV files with appointment data.".format(
            len(appt_filenames)
        )
    )

    # MASTER FILE
    foutname = os.path.join(DATA_DIR, "appointments.csv")

    # READ IN FROM FIRST FILE, INCLUDING HEADER
    with open(foutname, "w") as master:
        with open(appt_filenames[0], "r") as fin:
            for line in fin:
                master.write(line)

    N = len(appt_filenames)
    # N = min(200, len(appt_filenames))
    # N = min(3, len(appt_filenames))
    LOGGER.info(
        "Will read {0} files and write into {1}.".format(
            N, foutname
        )
    )
    if N != len(appt_filenames):
        LOGGER.warning(
            " ".join([
                "Not all files are read,",
                "possibly as a test working on a smaller set of files."])
        )

    # READ IN FROM ALL OTHER FILES, EXCLUDING HEADER
    with open(foutname, "a") as master:
        for i in range(N):
            appt_filename = appt_filenames[i]
            if ((((100*i//N) % 10) == 0) and ((100*(i-1)//N) % 10 != 0)):
                LOGGER.info(
                    "Processing {0} of {1} ({2}%)...".format(
                        appt_filenames.index(appt_filename) + 1,
                        N,  # len(appt_filenames),
                        100*i//N
                    )
                )
            with open(appt_filename, "r") as fin:
                fin.__next__()
                for line in fin:
                    master.write(line)

    # MASTER BACKUP DIRECTORY
    if os.path.exists(MASTER_BACKUP_DIR) is False:
        LOGGER.warning(
            "Master backup directory created (did not exist): ".format(
                MASTER_BACKUP_DIR
            )
        )
        os.makedirs(MASTER_BACKUP_DIR)

    # BACKUP appointments.csv IF ALREADY EXISTS
    if os.path.exists(foutname):
        LOGGER.warning(
            "Output file {} already exists, backing it up...".format(
                foutname)
            )

        backupname = foutname.split("/")[-1]
        backupname = re.sub(
            ".csv",
            run_time.strftime("-" + FMT_FN + ".csv"),
            backupname
        )
        backupname = os.path.join(
            MASTER_BACKUP_DIR,
            backupname
        )
        shutil.copyfile(foutname, backupname)
        LOGGER.warning("Backup at {}.".format(backupname))
    else:
        print("No output file {} yet.".format(foutname))

    # WRAPPING UP LOGGING
    end_of_script = TZ_UTC.localize(datetime.datetime.now())

    LOGGER.info("")
    LOGGER.info("End of script {}".format(SCRIPT_NAME))
    LOGGER.info(
        "Timestamp: {}".format(
            end_of_script.astimezone(TZ_LONDON).strftime(FMT_LOG)
        )
    )
    LOGGER.info(
        "Duration: {0} sec to process: {1} files (average {2}sec/file)".format(
            (end_of_script - run_time).seconds,
            N,
            (end_of_script - run_time).seconds/N
        )
    )


if __name__ == "__main__":
    """This will only be executed when this module is run directly."""
    main()
