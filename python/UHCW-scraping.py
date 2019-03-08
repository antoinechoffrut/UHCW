#!/usr/bin/env python3
"""
Goal of this script:
- go to UHCW online appointment portal
- go through categories "adult" and "child"
- go through each appointment center
- collect all remaining available appoint date and times

NOTE: this script was forked on 15 Jan 2019 10:25 from
/Users/antoine/Documents/projects/UHCW-appointments/UHCW-scraping.py
"""

import os
# import sys
# import time
import datetime
import pytz
import re
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd


# A FEW GLOBAL VARIABLES

# URL of point of entry of UHCW online appointment system
DOMAIN = "https://www.swiftqueue.co.uk"

# METADATA ON THIS SCRIPT
THIS_SCRIPT_NAME = os.path.basename(__file__)
THIS_SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# DIRECTORY WHERE RUNS ARE TO BE SAVED
HOME_DIR = os.path.expanduser("~")
PROJECT_DIR = os.path.join(
    HOME_DIR,
    "Projects/UHCW"
)
SCRIPT_DIR = os.path.join(
    PROJECT_DIR,
    "python"
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

# DATETIME FORMATS
FMT_TS = '%Y-%m-%d %H:%M'    # for timestamps
FMT_FN = '%Y-%m-%d-%H-%M-%S'    # for filenames
FMT_LOG = '%a %d %b %Y %H:%M:%S %Z%z'  # for logging

# TIMEZONES
TZ_UTC = pytz.timezone("UTC")
TZ_LONDON = pytz.timezone("Europe/London")


# LOGFILE FOR THIS SCRIPT (note: more logging in main function)
SCRIPT_LOG = re.sub(".py$", ".log", THIS_SCRIPT_NAME)
SCRIPT_LOG = os.path.join(THIS_SCRIPT_DIR, SCRIPT_LOG)
# Logger to record when this script is run
# (Details of run handled by a different logger, see logger below)
SCRIPT_LOGGER = logging.getLogger("script_logger")   # __name__
SCRIPT_LOGGER.setLevel(logging.DEBUG)
SCRIPT_FILE_HANDLER = logging.FileHandler(SCRIPT_LOG)
SCRIPT_FILE_FORMATTER = logging.Formatter(
    '%(asctime)s - %(levelname)-10s: %(message)s'
)
SCRIPT_FILE_HANDLER.setFormatter(SCRIPT_FILE_FORMATTER)
SCRIPT_LOGGER.addHandler(SCRIPT_FILE_HANDLER)

# FUNCTIONS


def get_age_group_url(age_group):
    """Returns URL of page for given age group."""
    url = "".join([DOMAIN,
                   "/uhcw.php?group=Coventry_",
                   age_group])
    return url


def get_center_soups(page):
    """Returns list containing responses for different centers from
    given age group page."""
    soup = BeautifulSoup(page.text, 'html.parser')
    centers = soup.find_all(
        "div",
        {"class": "search-result row"})
    return centers


def collect_center_info(center, age_group):
    """Returns a dictionary with center information."""
    center_geo_info = center.find_all(
        "div",
        {"class": "search-result-content col-md-3 col-xs-6"},
        limit=1)
    center_name = center_geo_info[0].find_all("h3", limit=1)[0].text
    center_web = center_geo_info[0].find_all("a", limit=1)[0]['href']
    center_latlon = re.sub(
        "\'",
        "",
        re.sub("goToAddress",
               "",
               center_geo_info[0].find_all("button")[0]['onclick']))
    center_latitude = re.sub(
        r',.*',
        "",
        re.sub(
            r'.*\(',
            "",
            center_latlon
        )
    ).strip()
    center_longitude = re.sub(
        r'\).*',
        "",
        re.sub(
            r'.*,',
            "",
            center_latlon
        )
    ).strip()

    center_address = re.sub(
        "\n",
        "",
        center_geo_info[0].find_all("button")[0].text.strip()
    )
    center_appts_href = center.find_all(
        "div",
        {"class":
         "search-result-content col-md-3 col-xs-12 col-lg-4"},
        limit=1)[0].find_all(
            "a",
            limit=1)[0]['href']
    center_id = re.sub(r".*=", "", center_appts_href)
    center_appts_url = "/".join([DOMAIN, center_appts_href])
    return {
        'center age group': age_group,
        'center id': center_id,
        'center name': center_name,
        'center web': center_web,
        'center latitude': center_latitude,
        'center longitude': center_longitude,
        'center address': center_address,
        'center appointments href': center_appts_href,
        'center appointments url': center_appts_url
    }


def extract_center_test_types(page):
    """Get href's for all test types for center whose page is specified."""
    result = []
    soup = BeautifulSoup(page.text, 'html.parser')
    block = soup.find_all(
        "div",
        {"class": "container text-center"})[0]
    test_types = block.find_all(
        "div",
        {"class": "col-sm-8"}
    )
    for test_type in test_types:
        test_type_href = test_type.find_all("a")[0]['href']
        test_type_type = test_type.find_all("h4")[0].text.strip()
        if test_type_type != "Cancel Appointment":
            result.append({
                'type': test_type_type,
                'href': test_type_href,
            })

    return result


def extract_appointments(page):
    """Extract appointment timestamps from page for given center and
    test type."""
    result = pd.DataFrame(columns=["appointment timestamp"])
    soup = BeautifulSoup(page.text, 'html.parser')
    timescreen = soup.find_all("div", {"id": "timescreen"})[0]
    timescreen_days = timescreen.find_all(
        "div",
        {"class": "timescreen-day"}
    )
    for timescreen_day in timescreen_days:
        the_date = timescreen_day.get('data-date')
        the_times = [item.get('data-time')
                     for item in timescreen_day.find_all("li")]
        timestamps = [
            # "-".join([the_date, re.sub(":", "-", the_time)])
            " ".join([the_date, the_time])
            for the_time in the_times]
        df_ts = pd.DataFrame(timestamps, columns=result.columns)
        result = result.append(df_ts)
    return result


def OLD_init_run(data_dir=DATA_DIR, script_logger=SCRIPT_LOGGER):
    """Creates subdirectory to save data for current run.
    Returns a tuple with:
    - run_time: date and time (UTC standard);
    - run_dir: full path of subdirectory;
    - run_log: log filename for this run;
    - run_logger: logger for this run.
    """
    # CLOCK IN
    run_time = TZ_UTC.localize(datetime.datetime.now())

    # DIRECTORY FOR RUN
    run_dir = os.path.join(
        data_dir,
        run_time.strftime("-".join(["DATA", FMT_FN]))
    )
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        script_logger.warning(
            "Directory \"%s\" created."
            # % (data_dir.split("/")[-2])
            % data_dir
        )

    if not os.path.exists(run_dir):
        os.makedirs(run_dir)
        script_logger.debug(
            "Directory \"%s\" created."
            # % (run_dir.split("/")[-2])
            % run_dir
        )

    # LOGGER FOR RUN
    # log filename
    run_log = os.path.join(
        run_dir,
        "".join([
            run_time.strftime(
                "-".join(["logfile", FMT_FN])
            ),
            ".log"
        ])
    )
    # time.strftime("logfile-%Y-%m-%d-%H-%M-%S", run_time)
    # create logger
    run_logger = logging.getLogger("run_logger")    # __name__
    run_logger.setLevel(logging.DEBUG)

    # create console and filer handlers
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # ch.setLevel(logging.DEBUG)
    fh = logging.FileHandler(run_log)
    fh.setLevel(logging.DEBUG)

    # create formatters
    cf = logging.Formatter()
    ff = logging.Formatter('%(asctime)s - %(levelname)-10s: %(message)s')

    # add formatter to console and file handlers
    ch.setFormatter(cf)
    fh.setFormatter(ff)

    # add console and file handlers to logger
    run_logger.addHandler(ch)
    run_logger.addHandler(fh)

    return run_time, run_dir, run_log, run_logger


def init_run(runs_dir=RUNS_DIR, script_logger=SCRIPT_LOGGER):
    """Creates subdirectory to save data for current run.
    Inputs:
    - runs_dir: name of directory containing all subdirectories
      containing data for each run
    - script_logger: logger for this script
    Returns a tuple with:
    - run_time: date and time (UTC standard);
    - run_dir: name (full path) of directory for this run
    - run_log: log filename for this run;
    - run_logger: logger for this run.
    """
    # CLOCK IN
    run_time = TZ_UTC.localize(datetime.datetime.now())

    # DIRECTORY FOR THIS RUN
    run_dir = os.path.join(
        runs_dir,
        run_time.strftime("-".join(["RUN", FMT_FN]))
    )

    if not os.path.exists(run_dir):
        os.makedirs(run_dir)
        script_logger.debug(
            "Directory \"%s\" created."
            # % (run_dir.split("/")[-2])
            % run_dir
        )

    # LOGGER FOR RUN
    # log filename
    run_log = os.path.join(
        run_dir,
        "".join([
            run_time.strftime(
                "-".join(["logfile", FMT_FN])
            ),
            ".log"
        ])
    )
    # time.strftime("logfile-%Y-%m-%d-%H-%M-%S", run_time)
    # create logger
    run_logger = logging.getLogger("run_logger")    # __name__
    run_logger.setLevel(logging.DEBUG)

    # create console and filer handlers
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # ch.setLevel(logging.DEBUG)
    fh = logging.FileHandler(run_log)
    fh.setLevel(logging.DEBUG)

    # create formatters
    cf = logging.Formatter()
    ff = logging.Formatter('%(asctime)s - %(levelname)-10s: %(message)s')

    # add formatter to console and file handlers
    ch.setFormatter(cf)
    fh.setFormatter(ff)

    # add console and file handlers to logger
    run_logger.addHandler(ch)
    run_logger.addHandler(fh)

    return run_time, run_dir, run_log, run_logger


def save_center_info(appt_centers, run_dir, run_time):
    """Save dataframe with center information as .csv file in
    appropriate location, returns path of .csv file."""
    appt_centers_path = os.path.join(
        run_dir,
        run_time.strftime(
            ".".join(
                ["-".join(["centers", FMT_FN]), "csv"]
            )
            )
        )

    appt_centers.to_csv(
        appt_centers_path,
        sep=";",
        index=False)
    os.chmod(appt_centers_path, 0o444)  # make it read-only
    return appt_centers_path


def save_appointments(appointments, run_dir, run_time):
    """Save dataframe with appointments as .csv file in appropriate
    location, returns path of .csv file."""
    # appointments_path = os.path.join(
    #     run_dir,
    #     "".join([
    #         time.strftime("appointments-%Y-%m-%d-%H-%M-%S", run_time),
    #         ".csv"]),
    #     )
    appointments_path = os.path.join(
        run_dir,
        run_time.strftime(
            ".".join(
                ["-".join(["appointments", FMT_FN]), "csv"]
            )
            )
        )

    appointments.to_csv(
        appointments_path,
        sep=";",
        index=False)
    os.chmod(appointments_path, 0o444)  # make it read-only
    return appointments_path


def get_age_group_centers(age_group, run_logger):
    """Return list of centers for specified age group."""
    age_group_url = get_age_group_url(age_group)
    run_logger.info("Grabbing age group page...")
    age_group_page = requests.get(age_group_url)

    if age_group_page.status_code == 200:
        run_logger.info("Done (status code: %d).", age_group_page.status_code)
    else:
        run_logger.warning(
            "Could not download page for age group \"%s\"",
            age_group)
        run_logger.debug("Status code: %d" % age_group_page.status_code)
        run_logger.debug("URL: %s", age_group_url)
        return None
    all_centers = get_center_soups(age_group_page)
    return all_centers

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# WARNING: THE FUNCTION collect_center_info IS CALLED A SECOND TIME IN
# BY THE FOLLOWING FUNCTION, THIS NEEDS TO BE REARRANGED
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


def get_center_test_types(center, age_group, run_logger):
    """Returns list of test types."""
    center_info = collect_center_info(center, age_group)
    page_center = requests.get(center_info['center appointments url'])
    if page_center.status_code == 200:
        run_logger.debug(" ... done.")
    else:
        run_logger.warning(
            "Could not download page for age group: %s",
            age_group)
        run_logger.warning("URL: %s", center_info['center appointments url'])
        run_logger.info("")
        return None
    test_types = extract_center_test_types(page_center)
    return test_types


def main():
    """Script collecting appointment times from University Hospital
    Coventry and Warwikcishire online appointment web portal:
    https://www.swiftqueue.co.uk/uhcw.php
    """
    SCRIPT_LOGGER.info("-"*60)
    SCRIPT_LOGGER.info("New run of script {}".format(THIS_SCRIPT_NAME))
    SCRIPT_LOGGER.info("Directory of script: {}".format(THIS_SCRIPT_DIR))
    # SCRIPT_LOGGER.debug("Process id: %d." % os.getpid())
    if not (THIS_SCRIPT_DIR == SCRIPT_DIR):
        SCRIPT_LOGGER.warning(
            "This script is NOT in the correct directory: {}".format(
                PROJECT_DIR
            )
        )

    # INITIALIZATION
    run_time, run_dir, run_log, run_logger = \
        init_run()
    # SCRIPT_LOGGER.info("Logfile for this script: {}".format(SCRIPT_LOG))
    SCRIPT_LOGGER.info("Logfile for this run: {}".format(run_log))

    # DATAFRAMES:
    # FOR CENTER INFORMATION
    appt_centers = pd.DataFrame(
        columns=[
            'grab timestamp',
            'center age group',
            'center id',
            'center name',
            'center web',
            'center latitude',
            'center longitude',
            'center address',
            'center appointments url'
        ]
    )
    # FOR APPOINTMENTS
    appointments = pd.DataFrame(
        columns=[
            # 'grab timestamp', # add after concat over age groups
            'center age group',
            'center id',
            'test type',
            'appointment timestamp',
        ]
    )

    ########################################
    # SOME DISPLAY
    os.system('clear')

    run_logger.info("~"*60)
    # run_logger.info(time.strftime("%a %d %b %Y %H:%M:%S", run_time))
    run_logger.info(
        # run_time.strftime(FMT_LOG)
        run_time.astimezone(TZ_LONDON).strftime(FMT_LOG)
    )
    run_logger.info("Running script %s" % THIS_SCRIPT_NAME)
    run_logger.debug("Directory: %s" % THIS_SCRIPT_DIR)
    run_logger.info("Logfile: %s", run_log)
    run_logger.info("~"*60)

    # VISIT AGE GROUPS
    for age_group in ["adult", "child"]:
        run_logger.info("-"*60)
        run_logger.info("Age group: \"%s\"" % age_group)
        run_logger.info("-"*60)
        # DATAFRAME FOR ALL APPOINTMENTS FOR THIS AGE GROUP
        df_age_group = pd.DataFrame(
            columns=[
                # 'center age group', # add after concat over centers
                'center id',
                'test type',
                'appointment timestamp',
            ]
        )
        # GET CENTERS FOR THIS AGE GROUP
        all_centers = get_age_group_centers(age_group, run_logger)
        if all_centers is None:
            continue

        run_logger.info("There are %d centers in the age group \"%s\".",
                        len(list(all_centers)),
                        age_group)

        # IN EACH AGE GROUP, VISIT ALL CENTERS
        for center in all_centers:
            run_logger.info("Center %d of %d",
                            (all_centers.index(center) + 1),
                            len(list(all_centers)))
            # DATAFRAME FOR ALL APPOINTMENTS FOR THIS CENTER
            df_center = pd.DataFrame(
                columns=[
                    # 'center id', # add after concat over types
                    'test type',
                    'appointment timestamp',
                ]
            )
            run_logger.info("> Collecting center info...")
            # GET CENTER INFO
            center_info = collect_center_info(center, age_group)
            # ADD INFO FOR THIS CENTER TO DATAFRAME
            appt_centers.loc[len(appt_centers)] = center_info
            for key in center_info.keys():
                run_logger.debug(
                    ' {0:<30}: {1}'.format(
                        key,
                        center_info[key]
                    )
                )

            run_logger.info("> Collecting test types...")
            test_types = get_center_test_types(
                center,
                age_group,
                run_logger)
            if test_types is None:
                continue
            run_logger.debug(
                "  Number of test types: %d" % len(list(test_types))
            )
            run_logger.debug(test_types)

            # FOR EACH CENTER, VISIT ALL TEST TYPES
            for test_type in test_types:
                run_logger.info("  + test type: %s (%d of %d):",
                                test_type['type'],
                                (test_types.index(test_type) + 1),
                                len(test_types),
                                )
                # Dataframe for all appointments for this test type
                df_type = pd.DataFrame(
                    columns=[
                        # 'test type', # add after timestamps collected
                        'appointment timestamp',
                    ]
                )
                # Grab web page
                test_type_page_url = "/".join([
                    DOMAIN,
                    test_type['href']
                ])
                run_logger.debug("    Grabbing appointments page...")
                test_type_page = requests.get(test_type_page_url)
                # Check connection
                if test_type_page.status_code == 200:
                    run_logger.debug(
                        "    Done (status code: %d).",
                        test_type_page.status_code
                    )
                else:
                    run_logger.warning(
                        "  Could not download page for test type %s",
                        test_type['type']
                    )
                    run_logger.warning(
                        "  Status code: %d.",
                        test_type_page.status_code
                    )
                    continue
                run_logger.debug("    Extracting appointment timestamps...")
                df_type = extract_appointments(test_type_page)
                df_type['test type'] = test_type['type']
                run_logger.debug("    Done.")
                run_logger.info(
                    "    Extracted %d timestamps for %d days",
                    df_type['appointment timestamp'].nunique(),
                    df_type['appointment timestamp'].apply(
                        lambda t: "-".join(t.split("-")[0:3])).nunique()
                )
                run_logger.debug(
                    "    Appending type appts to center df...")
                df_center = df_center.append(
                    df_type,
                    ignore_index=True,
                    sort=True
                )
            df_center['center id'] = center_info['center id']
            run_logger.debug("   Appending center appts to age group df...")
            df_age_group = df_age_group.append(
                df_center,
                ignore_index=True,
                sort=True
            )
        df_age_group['center age group'] = age_group
        run_logger.debug("  Appending age group appts to master appts df...")
        appointments = appointments.append(
            df_age_group,
            ignore_index=True,
            sort=True
        )

    appointments['grab timestamp'] = run_time.strftime(FMT_TS)

    # Add grab timestamp
    appt_centers['grab timestamp'] = run_time.strftime(FMT_TS)

    # Save appointment centers information to .csv file
    appt_centers_path = save_center_info(
        appt_centers,
        run_dir,
        run_time
    )

    # Save appointments to .csv file
    appointments_path = save_appointments(
        appointments,
        run_dir,
        run_time)

    # CONCLUDING SCRIPT
    # end_of_script = time.time()
    end_of_script = TZ_UTC.localize(datetime.datetime.now())

    run_logger.info("")
    run_logger.info("End of run.")
    run_logger.info(
        "Timestamp: {}".format(
            end_of_script.astimezone(TZ_LONDON).strftime(FMT_LOG)
        )
    )
    run_logger.info(
        "Duration: {} sec".format((end_of_script - run_time).seconds)
    )

    # run_logger.info(
    #     "Time stamp: %s duration: %.2f sec)",
    #     # time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(end_of_script)),
    #     end_of_script.strftime("%a %d %b %Y %H:%M:%S"),
    #     (end_of_script - run_time).seconds)
    run_logger.info("Logfile for this run: {}".format(run_log))
    run_logger.info(
        "End of script {}".format(THIS_SCRIPT_NAME)
    )

    SCRIPT_LOGGER.info("End of script {}".format(THIS_SCRIPT_NAME))


if __name__ == "__main__":
    main()
