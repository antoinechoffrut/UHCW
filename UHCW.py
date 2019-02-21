#!/usr/bin/env python3
"""Module with custom functions and classes for UHCW project."""
import os
import pytz
import pandas as pd
import datetime
# import matplotlib.pyplot as plt

# CONSTANTS
tz_utc = pytz.timezone("UTC")  # timestamp is in UTC standard
tz_london = pytz.timezone("Europe/London")  # test centers are in Coventry, UK

IMG_DIR = os.path.join(os.path.expanduser("~"), "Projects/UHCW/IMAGE_FILES")

# --------------------
# Functions
# --------------------
# Generic dataframe operations


def prepare_UHCW_dataframe(raw_data):
    """Convert columns of input dataframe to appropriate types.

    Parameters
    ----------

    raw_data:

    dataframe containing data from UHCW online appointment booking
    system.  Presumed to have columns 'id', 'test type', 'age group',
    'grab', 'appointment'.  Each row represents an available
    appointment at a given time (grab):
    - id: id number of test center offering appointment
    - test type: type of blood test administered
    - age group: age group servied (adult or child)
    - grab: timestamp (UTC) of collection of data
    - appointment: timestamp (local, Coventry, UK) of appointment
    """
    FMT_TIME = "%Hh%Mm%Ss"
    print("{0}: Make copy of data...".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    df = raw_data.copy()

    print("{0}: Type conversion: ".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    print("{0}: Column appointment:".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    print("{0}: Convert to datetime...".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    df['appointment'] = pd.to_datetime(df['appointment'])
    print("{0}: Convert to London timezone...".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    df['appointment'] = df['appointment'].apply(
        lambda ts: ts.tz_localize(tz_london)
    )

    print("{0}: Column grab:".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    print("{0}: Convert to datetime...".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    df['grab'] = pd.to_datetime(df['grab'])
    print("{0}: Localize to UTC and convert to London timezone...".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    df['grab'] = df['grab'].apply(
        lambda ts: ts.tz_localize(tz_utc).tz_convert(tz_london)
    )

    df = df[['id', 'test type', 'age group', 'appointment', 'grab']]

    return df


def filter_center(df, center_id, test_type):
    """Filter dataset for selected center and test type.

    Parameters
    ----------
    df:

    dataframe, assumed to have 'id' and 'test type' as column names
    center_id (int) id of desired test center, test_type (str) name of
    test type

    Returns
    -------

    filtered:

    dataframe with records from df corresponding to desired test
    center and test type

    """
    idxs = (df['id'] == center_id) & (df['test type'] == test_type)
    filtered = df.loc[idxs, :]
    return filtered


def decouple_data(schedule):
    """Create list of test centers with their test types.

    Parameters:
    ----------

    schedule:

    dataframe whose columns contain 'id', 'test type', and 'age group'

    Returns:
    -------

    center_info:

    dataframe indexed by center id, and two columns: 'age group'
    indicating age group serviced at test center, and 'test types', a
    list of test types administered at test center.

    TODO: there has to be a more natural way to extract the center info

    """

    s = schedule.copy()

    center_test_types =  \
        s.groupby('id')['test type'].unique().rename(
            "test types"
        ).to_frame()
    center_age_group = \
        s.groupby('id')['age group'].unique().to_frame()

    center_info = \
        pd.merge(
            center_test_types,
            center_age_group,
            on='id',
            how='outer'
        )

    s.drop('age group', axis=1, inplace=True)

    return s, center_info


def get_timegrid(schedule, past_appointments=True):
    """Generate grid of (grab, appointment) pairs appearing in
    dataset, excluding those with grab occurring after appointment.

    Parameters
    ----------

    schedule:

    Dataframe with columns 'id', 'test type', 'grab', 'appointment',
    recording available appointments at grab times.

    past_appointments:

    boolean, indicates whether only past appointments should be kept

    Returns - TODO: update this part
    -------

    timegrid:

    Dataframe with columns 'id', 'test type', 'grab', 'appointment',
    which, for each (id, test type) pair, contains all pairs (grab,
    appointment) where grab and appointment appearing in dataset.

    """
    print("Cartesian product grabs by appointments...")
    grabs = \
        schedule.loc[
            :, ['id', 'test type', 'grab']
        ].drop_duplicates().reset_index(drop=True)

    appointments = \
        schedule.loc[
            :, ['id', 'test type', 'appointment']
        ].drop_duplicates().reset_index(drop=True)

    t = pd.merge(
        left=grabs,
        right=appointments,
        on=['id', 'test type'],
        how='outer'
    )

    print("Ignore grabs past appointments...")
    t.query('grab <= appointment', inplace=True)

    if past_appointments:
        print("Restrict to past appointments...")
        last_grab = schedule['grab'].max()
        t.query('appointment <= @last_grab', inplace=True)

    print("Sort by id, test type, appointment, grab...")
    t.sort_values(
        ['id', 'test type', 'appointment', 'grab'],
        inplace=True
    )

    t = t[['id', 'test type', 'appointment', 'grab']]

    # Add column indicating whether appointment available or booked
    print("Add status...")
    t = pd.merge(
        left=t,
        right=schedule,
        on=['id', 'test type', 'appointment', 'grab'],
        how='left',
        indicator=True
    )

    t.rename(index=str, columns={'_merge': 'status'}, inplace=True)
    t['status'] = t['status'].apply(
        lambda ind: "booked" if ind == "left_only" else "available"
    )

    # Add columns with various groups of grabs
    print("Add extra grab columns:")
    print("{0}: grab hour...".format(
        datetime.datetime.now().strftime("%H:%M:%S")
    ))
    t['grab hour'] = \
        t['grab'].apply(lambda ts: ts.replace(minute=0, second=0))
    print("{0}: grab 3 hours...".format(
        datetime.datetime.now().strftime("%H:%M:%S")
    ))
    t['grab 3 hours'] = \
        t['grab'].apply(
            lambda ts: ts.replace(
                hour=3*(ts.hour // 3),
                minute=0,
                second=0)
        )
    print("{0}: grab day...".format(
        datetime.datetime.now().strftime("%H:%M:%S")
    ))
    t['grab day'] = t['grab'].apply(
        lambda ts: ts.replace(hour=0, minute=0, second=0)
    )

    # Add status in last hour
    status_in_hour = \
        t.groupby(
            ['id', 'test type', 'appointment', 'grab hour']
        )['status'].apply(
            lambda group: "booked" in list(group)
        ).reset_index()

    status_in_hour['status'] = \
        status_in_hour['status'].apply(
            lambda status: "booked" if status else "available"
        )
    status_in_hour.rename(
        index=str,
        columns={'status': 'status in last hour'},
        inplace=True
    )

    t = pd.merge(
        left=t,
        right=status_in_hour,
        on=['id', 'test type', 'appointment', 'grab hour'],
        how='left'
    )

    # Add status in last 3 hours
    status_in_3_hours = \
        t.groupby(
            ['id', 'test type', 'appointment', 'grab 3 hours']
        )['status'].apply(
            lambda group: "booked" in list(group)
        ).reset_index()
    status_in_3_hours['status'] = \
        status_in_3_hours['status'].apply(
            lambda status: "booked" if status else "available"
        )
    status_in_3_hours.rename(
        index=str,
        columns={'status': 'status in last 3 hours'},
        inplace=True
    )

    t = pd.merge(
        left=t,
        right=status_in_3_hours,
        on=['id', 'test type', 'appointment', 'grab 3 hours'],
        how='left'
    )

    # Add status in last day
    status_in_day = \
        t.groupby(
            ['id', 'test type', 'appointment', 'grab day']
        )['status'].apply(
            lambda group: "booked" in list(group)
        ).reset_index()

    status_in_day['status'] = \
        status_in_day['status'].apply(
            lambda status: "booked" if status else "available"
        )
    status_in_day.rename(
        index=str,
        columns={'status': 'status in last day'},
        inplace=True
    )

    t = pd.merge(
        left=t,
        right=status_in_day,
        on=['id', 'test type', 'appointment', 'grab day'],
        how='left'
    )

    # Add final status

    status_final = \
        t.groupby(
            ['id', 'test type', 'appointment']
        )['status'].apply(lambda group: list(group)[-1]).reset_index()

    status_final.rename(
        index=str, columns={'status': 'final status'},
        inplace=True
    )

    # calculate last grabs
    t_last_grab = t[
        ['id', 'test type', 'appointment', 'grab']
    ].groupby(
        ['id', 'test type', 'appointment']
    )['grab'].max().reset_index().rename(
        index=str,
        columns={'grab': 'last grab'}
    )

    # include last grabs
    status_final = pd.merge(
        left=status_final,
        right=t_last_grab,
        on=['id', 'test type', 'appointment'],
        how='left'
    )

    t = pd.merge(
        left=t,
        right=status_final,
        on=['id', 'test type', 'appointment'],
        how='left'
    )

    t.rename(
        index=str,
        columns={'status': 'status at grab'},
        inplace=True
    )
    t = t[[
        'id',
        'test type',
        'appointment',
        'grab',
        'status at grab',
        'grab hour',
        'status in last hour',
        'grab 3 hours',
        'status in last 3 hours',
        'grab day',
        'status in last day',
        'last grab',
        'final status'
    ]]

    return t


# def get_final_status(timegrid):
#     """Extract status of appointment at last grab."""
#
#     # Calculate last grab for each appointment
#     final_status = \
#         timegrid.groupby(
#             ['id', 'test type', 'appointment']
#         )['grab'].max().to_frame().reset_index()
#
#     # Restore action values (TODO: can this extra step be avoided?)
#     final_status = pd.merge(
#         left=final_status,
#         right=timegrid[['id', 'test type', 'appointment', 'grab', 'status']],
#         on=['id', 'test type', 'appointment', 'grab'],
#         how='left',
#         # indicator=True
#     )
#     return final_status


# def OLD_get_occupancy(final_status):
#     counts = final_status.groupby(
#         ['id', 'test type']
#     )['status'].value_counts().to_frame().rename(
#         index=str,
#         columns={'status': 'count'}
#     ).reset_index()
#
#     rates = \
#         pd.pivot_table(
#             counts,
#             values='count',
#             index=['id', 'test type'],
#             columns=['status']
#         ).fillna(0).astype(int)
#     rates['rate'] = \
#         (100*rates['booked']) // (rates['available'] + rates['booked'])
#
#     return rates

def get_occupancy(timegrid):

    occupancy = timegrid.copy()

    # Overall occupancy

    print("Calculate overall occupancy rate:")
    print("Calculate counts...")
    counts = occupancy.groupby(
        ['id', 'test type']
    )['final status'].value_counts().to_frame().rename(
        index=str,
        columns={'final status': 'count'}
    ).reset_index()

    print("Pivot table...")
    rates = \
        pd.pivot_table(
            counts,
            values='count',
            index=['id', 'test type'],
            columns=['final status']
        ).fillna(0).astype(int)

    # In case all "booked" or all "available", add column of 0s
    for status in ['booked', 'available']:
        if not(status in rates.columns):
            rates[status] = 0
    print("Calculate rates...")
    rates['rate'] = \
        (100*rates['booked']) \
        // (rates['available'] + rates['booked'])
    rates.reset_index(inplace=True)
    rates['id'] = rates['id'].astype(int)

    print("Merge rates with occupancy...")
    occupancy = pd.merge(
        left=occupancy,
        right=rates[['id', 'test type', 'rate']],
        on=['id', 'test type'],
        how='left'
    )
    occupancy.rename(
        index=str,
        columns={'rate': 'overall occupancy rate'},
        inplace=True
    )

    # daily occupancy rates

    print("Calculate daily occupancy rates:")
    print("Calculate appointment day...")
    occupancy['appointment day'] = occupancy['appointment'].apply(
        lambda ts: ts.replace(hour=0, minute=0, second=0)
    )
    print("Calculate daily counts...")
    counts_daily = occupancy.groupby(
        ['id', 'test type', 'appointment day']
    )['final status'].value_counts().to_frame().rename(
        index=str,
        columns={'final status': 'count'}
    ).reset_index()

    print("Pivot table of daily counts...")
    rates_daily = \
        pd.pivot_table(
            counts_daily,
            values='count',
            index=['id', 'test type', 'appointment day'],
            columns=['final status']
        ).fillna(0).astype(int)

    print("Calculate rates...")
    rates_daily['rate'] = \
        (100*rates_daily['booked']) \
        // (rates_daily['available'] + rates_daily['booked'])
    rates_daily.reset_index(inplace=True)
    rates_daily['id'] = rates_daily['id'].astype(int)
    rates_daily['appointment day'] = \
        rates_daily['appointment day'].apply(
            lambda ts: pd.to_datetime(ts).tz_localize(tz_london)
        )

    print("Merge with occupancy...")
    occupancy = pd.merge(
        left=occupancy,
        right=rates_daily[['id', 'test type', 'appointment day', 'rate']],
        on=['id', 'test type', 'appointment day'],
        how='left'
    )
    occupancy.rename(
        index=str,
        columns={'rate': 'daily occupancy rate'},
        inplace=True
    )

    # hourly occupancy rates

    print("Calculate hourly occupancy rates:")
    print("Add appointment hour...")
    occupancy['appointment hour'] = occupancy['appointment'].apply(
        lambda ts: ts.replace(minute=0, second=0)
    )
    print("Calculate hourly counts...")
    counts_hourly = occupancy.groupby(
        ['id', 'test type', 'appointment hour']
    )['final status'].value_counts().to_frame().rename(
        index=str,
        columns={'final status': 'count'}
    ).reset_index()

    print("Pivot table of hourly counts...")
    rates_hourly = \
        pd.pivot_table(
            counts_hourly,
            values='count',
            index=['id', 'test type', 'appointment hour'],
            columns=['final status']
        ).fillna(0).astype(int)

    print("Calculate hourly rates...")
    rates_hourly['rate'] = \
        (100*rates_hourly['booked']) \
        // (rates_hourly['available'] + rates_hourly['booked'])
    rates_hourly.reset_index(inplace=True)
    rates_hourly['id'] = rates_hourly['id'].astype(int)
    rates_hourly['appointment hour'] = \
        rates_hourly['appointment hour'].apply(
            lambda ts: pd.to_datetime(ts).tz_localize(tz_london)
        )

    print("Merge with occupancy...")
    occupancy = pd.merge(
        left=occupancy,
        right=rates_hourly[['id', 'test type', 'appointment hour', 'rate']],
        on=['id', 'test type', 'appointment hour'],
        how='left'
    )
    occupancy.rename(
        index=str,
        columns={'rate': 'hourly occupancy rate'},
        inplace=True
    )

    return occupancy


def compare_against_timegrid(schedule, timegrid):
    """Compare records of available appointments against timegrid.

    Parameters
    ----------

    schedule:

    Dataframe with records of available appointments

    timegrid:

    Timegrid of appointments

    Returns
    -------
    schedule_against_timegrid:

    Dataframe indicating whether appointments are available at all
    grab timestamps

    """
    schedule_against_timegrid = pd.merge(
        timegrid,               # timegrid_duplicate
        schedule.assign(key=1),
        on=['id', 'test type', 'appointment', 'grab'],
        how='outer'
    )
    schedule_against_timegrid.fillna(0, inplace=True)
    schedule_against_timegrid['key'] = \
        schedule_against_timegrid['key'].astype(int)

    return schedule_against_timegrid


def label_action(df, col='key'):
    """Detects booking and cancellation in dataframe.

    The dataframe

    Parameters
    ----------

    df:

    dataframe with 0's and 1's.  Rows are assumed to be labeled by
    timestamps representing time of data collection, and the column
    'key' represents the availability of one appointment: 0  if it is
    available, 1 if not.

    Returns
    -------

    df:

    the dataframe passed in, modified in place, replacing a transition
    from 0 to 1 with "cancel", a transition from 1 to 0 with "book",
    and "none" otherwise.

    """
    df[col] = df[col] - df[col].shift(1)
    # df[col].fillna(method='bfill', inplace=True)
    df[col].fillna(0, inplace=True)
    df[col] = df[col].astype(int)
    df[col] = df[col].apply(
        lambda delta: "cancel" if delta == 1 else (
            "book" if delta == -1 else "none")
    )
    return df


def get_first_appearance(schedule):
    """Calculate date of first appearance for each appointment.


    Parameters:
    ----------

    schedule:

    dataframe representing available appointments at grab times.
    Assumed with columns 'id', 'test type', 'grab', 'appointment'.


    first_appearance:

    dataframe containg timestamp of first appearance of appointments

    WARNING:

    Caution with type conversions: groupby will convert a
    timezone-aware datetime to an object (string), and using
    reset_index and pandas.to_datetime withtou caution may lead to
    errors.  Best is to convert to UTC at the beginning, and convert
    back to London timezone at the end.

    """
    sched = schedule.copy()
    sched['appointment'] = sched['appointment'].apply(
        lambda ts: ts.tz_convert(tz_utc)
    )
    first_appearance = sched.groupby(
        ['id', 'test type', 'appointment']
    )[['grab']].min()
    first_appearance.rename(
        index=str,
        columns={'grab': 'first appearance'},
        inplace=True
    )
    first_appearance.reset_index(inplace=True)
    first_appearance['appointment'] = pd.to_datetime(
        first_appearance['appointment']
    )
    first_appearance['appointment'] = \
        first_appearance['appointment'].apply(
            lambda ts: ts.tz_localize(tz_utc).tz_convert(tz_london)
        )
    first_appearance['id'] = first_appearance['id'].astype(int)

    return first_appearance


def get_first_posting(schedule):
    """Calculate date of first posting for each appointment.


    Parameters:
    ----------

    schedule:

    dataframe representing available appointments at grab times.
    Assumed with columns 'id', 'test type', 'grab', 'appointment'.


    first_appearance:

    dataframe containg timestamp of first appearance of appointments

    WARNING:

    Caution with type conversions: groupby will convert a
    timezone-aware datetime to an object (string), and using
    reset_index and pandas.to_datetime withtou caution may lead to
    errors.  Best is to convert to UTC at the beginning, and convert
    back to London timezone at the end.

    """
    sched = schedule.copy()
    sched['appointment'] = sched['appointment'].apply(
        lambda ts: ts.tz_convert(tz_utc)
    )
    sched['appointment'] = sched['appointment'].apply(
        lambda ts: ts.replace(hour=0, minute=0, second=0)
    )
    sched.rename(
        index=str,
        columns={'appointment': 'appointment date'},
        inplace=True
    )
    first_posting = sched.groupby(
        ['id', 'test type', 'appointment date']
    )[['grab']].min()
    first_posting.rename(
        index=str,
        columns={'grab': 'first posting'},
        inplace=True
    )
    first_posting.reset_index(inplace=True)
    first_posting['appointment date'] = pd.to_datetime(
        first_posting['appointment date']
    )
    first_posting['appointment date'] = \
        first_posting['appointment date'].apply(
            lambda ts: ts.tz_localize(tz_utc).tz_convert(tz_london)
        )
    first_posting['id'] = first_posting['id'].astype(int)

    return first_posting


def get_ticks(s, freq):
    """Return ticks and tick labels from a list of datetimes,
    depending on frequency.

    Parameters:
    ----------

    s:

    series with datetime values (in London timezone)

    freq:

    desired frequency of ticks, i.e. 'temporal spacing' on plot axis.

    Returns:
    -------

    date_range, tick_labels where:

    date_range:

    datetime index object containing the timestamps to use for axis
    ticks on plots.

    tick_labels:

    list of labels to be used as axis tick labels.


    """

    if freq in ["D", "W"]:
        fmt = "%a %d %b"
    elif freq == "H":
        fmt = "%a %d %H:%M"

    date_range = get_date_range(s, freq)

    if freq == "W":
        ticklabels = [
            pd.to_datetime(ts).strftime(fmt)
            if (ts.dayofweek == 0)
            or (list(date_range).index(ts) in [0, len(list(date_range))-1])
            else ""
            for ts in date_range
        ]
    elif freq in "D":
        ticklabels = [
            pd.to_datetime(ts).strftime("%H:%M")
            if (ts.hour == 12)
            else
            pd.to_datetime(ts).strftime(fmt)
            for ts in date_range
        ]
    elif freq == "H":
        ticklabels = [pd.to_datetime(ts).strftime(fmt)
                      if ts.hour == 0
                      else pd.to_datetime(ts).strftime("%H:%M")
                      for ts in date_range]

    return date_range, ticklabels


def get_date_range(s, freq="D"):
    """Returns date range for tick labels.

    Parameters:
    ----------

    s:

    series with datetime values (in London timezone)

    freq:

    desired frequency of ticks, i.e. 'temporal spacing' on plot axis.

    Returns:
    -------

    date_range:

    datetime index object containing the timestamps to use for axis
    ticks on plots

    """
    ts_min = s.min()
    ts_max = s.max()
    if freq == "W":
        date_range = pd.date_range(
            pd.Timestamp(ts_min.year, ts_min.month, ts_min.day),
            pd.Timestamp(ts_max.year, ts_max.month, ts_max.day),
            freq="W-MON"
        )
        date_range = pd.DatetimeIndex([ts_min.date()]).append(
            date_range).append(
            pd.DatetimeIndex([ts_max.date()])
        )
    elif freq == "D":
        # date_range = pd.date_range(
        #     pd.Timestamp(ts_min.year, ts_min.month, ts_min.day),
        #     pd.Timestamp(ts_max.year, ts_max.month, ts_max.day),
        #     freq="D"
        # )
        date_range = pd.date_range(
            pd.Timestamp(ts_min.year, ts_min.month, ts_min.day),
            pd.Timestamp(ts_max.year, ts_max.month, ts_max.day),
            freq="12H"
        )
    elif freq == "H":
        date_range = pd.date_range(
            pd.Timestamp(
                ts_min.year, ts_min.month, ts_min.day, 3*(ts_min.hour//3)
            ),
            pd.Timestamp(
                ts_max.year, ts_max.month, ts_max.day, 3*(ts_max.hour//3)
            ),
            freq="3H"
        )

    return date_range


class UHCW:
    """Main data structure in UHCW project.

    A UNCW object is instantiated from dataframe raw_data
    The main attributes are:
    - center_info: contains information about id, age group, and test
    types of centers present in the dataset
    - schedule: essentially the data passed in at instantiated, except
    that it has been converted to proper types, and the age group
    column removed from raw_data
    - timegrid: lists of pairs (grab, appointment) present in the
    dataset, labeled by (id, test type) pairs, and with grabs
    occurring after appointments removed.
    - occupancy: timegrid extended with 'status' column indicating
    whether appointment is available or not
    - first_appearance: timestamps where appointments appear first in
    the data set
    - hitory: history of appointments, i.e. all bookings and
    cancellations detected in dataset
    - naive_history: this is an intermediate dataframe, kept for
    debugging purposes.
    Only the attributes center_info and schedule is generated at
    instantiation. The others are generatedy via 'build' methods.

    NOTE ON TIMEZONES.

    All datetime values will be in the London-timezone.  When
    performing certain operations, these will sometimes be converted
    to other types (object, i.e. string).  In order to avoid errors,
    it is safer to begin with a conversion to the UTC standard, and
    end with a conversion back to the London timezone (with the
    appropriate) localization in between.

    """
    def __init__(self, schedule):
        """Instatiation of object of class UHCW.

        Parameters
        ----------

        raw_data:

        dataframe containing data from UHCW online appointment booking
        system.  Presumed to have columns 'id', 'test type', 'age group',
        'grab', 'appointment'.  Each row represents an available
        appointment at a given time (grab):
        - id: id number of test center offering appointment
        - test type: type of blood test administered
        - age group: age group servied (adult or child)
        - grab: timestamp (UTC) of collection of data
        - appointment: timestamp (local, Coventry, UK) of appointment

        The columns are assumed of type object (string) and converted
        at instantiation.

        """
        self.schedule, self.center_info = decouple_data(schedule)
        self.timegrid = None
        self.final_status = None
        self.occupancy = None
        self.first_appearance = None
        self.first_posting = None
        self.naive_history = None
        self.history = None

    def build_timegrid(self):
        """Generate timegrid of schedule attribute and assign it to
        timegrid attribute of object.

        """

        self.timegrid = get_timegrid(self.schedule)

        return None

    # def build_occupancy(self):
    #     """Generate dataframe of occupancy and assign it to attribute.
    #     """
    #     self.occupancy = get_occupancy(self.schedule)
    #
    #     return None

    # def build_final_status(self):
    #     """Generate dataframe with final status (booked or available) of
    #     appointments.
    #     """
    #     if self.timegrid is None:
    #         self.build_timegrid()
    #     self.final_status = get_final_status(self.timegrid)
    #
    #     return None

    def build_occupancy(self):

        if self.timegrid is None:
            self.build_timegrid()

        self.occupancy = get_occupancy(self.timegrid)

        return None

    def build_first_appearance(self):
        """Generate dataframe of first appearance datetimes and assign
        it to first_appearance attribute of object."""

        self.first_appearance = get_first_appearance(self.schedule)

        return None

    def build_first_posting(self):
        """Generate dataframe of first datetimes and assign it to
        first_appearance attribute of object.

        """

        self.first_posting = get_first_posting(self.schedule)

        return None

    def build_naive_history(self):
        """Generate booking history without correcting for artefacts.

        Attempts to create history of appointments, i.e. all bookings
        and cancellations of each appointment in dataset.  This is
        only an auxiliary object, as artefacts need to be corrected.

        Artefacts are due to the appointments being online an
        arbitrary number of days prior to the date.  Their
        'disappearance' is incorrectly interpreted as cancellation.

        Parameters:
        ----------

        self

        Returns:
        -------

        naive_history:

        dataframe with mostly correct bookings and cancellations,
        where artefacts are yet to be corrected.

        """

        # Will need timegrid
        if self.timegrid is None:
            self.build_timegrid()

        df_compare = compare_against_timegrid(
            self.schedule, self.timegrid
        )
        # Detect bookings and cancellations
        df_action = df_compare.groupby(
            ['id', 'test type', 'appointment']
        ).apply(label_action)
        df_action.rename(
            index=str,
            columns={'key': 'action'},
            inplace=True
        )
        self.naive_history = df_action.loc[df_action['action'] != "none", :]

        return None

    def remove_artefacts(self):
        """Remove artefacts from naive history and save it as history"""

        if self.first_posting is None:
            self.build_first_posting()

        naive_history = self.naive_history.copy()
        naive_history['appointment date'] = \
            naive_history['appointment'].apply(
                lambda ts: ts.replace(hour=0, minute=0, second=0)
            )

        genuine_history = pd.merge(
            left=naive_history,
            right=self.first_posting,
            left_on=['id', 'test type', 'grab', 'appointment date'],
            right_on=['id', 'test type', 'first posting', 'appointment date'],
            how='left',
            indicator=True
        )
        genuine_history = genuine_history.loc[
            genuine_history['first posting'].isna(), :
        ]

        self.history = genuine_history

        return None

    def build_history(self):
        """Generate booking history.

        Create history of appointments, i.e. all bookings and
        cancellations of each appointment in dataset.

        Parameters:
        ----------

        self

        Returns:
        -------

        history:

        dataframe with bookings and cancellations.
        """

        if self.naive_history is None:
            self.build_naive_history()

        self.remove_artefacts()

        return None

    def plot(
            self,
            feature='schedule',
            x_freq="W",
            y_freq="H",
            figsize=(16, 6),
            savefigure=False
    ):
        """Plotting functionality.

        Parameters:
        ----------

        feature:

        string specifying name of attribute to plot, among 'schedule',
        'timegrid', 'first_appearance', 'first_posting', 'history',
        'naive history'

        x_freq, y_freq:

        frequencies for ticks on x- and y-axes

        figsize:

        size of figure

        savefigure:

        automatically saves plot as image file if True, location being
        specified by IMG_DIR

        Returns:
        -------

        ax, filepath where:

        ax:

        axis of figure plot

        filepath:

        full path of image file if saved, otherwise None

        TODO: streamline parts with center id, test types, labeling etc.

        """

        # Create first bit in title - completed below
        if len(self.center_info.index) == 0:
            title = ""
        elif len(self.center_info.index) > 1:
            title = "for {0} centers".format(
                len(self.center_info.index)
            )
        else:
            center_test_types = self.center_info.iloc[0]['test types']
            if len(center_test_types) == 0:
                title = ""
            elif len(center_test_types) > 1:
                title = "for center {0} ({1} test types)".format(
                    self.center_info.index[0],
                    len(center_test_types)
                )
            else:
                title = "for center {0} ({1})".format(
                    self.center_info.index[0],
                    center_test_types[0]
                )

        if feature == 'schedule':
            x = 'appointment'
            y = 'grab'
            df = self.schedule[[x, y]]
            plot_style = {
                "marker": '.',
                'markersize': 3,
                'linestyle': ""
            }
            title = " ".join(["Schedule", title])
            x_label = "Appointment"
            y_label = "Data collection"
            legend_text = []
        elif feature == 'timegrid':
            x = 'appointment'
            y = 'grab'
            df = self.timegrid[[x, y]]
            plot_style = {
                "marker": '.',
                'markersize': 3,
                'linestyle': ""
            }
            title = " ".join(["Timegrid", title])
            x_label = "Appointment"
            y_label = "Data collection"
            legend_text = []
        elif feature == 'first appearance':
            x = 'appointment'
            y = 'first appearance'
            df = self.first_appearance[[x, y]]
            plot_style = {
                "marker": '.',
                'markersize': 3,
                'markeredgecolor': "k",
                'markerfacecolor': "w",
                'linestyle': ""
            }
            title = " ".join(["First appearance", title])
            x_label = "Appointment"
            y_label = "First appearance"
            legend_text = []
        elif feature == 'first posting':
            x = 'appointment date'
            y = 'first posting'
            df = self.first_posting[[x, y]]
            plot_style = {
                "marker": '.',
                'markersize': 3,
                'markeredgecolor': "k",
                'markerfacecolor': "w",
                'linestyle': ""
            }
            title = " ".join(["First posting", title])
            x_label = "Appointment date"
            y_label = "First appearance"
            legend_text = []
        elif feature == 'history':
            x = 'appointment'
            y = 'grab'
            df = self.history[[x, y, 'action']]
            plot_style = {
                "marker": '+',
                'markersize': 10,
                'linestyle': ""
            }
            title = " ".join(["History", title])
            x_label = "Appointment"
            y_label = "Action"
            legend_text = []
        elif feature == 'naive history':
            x = 'appointment'
            y = 'grab'
            df = self.naive_history[[x, y, 'action']]
            plot_style = {
                'marker': "+",
                'markersize': 10,
                # "marker": '.',
                # 'markersize': 10,
                # 'markeredgecolor': 'k',
                'linestyle': ""
            }
            title = " ".join(['"Naive" history', title])
            x_label = "Appointment"
            y_label = "Action"
            legend_text = []

        # x- and y-axes ticks and tick labels
        x_freq = x_freq  # "W"
        x_date_range, x_tick_labels = get_ticks(df[x], x_freq)
        y_freq = y_freq  # "H"
        y_date_range, y_tick_labels = get_ticks(df[y], y_freq)

        # Plot
        fig, ax = plt.subplots(figsize=figsize)
        if feature in ['naive history', 'history']:

            for action in ['book', 'cancel']:
                df_one_action = df.loc[
                    df['action'] == action, [x, y]
                ]
                if not df_one_action.empty:
                    legend_text.append(action)
                    df_one_action.plot(
                        x=x,
                        y=y,
                        **plot_style,
                        figsize=figsize,
                        ax=ax
                    )
        else:
            df.plot(x=x, y=y, **plot_style, figsize=figsize, ax=ax)

        # Labeling
        ax.set_title(title, fontsize=18)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)
        ax.set_xticks(x_date_range)
        ax.set_xticklabels(x_tick_labels)
        ax.set_yticks(y_date_range)
        ax.set_yticklabels(y_tick_labels)
        if not(legend_text is []):
            ax.legend(legend_text)
        else:
            ax.get_legend().remove()

        ax.grid()

        if savefigure:
            if len(self.center_info.index) == 0:
                filename = "{0}-empty-figure-{1}.png".format(
                    datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),
                    "-".join(feature.split())
                )
            elif len(self.center_info.index) > 1:
                filename = "{0}-{1}-centers-{2}.png".format(
                    datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),
                    len(self.center_info.index),
                    "-".join(feature.split())
                )
            else:
                center_test_types = self.center_info.iloc[0]['test types']

                if not(len(center_test_types) == 1):
                    filename = \
                        "{0}-center-{1}-{2}-test-types-{3}.png".format(
                            datetime.datetime.now().strftime(
                                "%Y-%m-%d-%H-%M-%S"
                            ),
                            self.center_info.index[0],
                            len(center_test_types[0]),
                            "-".join(feature.split())
                        )
                else:
                    filename = \
                        "{0}-center-{1}-{2}-{3}.png".format(
                            datetime.datetime.now().strftime(
                                "%Y-%m-%d-%H-%M-%S"
                            ),
                            self.center_info.index[0],
                            "-".join(center_test_types[0].split()),
                            "-".join(feature.split())
                        )

            filepath = os.path.join(IMG_DIR, filename)
            plt.savefig(filepath)
        else:
            filepath = None

        return ax, filepath


if __name__ == "__main__":
    print(datetime.datetime.now().strftime("Time: %H:%M:%S"))

    filename = 'appointments.csv'
    foldername = 'Data/UHCW'
    filename = 'sample_appointments.csv'  # appointments.csv
    foldername = 'Projects/UHCW'

    filepath = os.path.join(os.path.expanduser("~"), foldername, filename)

    filesize = os.path.getsize(filepath)
    if filesize > 1e6:
        print("Size of data file: {}MB.".format(filesize//1000000))
    elif filesize > 1e3:
        print("Size of data file: {}KB.".format(filesize//1000))
    else:
        print("Size of data file: {}B.".format(filesize))
    print("Loading data...")
    raw_data = pd.read_csv(filepath, sep=';')
    raw_data.rename(
        index=str,
        columns={
            'center id': 'id',
            'appointment timestamp': 'appointment',
            'center age group': 'age group',
            'grab timestamp': 'grab'
        },
        inplace=True
    )

    # Restrict to smaller collection period
    smaller_dataset = raw_data[
        raw_data['grab'] < '2019-01-19'
    ]
    schedule = prepare_UHCW_dataframe(smaller_dataset)
    # schedule = prepare_UHCW_dataframe(
    #     raw_data[
    #         (raw_data['grab'] < '2019-01-19') & (raw_data['id'].apply(
    #             lambda cid: cid in [10136, 10188, 10243])
    #         )
    #     ]
    # )

    # Restrict to past appointments only
    last_grab = schedule['grab'].max()

    s = schedule.query('appointment <= @last_grab').copy()

    # Calculate number of appointments
    s.groupby(['id', 'test type'])['appointment'].nunique().to_frame()

    # Selected center (and test type)
    cid = 10250  # 10254  # 10263
    test_type = "INR Test"  # "Blood Test"

    # timegrid
    t = get_timegrid(s)

    occ = get_occupancy(t)

    # For debuggin purposes
    s0 = s[(s['id'] == cid) & (s['test type'] == test_type)].copy()
    t0 = t[(t['id'] == cid) & (t['test type'] == test_type)].copy()

    # uhcw = UHCW(raw_data)
    # uhcw.build_timegrid()
    # uhcw.build_final_status()
    # uhcw.build_occupancy()

    print(datetime.datetime.now().strftime("Time: %H:%M:%S"))
