#!/usr/bin/env python3
"""Module with custom functions and classes for UHCW project."""
# --------------------
# IMPORTS
# --------------------
import os
import pytz
import pandas as pd
import datetime

# --------------------
# CONSTANTS
# --------------------
tz_utc = pytz.timezone("UTC")  # timestamp is in UTC standard
tz_london = pytz.timezone("Europe/London")  # test centers are in Coventry, UK

IMG_DIR = os.path.join(os.path.expanduser("~"), "Projects/UHCW/IMAGE_FILES")

# --------------------
# FUNCTIONS
# --------------------


def prepare_UHCW_dataframe(raw_data):
    """Perform type conversion and column renaming.

    Parameters
    ----------

    raw_data:

    Dataframe containing data from UHCW online appointment booking
    system.  Each row represents an available appointment at a given
    time (grab) posted on platform.

    Assumptions:

    all values are of type "object" (string)

    raw_data has columns:

    - id: id number of test center offering appointment
    - test: type of blood test administered
    - age group: age group servied (adult or child)
    - grab: timestamp (UTC) of collection of data
    - appointment: timestamp (local, Coventry, UK) of appointment

    Returns:
    -------

    schedule:

    Dataframe where each row corresponds to an appointment posted as
    available on the platform.  The columns must include 'id', 'test',
    'appointment', 'grab'.  The dataframe is sorted by values in the
    order: 'id', 'test', 'appointment', 'grab'.

    """
    FMT_TIME = "%Hh%Mm%Ss"
    print("{0}: Make copy of data...".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    schedule = raw_data.copy()

    print("{0}: Type conversion: ".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    print("{0}: Column appointment:".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    print("{0}: Convert to datetime...".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    schedule['appointment'] = pd.to_datetime(schedule['appointment'])
    print("{0}: Convert to London timezone...".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    schedule['appointment'] = schedule['appointment'].apply(
        lambda ts: ts.tz_localize(tz_london)
    )

    print("{0}: Column grab:".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    print("{0}: Convert to datetime...".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    schedule['grab'] = pd.to_datetime(schedule['grab'])
    print("{0}: Localize to UTC and convert to London timezone...".format(
        datetime.datetime.now().strftime(FMT_TIME)
    ))
    schedule['grab'] = schedule['grab'].apply(
        lambda ts: ts.tz_localize(tz_utc).tz_convert(tz_london)
    )

    schedule = schedule[
        ['id', 'test', 'age group', 'appointment', 'grab']
    ]
    schedule.sort_values(
        ['id', 'test', 'appointment', 'grab'],
        inplace=True
    )

    return schedule


def get_center_info(schedule):
    """Create list of test centers with their tests.

    Parameters:
    ----------

    schedule:

    Dataframe whose columns contain 'id', 'test', and 'age group'

    Returns:
    -------

    center_info:

    Dataframe containing id, age group served, and test types
    administered at centers.  It is indexed by the center id and has
    two columns 'test' and 'age group'.  A center serves only one age
    group, but may administer several types of test.

    """

    center_info = schedule[
        ['id', 'age group', 'test']
    ].drop_duplicates()
    center_info.sort_values(['id', 'test'])
    center_info.set_index('id', inplace=True)

    return center_info


def get_history(schedule, past_appointments=True):
    """Generate history from schedule.

    Parameters:
    ----------

    schedule:

    Dataframe where each row corresponds to an appointment posted as
    available on the platform.  The columns must include 'id', 'test',
    'appointment', 'grab'.

    past_appointments:

    boolean, indicates whether only past appointments should be kept
    (the schedule contains information about availability of
    appointments that are still in the future).

    Returns:
    -------

    history:

    Dataframe recording status ('available' or 'booked') of all
    appointments appearing in the dataset, at all grab times
    appearing in the dataset (for the same center id and test type).
    The columns are 'id', 'test', 'grab', 'appointment', 'status'.

    """

    print(datetime.datetime.now().strftime("%H:%M:%S"))
    if not (isinstance(schedule, pd.DataFrame)):
        print('WARNING: input variable "schedule is not a dataframe."')
        return None
    elif schedule.empty:
        print('WARNING: input variable "schedule" is an empty dataframe.')
        return None
    elif not set({'id', 'test', 'appointment', 'grab'}).issubset(
            schedule.columns
    ):
        print('WARNING: input variable "schedule" is missing columns.')
        print('(Must contain: "id", "test", "appointment", "grab".')
        return None
    else:
        None

    print("Cartesian product grabs by appointments...")
    grabs = \
        schedule.loc[
            :, ['id', 'test', 'grab']
        ].drop_duplicates().reset_index(drop=True)

    appointments = \
        schedule.loc[
            :, ['id', 'test', 'appointment']
        ].drop_duplicates().reset_index(drop=True)

    h = pd.merge(
        left=grabs,
        right=appointments,
        on=['id', 'test'],
        how='outer'
    )

    print("Ignore grabs past appointments...")
    h.query('grab <= appointment', inplace=True)
    h.sort_values(['appointment', 'grab'], inplace=True)

    if past_appointments:
        last_grab = schedule['grab'].max()
        print(
            "Restrict to past appointments (on or before {0})...".format(
                last_grab
            )
        )
        h.query('appointment <= @last_grab', inplace=True)

    h.sort_values(
        ['id', 'test', 'appointment', 'grab'],
        inplace=True
    )

    # Add column indicating whether appointment available or booked
    print("Add status...")
    t = pd.merge(
        left=h,
        right=schedule,
        on=['id', 'test', 'appointment', 'grab'],
        how='left',
        indicator=True
    )

    t.rename(index=str, columns={'_merge': 'status'}, inplace=True)
    t['status'] = t['status'].apply(
        lambda ind: "booked" if ind == "left_only" else "available"
    )

    if 'age group' in t.columns:
        t.drop('age group', axis=1, inplace=True)
    else:
        None

    return t


def get_final_status(history):
    """Calculate final status of appointments from history.

    Parameters:
    ----------

    history:

    Dataframe recording status ('available' or 'booked') of
    appointments appearing at a list of grab times.  The columns must
    include 'id', 'test', 'appointment', 'grab', 'status'.

    Returns:
    -------

    final_status:

    Dataframe with status ('available' or 'booked') of appointments at
    last grab times.  The columns are 'id', 'test', 'appointment',
    'last grab' and 'final status'.

    """
    if not (isinstance(history, pd.DataFrame)):
        print('WARNING: input variable "history" is not a dataframe."')
        return None
    elif history.empty:
        print('WARNING: input variable "history" is an empty dataframe.')
        return None
    elif not set({'id', 'test', 'appointment', 'grab', 'status'}).issubset(
            history.columns
    ):
        print('WARNING: input variable "history" is missing columns.')
        print(
            '(Must contain: "id", "test", "appointment", "grab", "status".'
        )
        return None
    else:
        None

    # # COMMENT OUT some of the following for the moment: too
    # # sophisticated at this point

    # # Add columns with various groups of grabs
    # print("Add extra grab columns:")
    # print("{0}: grab hour...".format(
    #     datetime.datetime.now().strftime("%H:%M:%S")
    # ))
    # t['grab hour'] = \
    #     t['grab'].apply(lambda ts: ts.replace(minute=0, second=0))
    # print("{0}: grab 3 hours...".format(
    #     datetime.datetime.now().strftime("%H:%M:%S")
    # ))
    # t['grab 3 hours'] = \
    #     t['grab'].apply(
    #         lambda ts: ts.replace(
    #             hour=3*(ts.hour // 3),
    #             minute=0,
    #             second=0)
    #     )
    # print("{0}: grab day...".format(
    #     datetime.datetime.now().strftime("%H:%M:%S")
    # ))
    # t['grab day'] = t['grab'].apply(
    #     lambda ts: ts.replace(hour=0, minute=0, second=0)
    # )
    #
    # # Add status in last hour
    # status_in_hour = \
    #     t.groupby(
    #         ['id', 'test', 'appointment', 'grab hour']
    #     )['status'].apply(
    #         lambda group: "booked" in list(group)
    #     ).reset_index()
    #
    # status_in_hour['status'] = \
    #     status_in_hour['status'].apply(
    #         lambda status: "booked" if status else "available"
    #     )
    # status_in_hour.rename(
    #     index=str,
    #     columns={'status': 'status in last hour'},
    #     inplace=True
    # )
    #
    # t = pd.merge(
    #     left=t,
    #     right=status_in_hour,
    #     on=['id', 'test', 'appointment', 'grab hour'],
    #     how='left'
    # )
    #
    # # Add status in last 3 hours
    # status_in_3_hours = \
    #     t.groupby(
    #         ['id', 'test', 'appointment', 'grab 3 hours']
    #     )['status'].apply(
    #         lambda group: "booked" in list(group)
    #     ).reset_index()
    # status_in_3_hours['status'] = \
    #     status_in_3_hours['status'].apply(
    #         lambda status: "booked" if status else "available"
    #     )
    # status_in_3_hours.rename(
    #     index=str,
    #     columns={'status': 'status in last 3 hours'},
    #     inplace=True
    # )
    #
    # t = pd.merge(
    #     left=t,
    #     right=status_in_3_hours,
    #     on=['id', 'test', 'appointment', 'grab 3 hours'],
    #     how='left'
    # )
    #
    # # Add status in last day
    # status_in_day = \
    #     t.groupby(
    #         ['id', 'test', 'appointment', 'grab day']
    #     )['status'].apply(
    #         lambda group: "booked" in list(group)
    #     ).reset_index()
    #
    # status_in_day['status'] = \
    #     status_in_day['status'].apply(
    #         lambda status: "booked" if status else "available"
    #     )
    # status_in_day.rename(
    #     index=str,
    #     columns={'status': 'status in last day'},
    #     inplace=True
    # )
    #
    # t = pd.merge(
    #     left=t,
    #     right=status_in_day,
    #     on=['id', 'test', 'appointment', 'grab day'],
    #     how='left'
    # )

    # Add final status

    h = history[
        ['id', 'test', 'appointment', 'grab', 'status']
    ].copy()
    h.sort_values(['id', 'test', 'appointment', 'grab'])

    final_status = \
        h.groupby(
            ['id', 'test', 'appointment']
        )['status'].apply(lambda group: list(group)[-1]).reset_index()

    final_status.rename(
        index=str, columns={'status': 'final status'},
        inplace=True
    )

    # calculate last grabs
    last_grabs = h[
        ['id', 'test', 'appointment', 'grab']
    ].groupby(
        ['id', 'test', 'appointment']
    )['grab'].max().reset_index().rename(
        index=str,
        columns={'grab': 'last grab'}
    )

    # merge last grabs with final status
    final_status = pd.merge(
        left=final_status,
        right=last_grabs,
        on=['id', 'test', 'appointment'],
        how='left'
    )

    final_status = pd.merge(
        left=h,
        right=final_status,
        on=['id', 'test', 'appointment'],
        how='left'
    )

    final_status = final_status[
        ['id', 'test', 'appointment', 'last grab', 'final status']
    ]
    final_status.drop_duplicates(inplace=True)
    # final_status.reset_index(drop=True, inplace=True)

    return final_status


def get_activity(history):
    """Detect activity in history.

    Parameters:
    ----------

    history:

    Dataframe recording status ('available' or 'booked') of
    appointments appearing at a list of grab times.  The columns must
    include 'id', 'test', 'appointment', 'grab', 'status'.

    Returns:
    -------

    activity:

    Dataframe where each row corresponds to an action: either booking
    or cancellation.  The columns are 'id', 'test', 'appointment',
    'grab', 'previous grab', and 'action'.  In a row, the value of
    'action' is 'cancel' if the appointment is not available at the
    row and it is available at the previous row.  The value of
    'action' is 'book', if the appointment is available at the current
    row and not at the previous row.

    """
    if not (isinstance(history, pd.DataFrame)):
        print('WARNING: input variable "history" is not a dataframe."')
        return history.copy()
    elif history.empty:
        print('WARNING: input variable "history" is an empty dataframe.')
        return history.copy()
    elif not set({'id', 'test', 'appointment', 'grab', 'status'}).issubset(
            history.columns
    ):
        print('WARNING: input variable "history" is missing columns.')
        print(
            '(Must contain: "id", "test", "appointment", "grab", "status".'
        )
        return history.copy()
    else:
        None
    activity = history.copy()

    activity['action'] = \
        activity['status'].apply(
            lambda status: 1 if status == 'available' else 0
        )

    activity['forward'] = activity.groupby(
        ['id', 'test', 'appointment']
    )['action'].shift(1)
    activity['forward'].fillna(activity['action'], inplace=True)
    activity['forward'] = activity['forward'].astype(int)
    activity['f diff'] = activity['forward'] - activity['action']
    activity['f diff'] = activity['f diff'].apply(
        lambda diff: 1 if diff == 1 else 0
    )

    activity['backward'] = activity.groupby(
        ['id', 'test', 'appointment']
    )['action'].shift(-1)
    activity['backward'].fillna(activity['action'], inplace=True)
    activity['backward'] = activity['backward'].astype(int)
    activity['b diff'] = activity['backward'] - activity['action']
    activity['b diff'] = activity['b diff'].apply(
        lambda diff: 1 if diff == 1 else 0
    )
    activity['b diff'] = activity['b diff'].shift(1)
    activity['b diff'].fillna(0, inplace=True)
    activity['b diff'] = activity['b diff'].astype(int)

    activity['action'] = activity['f diff'] - activity['b diff']
    activity['action'] = activity['action'].apply(
        lambda action: "cancel" if action == 1 else (
            "book" if action == -1 else "none")
    )

    activity['previous grab'] = \
        activity.groupby(
            ['id', 'test', 'appointment']
        )['grab'].shift(1)
    activity['previous grab'].fillna(activity['grab'], inplace=True)

    activity = activity[[
        'id',
        'test',
        'appointment',
        'grab',
        'previous grab',
        'action'
    ]]
    activity = activity.query('action != "none"')

    # activity.reset_index(drop=True, inplace=True)

    return activity


def get_occupancy(history):
    """Calculate occupancy rate of appointments.

    In progress.

    """

    occupancy = history.loc[
        history.index,
        ['id', 'test', 'appointment', 'final status']
    ]

    # Overall occupancy

    print("Calculate overall occupancy rate:")
    print("Calculate counts...")
    counts = occupancy.groupby(
        ['id', 'test']
    )['final status'].value_counts().to_frame().rename(
        index=str,
        columns={'final status': 'count'}
    ).reset_index()

    print("Pivot table...")
    rates = \
        pd.pivot_table(
            counts,
            values='count',
            index=['id', 'test'],
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
        right=rates[['id', 'test', 'rate']],
        on=['id', 'test'],
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
        ['id', 'test', 'appointment day']
    )['final status'].value_counts().to_frame().rename(
        index=str,
        columns={'final status': 'count'}
    ).reset_index()

    print("Pivot table of daily counts...")
    rates_daily = \
        pd.pivot_table(
            counts_daily,
            values='count',
            index=['id', 'test', 'appointment day'],
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
        right=rates_daily[['id', 'test', 'appointment day', 'rate']],
        on=['id', 'test', 'appointment day'],
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
        ['id', 'test', 'appointment hour']
    )['final status'].value_counts().to_frame().rename(
        index=str,
        columns={'final status': 'count'}
    ).reset_index()

    print("Pivot table of hourly counts...")
    rates_hourly = \
        pd.pivot_table(
            counts_hourly,
            values='count',
            index=['id', 'test', 'appointment hour'],
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
        right=rates_hourly[['id', 'test', 'appointment hour', 'rate']],
        on=['id', 'test', 'appointment hour'],
        how='left'
    )
    occupancy.rename(
        index=str,
        columns={'rate': 'hourly occupancy rate'},
        inplace=True
    )

    return occupancy






def get_first_appearance(schedule):
    """Calculate date of first appearance for each appointment.


    Parameters:
    ----------

    schedule:

    dataframe representing available appointments at grab times.
    Assumed with columns 'id', 'test', 'grab', 'appointment'.


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
        ['id', 'test', 'appointment']
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
    Assumed with columns 'id', 'test', 'grab', 'appointment'.


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
        ['id', 'test', 'appointment date']
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
    ts_min = s.min() - pd.Timedelta("3H")
    ts_max = s.max() + pd.Timedelta("3H")
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


if __name__ == "__main__":
    print("")

    foldername = 'Projects/UHCW'
    filename = 'appointments-tiny.csv'
    # filename = 'appointments-less-tiny.csv'
    # filename = 'appointments-one-week-two-centers.csv'
    # filename = 'appointments-24-hours.csv'
    # filename = 'appointments-one-week.csv'
    # filename = 'appointments-six-weeks.csv'
    # filename = 'appointments-three-weeks.csv'

    filepath = os.path.join(os.path.expanduser("~"), foldername, filename)

    raw_data = pd.read_csv(filepath, sep=';')
    raw_data.rename(
        index=str,
        columns={
            'center id': 'id',
            'test type': 'test',
            'appointment timestamp': 'appointment',
            'center age group': 'age group',
            'grab timestamp': 'grab'
        },
        inplace=True
    )

    schedule = prepare_UHCW_dataframe(raw_data)

    # Calculate number of appointments
    schedule.groupby(
        ['id', 'test']
    )['appointment'].nunique().to_frame()

    # Test first "by hand"
    s = schedule.copy()
    h = get_history(s)
    f = get_final_status(h)
    a = get_activity(h)

    center_info = get_center_info(schedule)
