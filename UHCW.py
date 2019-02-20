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

# BASIC DATAFRAME MANIPULATION


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
    print("{0}: : Make copy of data...".format(
        datetime.datetime.now().strftime("%H:%M:%S")
    ))
    df = raw_data.copy()

    print("{0}: Type conversion: ".format(
        datetime.datetime.now().strftime("%H:%M:%S")
    ))
    print("{0}: Column appointment:".format(
        datetime.datetime.now().strftime("%H:%M:%S")
    ))
    print("{0}: Convert to datetime...".format(
        datetime.datetime.now().strftime("%H:%M:%S")
    ))
    df['appointment'] = pd.to_datetime(df['appointment'])
    print("{0}: Convert to London timezone...".format(
        datetime.datetime.now().strftime("%H:%M:%S")
    ))
    df['appointment'] = df['appointment'].apply(
        lambda ts: ts.tz_localize(tz_london)
    )

    print("{0}: Column grab:".format(
        datetime.datetime.now().strftime("%H:%M:%S")
    ))
    print("{0}: Convert to datetime...".format(
        datetime.datetime.now().strftime("%H:%M:%S")
    ))
    df['grab'] = pd.to_datetime(df['grab'])
    print("{0}: Localize to UTC and convert to London timezone...".format(
        datetime.datetime.now().strftime("%H:%M:%S")
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


def get_timegrid(schedule):
    """Generate grid of (grab, appointment) pairs appearing in
    dataset, excluding those with grab occurring after appointment.

    Parameters
    ----------

    schedule:

    Dataframe with columns 'id', 'test type', 'grab', 'appointment',
    recording available appointments at grab times.

    Returns
    -------

    timegrid:

    Dataframe with columns 'id', 'test type', 'grab', 'appointment',
    which, for each (id, test type) pair, contains all pairs (grab,
    appointment) where grab and appointment appearing in dataset.

    """
    grabs = \
        schedule.loc[
            :, ['id', 'test type', 'grab']
        ].drop_duplicates().reset_index(drop=True)

    appointments = \
        schedule.loc[
            :, ['id', 'test type', 'appointment']
        ].drop_duplicates().reset_index(drop=True)

    cartesian = pd.merge(
        left=grabs,
        right=appointments,
        on=['id', 'test type'],
        how='outer'
    )

    cartesian.query('grab <= appointment', inplace=True)

    # Add column indicating whether appointment available or booked
    timegrid = pd.merge(
        left=cartesian,
        right=schedule,
        on=['id', 'test type', 'appointment', 'grab'],
        how='left',
        indicator=True
    )
    timegrid = timegrid[['id', 'test type', 'appointment', 'grab', '_merge']]
    timegrid.sort_values(
        ['id', 'test type', 'appointment', 'grab'],
        inplace=True
    )
    timegrid.rename(index=str, columns={'_merge': 'status'}, inplace=True)
    timegrid['status'] = timegrid['status'].apply(
        lambda ind: "booked" if ind == "left_only" else "available"
    )

    return timegrid


def get_final_status(occupancy):
    """Extract status of appointment at last grab."""

    # Calculate last grab for each appointment
    final_status = \
        occupancy.groupby(
            ['id', 'test type', 'appointment']
        )['grab'].max().to_frame().reset_index()

    # Restore action values (TODO: can this extra step be avoided?)
    final_status = pd.merge(
        left=final_status,
        right=occupancy[['id', 'test type', 'appointment', 'grab', 'status']],
        on=['id', 'test type', 'appointment', 'grab'],
        how='left',
        # indicator=True
    )
    return final_status


def get_occupancy(final_status):
    counts = final_status.groupby(
        ['id', 'test type']
    )['status'].value_counts().to_frame().rename(
        index=str,
        columns={'status': 'count'}
    ).reset_index()

    rates = \
        pd.pivot_table(
            counts,
            values='count',
            index=['id', 'test type'],
            columns=['status']
        ).fillna(0).astype(int)
    rates['rate'] = \
        (100*rates['booked']) // (rates['available'] + rates['booked'])

    return rates


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


def build_center_test_info(df):
    """Create list of test centers with their test types.

    Parameters:
    ----------

    df:

    dataframe whose columns contain 'id', 'test type', and 'age group'

    Returns:
    -------

    center_info:

    dataframe indexed by center id, and two columns: 'age group'
    indicating age group serviced at test center, and 'test types', a
    list of test types administered at test center.

    TODO: there has to be a more natural way to implement this function.

    """
    center_test_types =  \
        df.groupby('id')['test type'].unique().rename(
            "test types"
        ).to_frame()
    center_age_group = \
        df.groupby('id')['age group'].unique().to_frame()

    center_info = \
        pd.merge(
            center_test_types,
            center_age_group,
            on='id',
            how='outer'
        )

    return center_info


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
        self.schedule = schedule  # prepare_UHCW_dataframe(raw_data)
        self.center_info = build_center_test_info(self.schedule)
        self.schedule.drop('age group', axis=1, inplace=True)
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

    def build_final_status(self):
        """Generate dataframe with final status (booked or available) of
        appointments.
        """
        if self.occupancy is None:
            self.build_occupancy()
        self.final_status = get_final_status(self.occupancy)

        return None

    def build_occupancy(self):
        if self.final_status is None:
            self.build_final_status()

        self.occupancy = get_occupancy(self.final_status)

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
    filename = 'sample_appointments.csv'  # appointments.csv
    foldername = 'Projects/UHCW'
    # filename = 'appointments.csv'
    # foldername = 'Data/UHCW'
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
    print("Number of records: {0}.".format(raw_data.shape[0]))
    print("Column names:\n{}".format("\n".join(raw_data.columns)))

    uhcw = UHCW(raw_data)
    uhcw.build_timegrid()
    uhcw.build_final_status()
    uhcw.build_occupancy()

    print(datetime.datetime.now().strftime("Time: %H:%M:%S"))
