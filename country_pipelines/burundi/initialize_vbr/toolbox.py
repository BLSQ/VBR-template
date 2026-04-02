from openhexa.toolbox.dhis2.periods import Month, Quarter


def get_date_series(start, end, type):
    """
    Get a list of consecutive months or quarters between two dates.

    Parameters:
    --------------
    start: int
        The starting date (e.g. 201811)
    end: int
        The ending date (e.g. 201811)
    type: str
        The type of period to generate ("month" or "quarter").

    Returns
    ------
    range: list
        A list of consecutive months or quarters between the start and end dates.
    """
    if type == "trimestre":
        q1 = Quarter.from_string(start)
        q2 = Quarter.from_string(end)
        range = q1.get_range(q2)
    else:
        m1 = Month.from_string(start)
        m2 = Month.from_string(end)
        range = m1.get_range(m2)
    return range
