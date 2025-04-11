from openhexa.toolbox.dhis2.periods import Month, Quarter


def month_to_quarter(num):
    """
    Given a month, return the quarter corresponding to the given month.

    Parameters
    ----------
    num: int
        A given month (e.g. 201808)

    Returns
    -------
    str
        The quarter corresponding to the given month (e.g. 2018Q3)
    """
    num = int(num)
    y = num // 100
    m = num % 100
    return str(y) + "Q" + str((m - 1) // 3 + 1)


def quarter_to_months(name):
    """
    Given a quarter, return the third month of the quarter.

    Parameters
    ----------
    name: str
        A given quarter (e.g. 2018Q3)

    Returns
    -------
    int
        The third month of the quarter (e.g. 201809)
    """
    year, quarter = str(name).split("Q")
    return int(year) * 100 + int(quarter) * 3


def months_before(date, lag):
    """
    Get the month that is "lag" months before a given date.

    Parameters
    ----------
    date: int
        The given month (e.g. 201804).
    lag: int
        The number of months before (e.g. 6).

    Returns
    -------
    int
        The month corresponding to the period that is "lag" months before "date" (e.g. 201710).
    """
    date = int(date)
    year = date // 100
    m = date % 100
    lag_years = lag // 12
    year -= lag_years
    lag = lag - 12 * lag_years
    diff = m - lag
    if diff > 0:
        return year * 100 + m - lag
    else:
        year -= 1
        m = 12 + diff
        return year * 100 + m


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
    if type == "quarter":
        q1 = Quarter(start)
        q2 = Quarter(end)
        range = q1.get_range(q2)
    else:
        m1 = Month(start)
        m2 = Month(end)
        range = m1.get_range(m2)
    return range
