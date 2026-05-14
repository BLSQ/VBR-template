from datetime import date, timedelta, datetime

from openhexa.sdk import current_run
from openhexa.toolbox.dhis2.periods import Period, get_range, period_from_string


def calculate_relative_dates(days_back: int) -> tuple[str, str]:
    """Calculate relative date range based on today's date.

    Args:
        days_back (int): Number of days to go back from today for start date.

    Returns:
        tuple[str, str]: (start_date, end_date) in YYYY-MM-DD format.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def get_start_end_date(
    use_relative_dates: bool, days_back: int, start_date: str, end_date: str
) -> tuple[str, str]:
    if use_relative_dates:
        start_date, end_date = calculate_relative_dates(days_back)
        current_run.log_info(
            f"Using relative dates: {start_date} to {end_date} - Days back: {days_back}"
        )
    else:
        current_run.log_info(f"Using provided dates: {start_date} to {end_date}")

    return start_date, end_date


def get_periods_from_start_end_dates(start_date, end_date, period_type) -> list[str]:
    start_period = isodate_to_period_type(start_date, period_type)
    end_period = isodate_to_period_type(end_date, period_type)
    periods = get_range(start_period, end_period)
    if not periods:
        raise ValueError(
            f"No periods found between {start_date} and {end_date} for period type '{period_type}'. "
            "Check that start_date is before end_date."
        )
    periods_str = [str(p) for p in periods]
    current_run.log_info(f"Querying {len(periods)} periods: {periods[0]} to {periods[-1]}")
    return periods_str


def isodate_to_period_type(date_str: str, period_type: str) -> Period:
    """Converts an ISO date string to a DHIS2-compatible period string based on the specified period type.

    Args:
        date_str (str): The ISO date string in the format "YYYY-MM-DD".
        period_type (str): The DHIS2 period type. Supported values include:
            - "Daily": Converts to a daily period (e.g., "20230101").
            - "Weekly": Converts to a weekly period starting on Monday (e.g., "2023W1").
            - "WeeklyMonday", "WeeklyTuesday", ..., "WeeklySunday": Converts to a weekly period
              aligned to the specified weekday.
            - "Monthly": Converts to a monthly period (e.g., "202301").
            - "BiMonthly": Converts to a bi-monthly period (e.g., "202301" for Jan-Feb).
            - "Quarterly": Converts to a quarterly period (e.g., "2023Q1").
            - "SixMonthly": Converts to a six-monthly period (e.g., "2023S1" for Jan-Jun).
            - "SixMonthlyApril": Converts to a six-monthly period starting in April (e.g., "2023AprilS1").
            - "Yearly": Converts to a yearly period (e.g., "2023").
            - "FinancialApril": Converts to a financial year starting in April (e.g., "2023April").
            - "FinancialJuly": Converts to a financial year starting in July (e.g., "2023July").
            - "FinancialOct": Converts to a financial year starting in October (e.g., "2023Oct").

    Returns:
        Period: A DHIS2-compatible period object created from the generated period string.

    Raises:
        ValueError: If the provided period type is unsupported.
    """  # noqa: E501
    dt = datetime.strptime(date_str, "%Y-%m-%d")

    if period_type == "Daily":
        period_str = dt.strftime("%Y%m%d")

    elif period_type.startswith("Weekly"):
        iso_year, iso_week, _ = dt.isocalendar()
        if period_type != "Weekly" and period_type != "WeeklyMonday":
            period_str = f"{iso_year}{period_type.replace('Weekly', '')[:3]}W{iso_week}"
        else:
            period_str = f"{iso_year}W{iso_week}"

    elif period_type == "Monthly":
        period_str = dt.strftime("%Y%m")

    elif period_type == "BiMonthly":
        period_str = f"{dt.year}{(dt.month - 1) // 2 + 1:02d}B"

    elif period_type == "Quarterly":
        period_str = f"{dt.year}Q{(dt.month - 1) // 3 + 1}"

    elif period_type == "SixMonthly":
        period_str = f"{dt.year}S{1 if dt.month <= 6 else 2}"

    elif period_type == "SixMonthlyApril":
        if 4 <= dt.month <= 9:
            period_str = f"{dt.year}AprilS1"
        else:
            ref_year = dt.year if dt.month >= 4 else dt.year - 1
            period_str = f"{ref_year}AprilS2"

    elif period_type == "Yearly":
        period_str = f"{dt.year}"

    elif period_type == "FinancialApril":
        fy = dt.year if dt.month >= 4 else dt.year - 1
        period_str = f"{fy}April"

    elif period_type == "FinancialJuly":
        fy = dt.year if dt.month >= 7 else dt.year - 1
        period_str = f"{fy}July"

    elif period_type == "FinancialOct":
        fy = dt.year if dt.month >= 10 else dt.year - 1
        period_str = f"{fy}Oct"

    else:
        raise ValueError(f"Unsupported DHIS2 period type: {period_type}")

    return period_from_string(period_str)
