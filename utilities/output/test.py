from datetime import datetime, timedelta


def get_last_friday():
    today = datetime.today()
    days_until_friday = (today.weekday() - 0) % 7
    last_friday = today - timedelta(days=days_until_friday)
    # Check if last_friday is equal to today (which means today is a Friday)
    # If today is a Friday, then we want to get the Friday of the previous week
    if last_friday == today:
        last_friday -= timedelta(weeks=1)
    return last_friday.date()


last_friday_date = get_last_friday()
print(last_friday_date)