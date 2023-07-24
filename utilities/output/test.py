from datetime import datetime, timedelta

def get_previous_month_dates():
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
    return first_day_of_previous_month.date(), last_day_of_previous_month.date()

start_date, end_date = get_previous_month_dates()
print("Start date of previous month:", start_date)
print("End date of previous month:", end_date)