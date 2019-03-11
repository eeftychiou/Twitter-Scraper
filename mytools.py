from datetime import timedelta, date

def daterange(start_date, end_date, interval):

    for n in range(int ((end_date - start_date).days/interval)+1):
        yield start_date + timedelta(n*interval)
