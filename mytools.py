from datetime import timedelta, date

def daterange(start_date, end_date, interval):

    for n in range(int ((end_date - start_date).days/interval)+1):
        yield start_date + timedelta(n*interval)

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i + n]