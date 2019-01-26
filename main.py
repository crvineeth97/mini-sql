import sys


def report_error(error):
    print(error)
    sys.exit(1)


if len(sys.argv) != 2:
    report_error("Usage: python main.py <Query in double quotes>")

# Can't do sys.argv[1].lower() cause file names will not be proper
# Split will be imperfect, will need to handle cases where there is
# no space after commas and so on
queries = [line.split(' ') for line in sys.argv[1].split(';')]

# Some wrong queries to be handled:
# * Ensure that ; is present at the end of the query
# * select *, col from table;
# * select max(col1, col2)
# * Ambiguity w.r.t projection from multiple tables, like 2 columns with
#     same names can be present in 2 different tables, which to pick?
#     Check what MYSQL does in this case


for query in queries:
    if query[0].lower() != 'select':
        report_error("No select statement in query")
    if query[1].lower() == 'distinct':
        distinct_flag = 1
        i = 2
    else:
        distinct_flag = 0
        i = 1
    while query[i].lower() != 'from':

        i += 1
