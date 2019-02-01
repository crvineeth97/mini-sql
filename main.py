import sys
from collections import defaultdict


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
        report_error("ERROR: No select statement in query")
    if query[1].lower() == 'distinct':
        distinct_flag = 1
        i = 2
    else:
        distinct_flag = 0
        i = 1
    selections = []
    query_length = len(query)
    while i < query_length and query[i].lower() != 'from':
        for selection in query[i].split(','):
            if selection == '':
                continue
            selections.append(selection)
        i += 1
    if len(selections) == 0:
        report_error(
            "ERROR: No selections have been provided after SELECT statement")
    if i == query_length:
        report_error(
            "ERROR: No tables to select from. Syntax error in FROM clause.")
    tables = []
    while i < query_length and query[i].lower() != 'where':
        for name in query[i].split(','):
            if name == '':
                continue
            tables.append(name)
        i += 1
    conditions = []
    op = ''
    while i < query_length and query[i].lower() != ';':
        cond = ''
        while i < query_length and query[i].lower() != 'and' and query[i].lower() != 'or':
            cond += query[i]
            i += 1
        if i < query_length:
            op = query[i].lower()
        conditions.append(cond)
        i += 1
    # Selections can be *, sum, average, max and min(col), multiple columns
    # Tables are straight-forward
    # Conditions can be <, >, <=, >=, = and can have one AND or OR
    metadata = defaultdict(list)
    with open('metadata.txt', 'r') as f:
        lines = f.readlines()
        lines = [line.strip() for line in lines]
    table_name = ''
    flg = 0
    for line in enumerate(lines):
        if line == '<begin_table>':
            flg = 1
        if flg:
            table_name = line
            flg = 0
            continue
        metadata[table_name].append(line)
    tables_info = defaultdict(defaultdict(list))
    for table in tables:
        with open(table + '.csv', 'r'):
            lines = f.readlines()
            lines = [line.strip().split(',') for line in lines]
        for line in lines:
            for i, val in enumerate(line):
                tables_info[table][metadata[table][i]].append(val)
    rel_ops = []
    operands = []
    for cond in conditions:
        if '>=' in cond:
            rop = '>='
        elif '<=' in cond:
            rop = '<='
        elif '=' in cond:
            rop = '='
        elif '<' in cond:
            rop = '<'
        elif '>' in cond:
            rop = '>'
        else:
            report_error('ERROR: Condition does not have a relational operator')
        rel_ops.append(rop)
        operands.append(cond.split(rop))