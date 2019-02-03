import sys
from collections import defaultdict
import copy
import operator


def report_error(error):
    print(error)
    sys.exit(1)


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def parse_query(query):
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
            if selection.strip() == '':
                continue
            selections.append(selection.strip())
        i += 1
    if len(selections) == 0:
        report_error(
            "ERROR: No selections have been provided after SELECT statement")
    if i == query_length:
        report_error(
            "ERROR: No tables to select from. Syntax error in FROM clause.")
    tables = []
    i += 1
    while i < query_length and query[i].lower() != 'where':
        for name in query[i].split(','):
            if name.strip() == '':
                continue
            if name.strip() in tables:
                report_error(
                    "ERROR: Cannot repeat table name '" + name.strip() + "' in FROM clause.")
            tables.append(name.strip())
        i += 1
    i += 1
    conditions = []
    lop = ''
    while i < query_length:
        cond = ''
        while i < query_length and query[i].lower() != 'and' and query[i].lower() != 'or':
            cond += query[i]
            i += 1
        if i < query_length:
            lop = query[i].lower()
        conditions.append(cond)
        i += 1
    rel_ops = []
    operands = []
    join_flag = 0
    for cond in conditions:
        if '>=' in cond:
            rop = operator.ge
            hlpme = '>='
        elif '<=' in cond:
            rop = operator.le
            hlpme = '<='
        elif '=' in cond:
            rop = operator.eq
            hlpme = '='
        elif '<' in cond:
            rop = operator.lt
            hlpme = '<'
        elif '>' in cond:
            rop = operator.gt
            hlpme = '>'
        else:
            report_error(
                'ERROR: Condition does not have a relational operator')
        rel_ops.append(rop)
        operands.append([operand.strip().split('.')
                         for operand in cond.split(hlpme)])
        if rop == operator.eq:
            if not is_number(operands[-1][1][0]):
                if join_flag:
                    report_error(
                        'ERROR: Cannot handle queries with 2 join operations')
                else:
                    join_flag = 1
        else:
            if not is_number(operands[-1][1][0]):
                report_error(
                    'ERROR: Cannot handle queries where the join condition uses a relational operator other than "="')
    return distinct_flag, join_flag, selections, tables, rel_ops, operands, lop


def read_data(tables):
    metadata = defaultdict(list)
    cols = defaultdict(list)
    try:
        with open('metadata.txt', 'r') as f:
            lines = f.readlines()
            lines = [line.strip() for line in lines]
    except:
        report_error("ERROR: No file 'metadata.txt' found")
    table_name = ''
    flg = 0
    for line in lines:
        if line == '<begin_table>':
            flg = 1
            continue
        if line == '<end_table>':
            continue
        if flg:
            table_name = line
            flg = 0
            continue
        if table_name in tables:
            cols[line].append(table_name)
            metadata[table_name].append(line)
    tables_info = defaultdict(list)
    for table in tables:
        try:
            with open(table + '.csv', 'r') as f:
                lines = f.readlines()
                lines = [line.strip().split(',') for line in lines]
        except:
            report_error("ERROR: No file '" + table + ".csv' found")
        for line in lines:
            for i, val in enumerate(line):
                app = [j for j in val.strip().split('"') if j != ''][0]
                tables_info[table + '.' + metadata[table]
                            [i]].append(int(app))
    return metadata, cols, tables_info


def handle_join(metadata, tables_info, table1, col1, table2, col2):
    result = defaultdict(list)
    my_col1 = table1 + '.' + col1
    my_col2 = table2 + '.' + col2
    len_table1 = len(tables_info[my_col1])
    len_table2 = len(tables_info[my_col2])
    for i in range(len_table1):
        for j in range(len_table2):
            if tables_info[my_col1][i] == tables_info[my_col2][j]:
                for col in metadata[table1]:
                    my_col = table1 + '.' + col
                    result[my_col].append(tables_info[my_col][i])
                for col in metadata[table2]:
                    my_col = table2 + '.' + col
                    if my_col == my_col2:
                        continue
                    result[my_col].append(tables_info[my_col][j])
    if not result:
        report_error("Current selection does not contain any record")
    return [table1, table2], result


def handle_cond(tables_info, table1, col1, value, rop):
    col = table1 + '.' + col1
    col_length = len(tables_info[col])
    use_rows = set()
    for i in range(col_length):
        if rop(tables_info[col][i], value):
            use_rows.add(i)
    return use_rows


def handle_cond_join(metadata, tables_info, use_rows, my_tables, lop):
    result = defaultdict(list)
    used_tables = []
    if len(use_rows) == 1:
        if use_rows[0] == set():
            report_error("Current selection does not contain any record")
        table = my_tables[0]
        for col in metadata[table]:
            my_col = table + '.' + col
            for i in use_rows[0]:
                result[my_col].append(tables_info[my_col][i])
        used_tables.append(table)
    else:
        if my_tables[0] == my_tables[1]:
            if lop == 'or':
                use_rows = use_rows[0].union(use_rows[1])
            elif lop == 'and':
                use_rows = use_rows[0].intersection(use_rows[1])
            else:
                report_error(
                    "ERROR: Logical operator in WHERE clause not supported")
            if use_rows == set():
                report_error("Current selection does not contain any record")
            table = my_tables[0]
            for col in metadata[table]:
                my_col = table + '.' + col
                for i in use_rows:
                    result[my_col].append(tables_info[my_col][i])
            used_tables.append(table)
        else:
            table1 = my_tables[0]
            table2 = my_tables[1]
            len_table1 = len(tables_info[table1 + '.' + metadata[table1][0]])
            len_table2 = len(tables_info[table2 + '.' + metadata[table2][0]])
            kll_me1 = len(use_rows[0])
            kll_me2 = len(use_rows[1])
            use_rows = [list(i) for i in use_rows]
            if use_rows[0] != [] and use_rows[1] != []:
                used_tables.append(table1)
                used_tables.append(table2)
                k = 0
                for i in range(len_table1):
                    l = 0
                    if k == kll_me1 or i != use_rows[0][k]:
                        continue
                    else:
                        k += 1
                    for j in range(len_table2):
                        if lop == 'and':
                            if l == kll_me2 or j != use_rows[1][l]:
                                continue
                            else:
                                l += 1
                        for col in metadata[table1]:
                            my_col = table1 + '.' + col
                            result[my_col].append(tables_info[my_col][i])
                        for col in metadata[table2]:
                            my_col = table2 + '.' + col
                            result[my_col].append(tables_info[my_col][j])
            elif use_rows[0] != [] and lop == 'OR':
                for col in metadata[table1]:
                    my_col = table1 + '.' + col
                    for i in use_rows[0]:
                        result[my_col].append(tables_info[my_col][i])
                used_tables.append(table1)
            elif use_rows[1] != [] and lop == 'OR':
                for col in metadata[table2]:
                    my_col = table2 + '.' + col
                    for i in use_rows[1]:
                        result[my_col].append(tables_info[my_col][i])
                used_tables.append(table2)
            else:
                report_error("Current selection does not contain any record")

    return used_tables, result


def merge_tables(result, used_tables, metadata, tables_info):
    for table in metadata.keys():
        if table in used_tables:
            continue
        if not result:
            for col in metadata[table]:
                my_col = table + '.' + col
                result[my_col] = tables_info[my_col]
        else:
            prev = list(result.keys())
            len_j = len(result[prev[0]])
            len_i = len(tables_info[table + '.' + metadata[table][0]])
            for i in range(len_i):
                for j in range(len_j):
                    for col1 in metadata[table]:
                        my_col1 = table + '.' + col1
                        result[my_col1].append(tables_info[my_col1][i])
                    if i == 0:
                        continue
                    for col2 in prev:
                        result[col2].append(tables_info[col2][j])
    return result


def display_result(cols, table_keys, selections, distinct_flag, result):
    used_cols = defaultdict(bool)
    final_selections = []
    if len(selections) == 1 and '(' in selections[0]:
        agg_func = selections[0].split('(')[0].strip().lower()
        col = selections[0].split('(')[1][:-1].strip()
        if not cols[col]:
            report_error('ERROR: No column "' + col +
                         '" present in the tables provided')
        if len(cols[col]) > 1:
            report_error("ERROR: The specified field '" + col +
                         "' could refer to more than one table")
        my_col = cols[col][0] + '.' + col
        print(my_col)
        if agg_func == 'max':
            print(max(result[my_col]))
        elif agg_func == 'min':
            print(min(result[my_col]))
        elif agg_func == 'average':
            print(sum(result[my_col]) / len(result[my_col]))
        elif agg_func == 'sum':
            print(sum(result[my_col]))
        else:
            report_error("ERROR: Unsupported aggregate function")
        return

    for selection in selections:
        if '(' in selection:
            report_error(
                "ERROR: Aggregate function cannot have other selections along with it")
        if selection == '*':
            for col in table_keys:
                if not used_cols[col] and result[col]:
                    used_cols[col] = True
                    final_selections.append(col)
            continue
        my_col = selection
        if not '.' in selection:
            if not cols[selection]:
                report_error('ERROR: No column "' + selection +
                             '" present in the tables provided')
            if len(cols[selection]) > 1:
                report_error("ERROR: The specified field '" + selection +
                             "' could refer to more than one table")
            my_col = cols[selection][0] + '.' + selection
        if not result[my_col]:
            report_error('ERROR: No column "' + my_col +
                         '" present in the tables provided')
        if not used_cols[my_col]:
            used_cols[my_col] = True
            final_selections.append(my_col)
    statements = []
    statement = ''
    for selection in final_selections:
        statement += selection + ','
    statements.append(statement[:-1])
    # All of the columns should have the same length
    res_len = len(result[final_selections[0]])
    for i in range(res_len):
        statement = ''
        for selection in final_selections:
            if not result[selection]:
                continue
            statement += str(result[selection][i]) + ','
        if not distinct_flag:
            statements.append(statement[:-1])
        else:
            if not statement[:-1] in statements:
                statements.append(statement[:-1])
    for statement in statements:
        print(statement)


def main():
    if len(sys.argv) != 2:
        report_error("Usage: python main.py <Query in double quotes>")

    # Can't do sys.argv[1].lower() cause file names will not be proper
    # Split will be imperfect, will need to handle cases where there is
    # no space after commas and so on
    query = sys.argv[1].split()
    # query = "Select * from table1, table2 where table1.B = table2.B;".split()
    if query[-1][-1] == ';':
        query[-1] = query[-1][:-1]
    elif query[-1] == ';':
        query = query[:-1]
    else:
        report_error('ERROR: Missing semi-colon at end of query')
    # Some wrong queries to be handled:
    # * Ensure that ; is present at the end of the query
    # * select *, col from table;
    # * select max(col1, col2)

    distinct_flag, join_flag, selections, tables, rel_ops, operands, lop = parse_query(
        query)
    # Selections can be *, sum, average, max and min(col), multiple columns
    # Tables are straight-forward
    # Conditions can be <, >, <=, >=, = and can have one AND or OR
    metadata, cols, tables_info = read_data(tables)
    result = defaultdict(list)
    use_rows = []
    used_tables = []
    my_tables = []
    for i in range(len(rel_ops)):
        rop = rel_ops[i]
        table1 = ''
        table2 = ''
        if len(operands[i][0]) > 1:
            table1 = operands[i][0][0]
            col1 = operands[i][0][1]
        else:
            col1 = operands[i][0][0]
        if len(operands[i][1]) > 1:
            table2 = operands[i][1][0]
            col2 = operands[i][1][1]
        else:
            col2 = operands[i][1][0]
        if table1 == '':
            if len(cols[col1]) == 0:
                report_error("ERROR: No column '" + col1 +
                             "' found in any of the tables")
            if len(cols[col1]) > 1:
                report_error(
                    "ERROR: The specified field '" + col1 + "' could refer to more than one table listed in the FROM clause")
            else:
                table1 = cols[col1][0]
        my_tables.append(table1)
        if table2 == '':
            if len(cols[col2]) > 1:
                report_error(
                    "ERROR: The specified field ''" + col2 + "' could refer to more than one table listed in the FROM clause")
            elif cols[col2] == 1:
                table2 = cols[col2][0]
            else:
                if not is_number(col2):
                    report_error("ERROR: No column '" + col2 +
                                 "' found in any of the tables")
                value = float(col2)
        if table1 != '' and not tables_info[table1 + '.' + col1]:
            report_error("ERROR: No column '" + table1 + '.' + col1 +
                         "' found")
        if table2 != '' and not tables_info[table2 + '.' + col2]:
            report_error("ERROR: No column '" + table2 + '.' + col2 +
                         "' found")
        if join_flag:
            used_tables, result = handle_join(
                metadata, tables_info, table1, col1, table2, col2)
        else:
            use_rows.append(handle_cond(tables_info, table1, col1, value, rop))

    if not join_flag and use_rows != []:
        used_tables, result = handle_cond_join(
            metadata, tables_info, use_rows, my_tables, lop)

    result = merge_tables(result, used_tables,
                          metadata, tables_info)

    display_result(cols, tables_info.keys(), selections, distinct_flag, result)


if __name__ == "__main__":
    main()
