import re
import os
import sqlite3

import pandas as pd

from tqdm import tqdm


def get_algorithm(x):
    if 'result' in x.lower():
        return ' '.join(x.split('.')[0].split(' ')[1:])
    elif 'count' in x.lower():
        return ' '.join(x.split(',')[0].split(' ')[1:])
    else:
        return ' '.join(x.split('|')[0].split(' ')[1:])


def execute_sql(query, connection):
    return pd.read_sql(query, con=connection)


if __name__ == '__main__':
    log_paths = ['../data/' + path for path in os.listdir(path='../data') if '.log' in path]
    count = 0
    limit = len(log_paths)

    columns = ['session', 'code', 'model', 'date', 'info', 'log_path']
    df = pd.DataFrame(columns=columns)

    error_paths = []

    for log_path in tqdm(log_paths):
        if count == limit:
            break
        with open(log_path) as file:
            log = file.read()
        for row in log.split('\n'):
            if row != '':
                elements = re.sub('[\[\]]', ' ', row).strip().split('  ')
                try:
                    df.loc[len(df)] = elements + [log_path]
                except ValueError:
                    error_paths.append(log_path)
        count += 1

    df['algo'] = df['info'].apply(lambda x: get_algorithm(x))
    df['message'] = df['info'].apply(lambda x: x.split(' ')[0])

    with sqlite3.connect(":memory:") as conn:
        df[df.message == 'AlgorithmStarted'].to_sql(name="logs", con=conn, index=False)

    ex1_sql_query = """
                        with foo1 as (
                            with foo as (
                                select log_path, group_concat(algo, ' -> ') over(partition by log_path) pattern
                                from logs)
                            select log_path, pattern
                            from foo
                            group by log_path)
                        select pattern, count(pattern) pattern_counts
                        from foo1
                        group by pattern
                        order by 2 desc;
                    """

    top10 = execute_sql(ex1_sql_query, conn)
    top10.to_csv('../output/top10.csv', index=False)

    print(f'Total amount of sessions: {sum(top10.pattern_counts)}')

    print('Top-10 most popular algorithm sequences: ')
    for algo_seq in top10.pattern.iloc[:10].values:
        print(algo_seq)
