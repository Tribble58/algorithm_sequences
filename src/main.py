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
    # Collect all paths that contain '.log' substring
    log_paths = ['../data/' + path for path in os.listdir(path='../data') if '.log' in path]

    # This is for the case when we need to process limited number of logs: to check whether script
    # is executable, for instance
    count = 0
    limit = len(log_paths)

    # Create pandas DataFrame for raw and transformed data
    columns = ['session', 'code', 'model', 'date', 'info', 'log_path']
    df = pd.DataFrame(columns=columns)

    # Logs can be of various formats, we need to check whether some files were processed with error
    error_paths = []

    print('Log processing has began')
    for log_path in tqdm(log_paths):

        # If limit is defined, stop when limit is reached
        if count == limit:
            break
        with open(log_path) as file:
            log = file.read()
        for row in log.split('\n'):
            if row != '':
                # Template search and replace
                elements = re.sub('[\[\]]', ' ', row).strip().split('  ')
                try:
                    df.loc[len(df)] = elements + [log_path]
                except ValueError:
                    # Process error logs
                    error_paths.append(log_path)
        count += 1
    print('=' * 50)
    print('Log processing has ended')

    # Define algorithm and message (Algorithm Started, Algorithm Ended) in a separate column
    df['algo'] = df['info'].apply(lambda x: get_algorithm(x))
    df['message'] = df['info'].apply(lambda x: x.split(' ')[0])

    # Connect to sql instance for convenient data analysis
    with sqlite3.connect(":memory:") as conn:
        df[df.message == 'AlgorithmStarted'].to_sql(name="logs", con=conn, index=False)

    # Firstly, we concatenate algorithms within each session (log_path) and call it pattern (aka algorithm sequence)
    # Secondly we group by sessions to see distinct patterns
    # Finally, we count number of pattern occurrences and order them descendingly
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

    # Execute SQL script and save the result to the .csv file
    top10 = execute_sql(ex1_sql_query, conn)
    top10.to_csv('../output/top10.csv', index=False)

    # Display the results
    print(f'Total amount of sessions: {sum(top10.pattern_counts)}')

    print('Top-10 most popular algorithm sequences: ')
    for algo_seq, number in top10.iloc[:10].values:
        print(f'Algorithm sequence: {algo_seq}, number of occurrences: {number}')
