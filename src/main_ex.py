import os
import re
import sqlite3

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

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
    for k in range(1000):
        with open(f'data/000000000000000000000000000_fake_log_{k}.log', 'w') as file:
            file.write('this is a fake log file')

    fake_log_paths = ['data/' + path for path in os.listdir(path='data') if 'fake' in path]
    # for path in fake_log_paths:
    #     os.remove(path)

    log_paths = ['data/' + path for path in os.listdir(path='data') if '.log' in path] + fake_log_paths
    count = 0
    limit = len(log_paths)

    columns = ['session', 'code', 'model', 'date', 'info', 'log_path']
    df = pd.DataFrame(columns=columns)

    error_paths = []

    with plt.style.context(("seaborn", "ggplot")):
        plt.ion()
        fig = plt.figure(figsize=(20, 8))
        axs = gridspec.GridSpec(ncols=2, nrows=2, figure=fig)
        ax1 = fig.add_subplot(axs[0, :])
        ax2 = fig.add_subplot(axs[1, 0])
        ax3 = fig.add_subplot(axs[1, 1])

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
                    except ValueError as e:
                        error_paths.append(error_paths)

            if count % 50 == 0:
                df_temp = df.copy()
                df_temp['algo'] = df_temp['info'].apply(lambda x: get_algorithm(x))
                df_temp['message'] = df_temp['info'].apply(lambda x: x.split(' ')[0])

                with sqlite3.connect(":memory:") as conn:
                    df_temp[df_temp.message == 'AlgorithmStarted'].to_sql(name="logs", con=conn, index=False)

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
                fig.suptitle(f'Sessions processed: {count}')

                top10 = execute_sql(ex1_sql_query, conn).iloc[:10]
                top10.sort_values('pattern_counts').plot.barh(ax=ax1, legend=False, color='grey')
                ax1.set_yticklabels(top10['pattern'].values[::-1], fontsize=6, wrap=True)
                ax1.set_ylabel('Algorithm sequences used in session')
                ax1.set_xlabel('Counts')
                ax1.set_title('Top 10 most popular algorithm sequences')

                ex2_sql_query = """
                        select algo
                        from logs
                    """
                temp = execute_sql(ex2_sql_query, conn)
                temp.algo.value_counts()[:10].plot(kind='bar', ax=ax2, color='green')
                ax2.set_xlabel('Algorithms')
                ax2.set_ylabel('Counts')
                ax2.set_title('Distribution of top 10 popular algorithms in all processed sessions')

                ax3.scatter(count, len(error_paths), color='red')
                ax3.set_title('Logs that are being processed with error')
                ax3.set_ylabel('Error logs')
                ax3.set_xlabel('Sessions processed')

                fig.canvas.draw()
                fig.canvas.flush_events()
            count += 1

    top10.to_csv('../output/top10_0.csv', index=False)
    plt.savefig('../output/dashboard.png')
