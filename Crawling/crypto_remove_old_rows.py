import pandas as pd
import jdatetime


if __name__ == "__main__":

    time = jdatetime.datetime.now()
    # time = jdatetime.datetime(1401, 3, 31, 12, 00, 00)
    df = pd.read_csv(
        '/opt/airflow/logs/temp.tsv', sep='\t',
        names=['title_fa', 'title_en', 'symbol', 'price_usdt', 'price_irr', 'delta_percentage', 'time']
    )
    df['old'] = df.apply(lambda row: time - (jdatetime.datetime(
        int(row['time'].split()[0].split('/')[0]),
        int(row['time'].split()[0].split('/')[1]),
        int(row['time'].split()[0].split('/')[2]),
        int(row['time'].split()[1].split(':')[0]),
        int(row['time'].split()[1].split(':')[1])
    )) > jdatetime.timedelta(hours=1), axis=1)
    df = df.drop(df[df.old].index)
    df = df.drop('old', axis=1)
    df.to_csv(f'/opt/airflow/logs/{str(time).replace(" ", "_").replace(":","-")}.tsv', sep='\t')





