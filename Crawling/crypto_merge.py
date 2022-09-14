import jdatetime
import os


if __name__ == "__main__":

    time = jdatetime.datetime.now()
    to_be_deleted = []
    with open(f'/opt/airflow/logs/{time.year}-{time.month}-{time.day}.tsv', 'w') as f_w:
        for entry in os.listdir('/opt/airflow/logs/'):
            if entry.endswith('.tsv') and entry.startswith(f'{time.year}-0{time.month}-{time.day if time.day // 10 != 0 else "0"+str(time.day)}'):
                with open(f'/opt/airflow/logs/{entry}', 'r') as f_r:
                    data = f_r.read()
                    data = '\n'.join(data.split('\n')[1:])
                    print(data)
                    f_w.write(f'{data}\n')
                to_be_deleted.append(f'/opt/airflow/logs/{entry}')

    print(' '.join(to_be_deleted))
