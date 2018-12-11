import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

DATA_FOLDER = "data"


def decorate_file(input_path, output_path):
    with open(input_path, "r") as in_file:
        line = in_file.read()

    with open(output_path, "w") as out_file:
        out_file.write("My "+line)


default_args = {
    "owner": "lorenzo",
    "depends_on_past": False,
    "start_date": datetime(2018, 9, 20),
    "email": ["l.peppoloni@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


dag = DAG(
    "multiple_files_dag",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    )

for i in range(10):
    output_one_path = os.path.join(DATA_FOLDER, "output_one_{:d}.txt".format(i))
    output_two_path = os.path.join(DATA_FOLDER, "output_two_{:d}.txt".format(i))

    t1 = BashOperator(
        task_id="print_file_{:d}".format(i),
        bash_command='echo "pipeline" > {}'.format(output_one_path),
        dag=dag)

    t2 = PythonOperator(
        task_id="decorate_file_{:d}".format(i),
        python_callable=decorate_file,
        op_kwargs={"input_path": output_one_path, "output_path": output_two_path},
        dag=dag)

    t2.set_upstream(t1)
