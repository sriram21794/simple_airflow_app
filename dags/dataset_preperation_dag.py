import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago



args = {
    'owner': 'sriram',
    'email': ['sriram21794@gmail.com']
}


with DAG(
    'dataset_preperation_dag',
    default_args=args,
    start_date=days_ago(2)
    ) as dag:
    
    if Variable.get("dataset_folder", default_var=None) is not None:
        
        working_dir = __file__.rsplit(os.sep,2)[0]
        interpreter = "python"
        folder = Variable.get("dataset_folder")

        download_task = BashOperator(
            task_id='download',
            bash_command=f'{interpreter} {os.path.join(working_dir, "download.py")} --folder={folder}',
        )

        strat_arg_names = ["test_size"]
        strat_args = f' --input_txt_file={os.path.join(folder, "download.txt")}'
        for arg_name in strat_arg_names:
            if Variable.get(arg_name, default_var=None) is not None:
                strat_args += f' --{arg_name}={Variable.get(arg_name)}'

        stratify_task = BashOperator(
            task_id='stratify',
            bash_command=f'{interpreter} {os.path.join(working_dir, "stratifier.py")} {strat_args}',
        )

        vis_arg_names = ["top"]
        vis_args = f' --input_txt_file={os.path.join(folder, "download.txt")}'
        for arg_name in vis_arg_names:
            if Variable.get(arg_name, default_var=None) is not None:
                vis_args += f' --{arg_name}={Variable.get(arg_name)}'

        visualize_task = BashOperator(
            task_id='visualize',
            bash_command=f'{interpreter} {os.path.join(working_dir, "visualize.py")} {vis_args}',
        )

        vis_arg_names = ["top"]
        vis_args = f' --input_txt_file={os.path.join(folder, "download_train.txt")}'
        for arg_name in vis_arg_names:
            if Variable.get(arg_name, default_var=None) is not None:
                vis_args += f' --{arg_name}={Variable.get(arg_name)}'

        visualize_train_task = BashOperator(
            task_id='visualize_train',
            bash_command=f'{interpreter} {os.path.join(working_dir, "visualize.py")} {vis_args}',
        )

        vis_arg_names = ["top"]
        vis_args = f' --input_txt_file={os.path.join(folder, "download_test.txt")}'
        for arg_name in vis_arg_names:
            if Variable.get(arg_name, default_var=None) is not None:
                vis_args += f' --{arg_name}={Variable.get(arg_name)}'

        visualize_test_task = BashOperator(
            task_id='visualize_test',
            bash_command=f'{interpreter} {os.path.join(working_dir, "visualize.py")} {vis_args}',
        )


        
        train_tf_task = BashOperator(
            task_id='tfrecords_train',
            bash_command=f'{interpreter} {os.path.join(working_dir, "prepare_tfrecords.py")}' 
                            + f' --input_txt_file={os.path.join(folder, "download_train.txt")}'
                            +  ' --output_tfrecord_path={{ dag_run.conf["train.output_tfrecord_path"]}}',
        )

        test_tf_task = BashOperator(
            task_id='tfrecords_test',
            bash_command=f'{interpreter} {os.path.join(working_dir, "prepare_tfrecords.py")}' 
                            + f' --input_txt_file={os.path.join(folder, "download_train.txt")}'
                            +  ' --output_tfrecord_path={{ dag_run.conf["test.output_tfrecord_path"] }}',
        )

        # Creating dependencies
        download_task >> [ visualize_task, stratify_task ]
        stratify_task >> [visualize_train_task, 
                            visualize_test_task, 
                            train_tf_task, 
                            test_tf_task]
                            