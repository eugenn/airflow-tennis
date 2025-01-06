from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

# --------------------------------------------------------------------------------
# Default args for the DAG
# --------------------------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
}

# --------------------------------------------------------------------------------
# DAG Definition
# --------------------------------------------------------------------------------
with DAG(
        dag_id='image_detector_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:

    # ----------------------------------------------------------------------------
    # 1) Sensor Task: Detect if a new image file exists
    # ----------------------------------------------------------------------------
    file_sensor = FileSensor(
        task_id='wait_for_image_file',
        filepath='/path/to/watched_folder/*.jpg',  # Adjust to your folder path
        fs_conn_id='fs_default',
        poke_interval=10,
        timeout=60 * 60 * 24,
        mode='poke',
    )

    # ----------------------------------------------------------------------------
    # 2) K8s Pod Operator: Run the container with the detected file
    # ----------------------------------------------------------------------------
    run_detector = KubernetesPodOperator(
        task_id='run_tennis_court_detector',
        name='tennis-court-detector-job',
        namespace='default',
        image='tennis-court-detector:latest',
        cmds=['python3', 'scripts/main.py'],
        arguments=[
            '--input_path', "{{ task_instance.xcom_pull(task_ids='wait_for_image_file') }}",
            '--output_path', '/output/court.png',
        ],
        volumes=[
            V1Volume(
                name='output-volume',
                host_path=V1HostPathVolumeSource(path='/home/enekhai/workspace/projects/sai/tennis_court_detector')
            )
        ],
        volume_mounts=[
            V1VolumeMount(
                name='output-volume',
                mount_path='/output',
                read_only=False
            )
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # ----------------------------------------------------------------------------
    # Task Dependencies
    # ----------------------------------------------------------------------------
    file_sensor >> run_detector