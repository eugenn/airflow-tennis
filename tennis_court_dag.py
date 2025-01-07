from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.sensors.filesystem import FileSensor
from kubernetes.client import V1Volume, V1VolumeMount, V1HostPathVolumeSource
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
    # file_sensor = FileSensor(
    #     task_id='wait_for_image_file',
    #     filepath='/opt/airflow/external_input/*.jpg',  # Adjust to your folder path
    #     fs_conn_id='fs_default',
    #     poke_interval=10,
    #     timeout=60 * 60 * 24,
    #     mode='poke',
    #     do_xcom_push=True,  # Push the detected file path to XCom
    # )

    # ----------------------------------------------------------------------------
    # 2) K8s Pod Operator: Run the container with the detected file
    # ----------------------------------------------------------------------------
    run_detector = KubernetesPodOperator(
        task_id='run_tennis_court_detector',
        name='tennis-court-detector-job',
        namespace='airflow',  # Ensure it matches your namespace
        image='tennis-court-detector:latest',
        cmds=['python3', 'scripts/main.py'],
        arguments=[
            '--input_path', "./data/image/0iY7kaPVJph_frame_3.jpg",
            '--output_path', '/output/court.png',
        ],
        volumes=[
            # 1) Input Volume
            V1Volume(
                name='external-input-volume',
                host_path=V1HostPathVolumeSource(
                    path='/home/enekhai/workspace/projects/sai/airflow-k8s/input'
                )
            ),
            # 2) Output Volume
            V1Volume(
                name='external-output-volume',
                host_path=V1HostPathVolumeSource(
                    path='/home/enekhai/workspace/projects/sai/airflow-k8s/output'
                )
            ),
        ],
        volume_mounts=[
            # 1) Mount for Input
            V1VolumeMount(
                name='external-input-volume',
                mount_path='/opt/airflow/external_input',
                read_only=False
            ),
            # 2) Mount for Output
            V1VolumeMount(
                name='external-output-volume',
                mount_path='/output',  # This will match the --output_path argument
                read_only=False
            ),
        ],
        # security_context={
        #     'runAsUser': 50000,
        #     'runAsGroup': 50000,
        #     'fsGroup': 50000
        # },
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # ----------------------------------------------------------------------------
    # Task Dependencies
    # ----------------------------------------------------------------------------
    run_detector
