from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.sensors.filesystem import FileSensor
from kubernetes.client import V1Volume, V1VolumeMount, V1HostPathVolumeSource
from kubernetes.client import V1ResourceRequirements
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
        # The correct way to specify container resource requests/limits:
        container_resources=V1ResourceRequirements(
            requests={'nvidia.com/gpu': '1'},  # or however many you need
            limits={'nvidia.com/gpu': '1'}
        ),
        # Mount volumes
        volumes=[
            V1Volume(
                name='my-image-dir',
                host_path=V1HostPathVolumeSource(
                    # The folder on your K8s worker node that contains the images
                    path='/home/enekhai/workspace/projects/sai/data/image'
                )
            ),
            V1Volume(
                name='my-output-dir',
                host_path=V1HostPathVolumeSource(
                    path='/home/enekhai/workspace/projects/sai/tennis_court_detector'
                )
            ),
        ],
        volume_mounts=[
            V1VolumeMount(
                name='my-image-dir',
                # Mount it under /app/data/image if that’s the container’s expected location
                mount_path='/app/data/image'
            ),
            V1VolumeMount(
                name='my-output-dir',
                mount_path='/output'
            ),
        ],

        # security_context={
        #     'runAsUser': 50000,
        #     'runAsGroup': 50000,
        #     'fsGroup': 50000
        # },
        get_logs=True,
        is_delete_operator_pod=False,
    )

    # ----------------------------------------------------------------------------
    # Task Dependencies
    # ----------------------------------------------------------------------------
    run_detector
