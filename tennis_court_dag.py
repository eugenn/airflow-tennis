from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

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
        schedule_interval=None,  # or a CRON if desired, but for continuous sensor, often None
        catchup=False
) as dag:
    # ----------------------------------------------------------------------------
    # 1) Sensor task: check if a new image file exists in the external folder
    # ----------------------------------------------------------------------------
    # Example: We’re looking for any .jpg file in /path/to/watched_folder
    # and we’ll poke every 10 seconds.
    file_sensor = FileSensor(
        task_id='wait_for_image_file',
        filepath='*.jpg',  # could be a single file or wildcard
        fs_conn_id='fs_default',  # must be defined in Airflow Connections
        poke_interval=10,  # 10 seconds
        timeout=60 * 60 * 24,  # how long to keep poking (1 day here)
        mode='reschedule',  # or 'poke'; 'reschedule' is often better
    )

    # ----------------------------------------------------------------------------
    # 2) K8s Pod Operator to run the container
    # ----------------------------------------------------------------------------
    # NOTE: Instead of "docker run ...", we simply run the image directly in K8s.
    # If GPU is required, you must configure GPU resource requests in your cluster.
    #
    # If you REALLY need to replicate exactly "docker run --gpus all ...",
    # you'd need a container that has Docker installed, privileged mode, and
    # all the GPU drivers set up. That’s advanced. Typically, you just run
    # the container *as* the Pod with GPU resources requested.
    #
    # This example just shows a direct run. Adjust volumes/commands for your needs.

    # Example volume/volume_mount for output
    from kubernetes.client import V1Volume, V1VolumeMount, V1HostPathVolumeSource

    output_volume = V1Volume(
        name='output-volume',
        host_path=V1HostPathVolumeSource(path='/home/user/tennis_output')
    )
    output_volume_mount = V1VolumeMount(
        name='output-volume',
        mount_path='/output',
        read_only=False
    )

    # Command & arguments for the container
    # e.g. the entrypoint is the tennis-court-detector itself,
    # passing --input_path to point at an image.
    # Adjust these as needed for your container's entrypoint.
    run_detector = KubernetesPodOperator(
        task_id='run_tennis_court_detector',
        name='tennis-court-detector-job',
        namespace='default',  # or your namespace
        image='tennis-court-detector:latest',  # your built Docker image
        cmds=['/bin/bash', '-c'],
        arguments=[
            # If your container has an ENTRYPOINT, you might just pass these
            # as arguments. Or if you have to run the python script, adapt below.
            'tennis-court-detector --input_path /data/image/0oEvj5HuYMp_frame_2.jpg'
        ],
        volumes=[output_volume],
        volume_mounts=[output_volume_mount],
        # If you need GPU, you may need to specify
        # resources={'limits': {'nvidia.com/gpu': 1}},
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # ----------------------------------------------------------------------------
    # Task Dependencies
    # ----------------------------------------------------------------------------
    file_sensor >> run_detector