"""
Data processing pipeline
"""

from datetime import datetime
import argparse
import logging
import pprint

from tr_aisha_for_product_recommendation import pipeline


logging.getLogger().setLevel(logging.INFO)


def main(
    project: str,
    gcs_bucket: str,
    region: str,
    machine_type: str,
    max_num_workers: str,
    runner: str,
    job_timestamp: str,
    **kwargs
):
    pipeline_args_dict = {
        "job_name": f"tr-data-pipeline-{job_timestamp}",
        "machine_type": machine_type,
        "num_workers": "1",
        "max_num_workers": max_num_workers,
        "autoscaling_algorithm": "THROUGHPUT_BASED",
        "runner": runner,
        "setup_file": "./setup.py",
        "project": project,
        "region": region,
        "gcs_location": f"gs://{gcs_bucket}",
        "temp_location": f"gs://{gcs_bucket}/temp",
        "staging_location": f"gs://{gcs_bucket}/staging",
        "save_main_session": "False",
        "streaming": "False",
    }

    # Convert the dictionary to a list of (argument, value) tuples and flatten the list.
    pipeline_args = [(f"--{k}", v) for k, v in pipeline_args_dict.items()]
    pipeline_args = [x for y in pipeline_args for x in y]

    logging.info(
        f"Executing beam pipeline with args:\n{pprint.pformat(pipeline_args_dict)}"
    )

    pipeline(
        pipeline_args,
        **kwargs
    )


def parse_arguments():
    job_timestamp = datetime.utcnow().strftime("%y%m%d-%H%M%S")
    logging.info("Job timestamp is", job_timestamp)
    
    parser = argparse.ArgumentParser(
        description="Beam pipeline for generating TFRecords from a pandas dataframe."
    )
    parser.add_argument(
        "-p",
        "--project",
        default="technology-robot",
        type=str,
        help="The name of the GCP project.",
    )
    parser.add_argument(
        "-b",
        "--gcs-bucket",
        default="tr-aisha-for-product-recommendation-data-pipeline",
        type=str,
        help="The Google Cloud Storage bucket name.",
    )
    parser.add_argument(
        "-reg", "--region", default="us-central1", type=str, help="The GCP region.",
    )
    parser.add_argument(
        "-m",
        "--machine-type",
        type=str,
        default="e2-standard-2",
        help="Machine type for the Dataflow workers.",
    )
    parser.add_argument(
        "-w",
        "--max-num-workers",
        default="5",
        type=str,
        help="Number of maximum workers for Dataflow",
    )
    parser.add_argument(
        "-r",
        "--runner",
        type=str,
        choices=["DirectRunner", "DataflowRunner"],
        help="The runner for the pipeline.",
    )
    parser.add_argument(
        "-is",
        "--input-source",
        type=str,
        default="gs://tr-aisha/product-recommendation/sample-aldi/output.csv",
        help="CSV input source",
    )
    parser.add_argument(
        "-cn",
        "--collection-name",
        type=str,
        default="tr-aisha-pd-sample-aldi",
        help="DB name for mongodb and qdrant",
    )
    parser.add_argument(
        "-md",
        "--metadata-dir",
        type=str,
        default="gs://tr-aisha/product-recommendation/sample-aldi/metadata",
        help="Metadata parent directory",
    )
    parser.add_argument(
        "-v",
        "--version",
        type=str,
        default="v1",
        help="Version name",
    )
    parser.add_argument(
        "--qdrant-api-url",
        type=str,
        required="True",
    )
    parser.add_argument(
        "--qdrant-api-key",
        type=str,
        required="True",
    )
    parser.add_argument(
        "--openai-api-key",
        type=str,
        required="True",
    )
    parser.add_argument(
        "--mongo-uri",
        type=str,
        required="True",
    )
    parser.add_argument(
        "--state-path",
        type=str,
        default="gs://tr-aisha/product-recommendation/sample-aldi/metadata/states/" + job_timestamp + ".json",
        help="state path to write"
    )
    parser.add_argument(
        "--job_timestamp",
        type=str,
        default=job_timestamp,
        help="Pipeline job timestamp"
    )
    args = parser.parse_args()
    return vars(args)


if __name__ == "__main__":
    args = parse_arguments()
    main(**args)
