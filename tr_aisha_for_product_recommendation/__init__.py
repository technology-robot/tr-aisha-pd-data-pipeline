import apache_beam as beam

from tr_aisha_for_product_recommendation.preprocess import (
    filter,
    refine,
)
from tr_aisha_for_product_recommendation.process import Llamaindexing
from tr_aisha_for_product_recommendation.schema import SourceSchema


def pipeline(
        pipeline_args,
        input_source,
        collection_name,
        metadata_dir,
        version,
        openai_api_key,
        qdrant_api_key,
        qdrant_api_url,
        mongo_uri,
        state_path,
    ):
    with beam.Pipeline(argv=pipeline_args) as pipeline:
        _ = (
            pipeline
            | "Read" >> beam.io.ReadFromCsv(input_source)
            | "Filter" >> beam.Filter(filter)
            | "Refine" >> beam.Map(refine).with_output_types(SourceSchema)
            | "Llamaindexing" >> beam.ParDo(
                Llamaindexing(
                    collection_name=collection_name,
                    metadata_dir=metadata_dir,
                    version=version,
                    openai_api_key=openai_api_key,
                    qdrant_api_key=qdrant_api_key,
                    qdrant_api_url=qdrant_api_url,
                    mongo_uri=mongo_uri,
                    state_path=state_path,
                )
            )
        )
