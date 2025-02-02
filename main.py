# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

import time

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

from setup_dtmf import app


if __name__ == "__main__":
    import argparse
    import logging

    logging.getLogger().setLevel(logging.INFO)

    # Parse the command line arguments
    parser = argparse.ArgumentParser(
        description="Read messages from Pub/Sub to BQ"
    )
    parser.add_argument(
        "--project", required=True, help="Specify Google Cloud project"
    )
    parser.add_argument(
        "--region", required=True, help="Specify Google Cloud region"
    )
    parser.add_argument(
        "--staging_location",
        required=True,
        help="Specify Cloud Storage bucket for staging",
    )
    parser.add_argument(
        "--temp_location",
        required=True,
        help="Specify Cloud Storage bucket for temp",
    )
    parser.add_argument(
        "--runner", required=True, help="Specify Apache Beam Runner"
    )
    parser.add_argument(
        "--input_topic", required=True, help="Input Pub/Sub Topic"
    )
    parser.add_argument(
        "--table_name",
        required=True,
        help="BigQuery table name for aggregate results",
    )

    opts, beam_opts = parser.parse_known_args()

    # Setting Pipeline Options
    options = PipelineOptions(
        beam_opts,
        save_main_session=True,
        streaming=True,
        setup_file="./setup.py",
    )
    options.view_as(GoogleCloudOptions).project = beam_opts.project
    options.view_as(GoogleCloudOptions).region = beam_opts.region
    options.view_as(
        GoogleCloudOptions
    ).staging_location = beam_opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = beam_opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = "{0}{1}".format(
        "streaming-twitter-messages", time.time_ns()
    )
    options.view_as(StandardOptions).runner = opts.runner

    # Runs the Pipeline
    app.run(beam_opts=beam_opts, opts=opts)
