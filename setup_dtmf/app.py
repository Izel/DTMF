# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from typing import Callable
import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions


def run(
    beam_options: PipelineOptions,
    options: argparse,
    test: Callable[[beam.PCollection], None] = lambda _: None,
) -> None:
    with beam.Pipeline(options=beam_options) as p:
        p | "ReadFromPubSub" >> beam.ReadFromPubSub(options.input_topic)
        # Used for testing only.
        test(options)
