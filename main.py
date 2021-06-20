import argparse

from processors import get_stream_processor

from kafka_utilis.common.config import SOURCES


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--jobname", "-jobname", help="Spark job Name", required=True)
    args = parser.parse_args()

    for source in SOURCES:
        stream_processor = get_stream_processor(source, args.jobname)
        stream_processor.process()
        stream_processor.write_stream()
