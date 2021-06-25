import argparse

from processors import get_stream_processor


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--jobname", "-jobname", help="Spark job name", required=True)
    parser.add_argument("--source", "-source", help="Data source", required=True)
    args = parser.parse_args()

    stream_processor = get_stream_processor(args.source, args.jobname)
    stream_processor.process()
    stream_processor.write_stream()
