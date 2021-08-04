

from absl import app
from absl import flags
from absl import logging

import random
from time import sleep
import string

flags.DEFINE_string(
    'input_txt_file',
    default=None,
    help='Text File having input')

flags.DEFINE_string(
    'output_tfrecord_path',
    default=None,
    help='Tfrecord path to dump tfrecords')

FLAGS = flags.FLAGS

def main(_):
    logging.info("Creating tfrecords...")

    with open(FLAGS.output_tfrecord_path, "w") as fp:
        sleep(random.choice(list(range(1, 5))))
        fp.write("".join(str(random.choice(string.ascii_lowercase)) for _ in range(100)))
    
    logging.info(f"Created tfrecord: {FLAGS.output_tfrecord_path}")

if __name__ == '__main__':
    app.run(main)