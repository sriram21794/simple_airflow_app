from absl import app
from absl import flags
from absl import logging

from time import sleep
import string
import random
import os

flags.DEFINE_string(
    'dataset_folder',
    default=None,
    help='Folder where dataset will be downloaded to')


FLAGS = flags.FLAGS

def main(_):
    logging.info("Downloading content ...")
    
    # Using RANDOM_SEED and env variable so that it we could 
    # figure out a way to change env variable from airflow ui
    random_seed = os.environ.get("RANDOM_SEED", 22)     
    random.seed(random_seed)
    logging.info(f"Setting random seed to {random_seed}")
    
    sleep(random.choice(list(range(1, 5))))
    
    file_path = os.path.join(FLAGS.dataset_folder, "download.txt")
    with open(file_path, "w") as fp:
        fp.write("".join(str(random.choice(string.ascii_lowercase)) for _ in range(10000)))
    
    logging.info(f"Downloaded to {file_path}")

if __name__ == '__main__':
    app.run(main)