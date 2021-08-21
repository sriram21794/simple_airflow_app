from absl import app
from absl import flags
from absl import logging

from time import sleep

import random
import os

flags.DEFINE_string(
    'input_txt_file',
    default=None,
    help='Text File having input')

flags.DEFINE_float(
    'test_size',
    default=0.2,
    help='Text File having input')

FLAGS = flags.FLAGS

def main(_):
    logging.info("Stratifying the dataset into train test split")
    
    # Using RANDOM_SEED and env variable so that it we could 
    # figure out a way to change env variable from airflow ui
    random_seed = os.environ.get("RANDOM_SEED", 22) 
    random.seed(random_seed)
    logging.info(f"Setting random seed to {random_seed}")

    containing_folder, file_name = FLAGS.input_txt_file.rsplit(os.sep, 1)
    file_prefix = file_name.rsplit(".", 1)[0]
    
    with open(FLAGS.input_txt_file) as fp:
        data = fp.read()
    indices = list(range(len(data)))
    random.shuffle(indices)
    
    train_indices, test_indices = indices[:-int(len(indices) * FLAGS.test_size)], \
        indices[-int(len(indices) * FLAGS.test_size):]
    
    logging.info(f"Train Set Size: {len(train_indices)}")
    logging.info(f"Test Set Size: {len(test_indices)}")
    
    train_file = os.path.join(containing_folder,  f"{file_prefix}_train.txt")
    test_file = os.path.join(containing_folder,  f"{file_prefix}_test.txt")
    
    with open(train_file, "w") as fp:
        fp.write(''.join(map(str, filter(data.__getitem__, train_indices))))

    with open(test_file, "w") as fp:
        fp.write(''.join(map(str, filter(data.__getitem__, test_indices))))

    logging.info(f"Stratified to {train_file} and {test_file} files")
    

if __name__ == '__main__':
    app.run(main)