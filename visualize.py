
from absl import app
from absl import flags
from absl import logging

from matplotlib import pyplot as plt
import seaborn as sns

import os
from collections import Counter
from operator import itemgetter

flags.DEFINE_string(
    'input_txt_file',
    default=None,
    help='Text File having input')

flags.DEFINE_integer(
    'top',
    default=10,
    help='Top k to consider for visualization')

FLAGS = flags.FLAGS

def main(_):
    logging.info("Visualizing...")

    with open(FLAGS.input_txt_file) as fp:
        most_commons = Counter(fp.read()).most_common(FLAGS.top)
    containing_folder, file_name = FLAGS.input_txt_file.rsplit(os.sep, 1)
    file_prefix = file_name.rsplit(".", 1)[0]
    
    plt.figure(figsize=(10,5))
    sns.barplot(list(map(itemgetter(0), most_commons)), 
                list(map(itemgetter(1), most_commons)), alpha=1)
    plt.title('Char Distribution', )
    plt.ylabel('Number of Occurrences', fontsize=12)
    plt.xlabel('Char', fontsize=12)
    plt.savefig(os.path.join(containing_folder,  f"{file_prefix}_plot.jpg"))

if __name__ == '__main__':
    app.run(main)