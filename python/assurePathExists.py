import glob
import os
import shutil


def assure_path_exists(self, path):
    dir = os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)
    else:
        try:
            files = glob.glob("{0}/*".format(path))
            for f in files:
                os.remove(f)
        except:
            files = glob.glob("{0}/*".format(path))
            for f in files:
                shutil.rmtree(f)