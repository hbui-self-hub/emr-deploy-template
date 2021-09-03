import os

def join_path(*paths):
    p = sum([_p.strip('/').split('/') for _p in paths], [])
    return os.path.join(*p)
