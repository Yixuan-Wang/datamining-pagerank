import functools
import pickle

import numpy as np
import pyarrow.dataset as ds
import scipy.sparse as sparse
from rich.console import Console
from rich.progress import Progress

data = ds.dataset("data/wikipedia-links-index/")

# 读取页面总数，以确定邻接矩阵的 shape
with open("title.pickle", "rb") as file:
    titles = pickle.load(file)
    SIZE = len(titles) # 1911167
    del titles


def get_arr():
    """
    计算邻接稀疏矩阵
    """
    with Progress(console=Console()) as progress:
        task = progress.add_task(
            "Initializing sparse array...", total=data.count_rows(filter=ds.field("link") > 0)
        )
        for idx, batch in enumerate(data.to_batches(filter=ds.field("link") > 0)):
            progress.update(
                task, description=f"Initializing sparse array with batch {idx}..."
            )
            rows = batch["title"].to_numpy()
            cols = batch["link"].to_numpy()
            initial_values = np.ones_like(rows, dtype=np.float64)
            arr = sparse.coo_array((initial_values, (rows, cols)), shape=(SIZE, SIZE))
            yield arr
            progress.advance(task, len(batch))


# 把每一分块的邻接矩阵累加
arr = functools.reduce(np.add, get_arr()) # type: ignore


with open("sparse.pickle", "wb") as file:
    pickle.dump(arr, file)

