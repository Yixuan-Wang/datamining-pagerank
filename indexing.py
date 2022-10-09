import pickle

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from rich.console import Console
from rich.progress import Progress

# 读入 Parquet 表
data = ds.dataset("data/wikipedia-links", format="parquet")

# 取出 title
table = data.to_table(columns=["title"])

# 提取出全部不重复的 title 并排序
unique_titles = pa.compute.unique(table["title"])
unique_titles_id = pa.compute.array_sort_indices(unique_titles)

# 构建哈希表
hashmap_titles = dict(
    map(
        lambda x: (x[0].as_py(), x[1].as_py()), zip(unique_titles, unique_titles_id)
    )
)

with open("title.pickle", "wb") as file:
    pickle.dump(hashmap_titles, file)

@np.vectorize
def map_title_to_id(title: str) -> int:
    """
    把标题转换为 `int64` 范围内的非负编号用于矩阵运算，如果找不到该标题则返回 -1。
    """
    return hashmap_titles.get(title, -1)

# 分块读取 Parquet 表入内存，并将 title 和 link 全部转换为整型编号
with Progress(console=Console()) as progress:
    task = progress.add_task("Indexing...", total=data.count_rows())
    for idx, batch in enumerate(data.to_batches()):
        progress.update(
            task, description=f"Indexing batch {idx}", advance=len(batch)
        )

        mapped = map_title_to_id([batch["title"], batch["link"]])
        result_shard = pa.table({"title": mapped[0], "link": mapped[1]})
        pq.write_to_dataset(result_shard, "data/wikipedia-links-index")
