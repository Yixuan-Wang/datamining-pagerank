import pickle


# import altair as alt
import numpy as np
import numpy.typing as npty
import pandas as pd
from rich import print
from rich.console import Console
from rich.progress import Progress

with open("sparse.pickle", "rb") as file:
    A = pickle.load(file)

# 按行归一化
n = A.sum(axis=-1)  # n 代表每一个页面的链接总数
n[n == 0] = 1.0  # 把不存在链接的页面的链接总数置为 1
n = (1 / n)[np.newaxis]  # 稀疏矩阵不能使用除法，把 n 置为倒数且提升一维（二维向量才可以转置）
P = A * n.T  # 归一化得到 P

# 超参数
damping_factor = 0.85
converge_factor = 1.0e-7
maximum_iteration = 100

pi = np.ones(P.shape[0]) # 初始化 PageRank 向量
teleport = np.ones_like(pi, dtype=np.float64) / pi.shape[0]  # Teleport vector

pagerank_top_record = [] # 用于记录迭代中排名前 10 的 PageRank

with Progress(console=Console()) as progress:
    task = progress.add_task("Calculate PageRank...", total=maximum_iteration)

    for iteration in range(maximum_iteration):
        progress.update(task, description=f"Calulate PageRank, iteration {iteration}...")
        prev_pi = pi

        pi = damping_factor * P.T.dot(prev_pi) + (1 - damping_factor) * teleport #! PageRank 计算

        top = np.argsort(pi)[-10:][::-1] # 选取排名前 10 的页面
        pagerank_top_record.append(
            pd.DataFrame({"key": top, "page_rank": pi[top], "iteration": iteration})
        ) # 记录

        if np.abs(prev_pi - pi).sum() < pi.shape[0] * converge_factor:
            progress.update(task, description=f"[green]PageRank [bold]converged[/] at iteration {iteration}", completed=maximum_iteration)
            progress.refresh()
            break
        progress.advance(task)


with open("title.pickle", "rb") as file:
    titles = pickle.load(file)


keys = {key: title for title, key in titles.items()}


@np.vectorize
def get_title(key: int) -> str:
    """
    从编号获取标题
    """
    return keys.get(key, "?")


@np.vectorize
def get_key(title: str) -> int:
    """
    从标题获取编号
    """
    return titles.get(title, -1)


def get_pagerank_of(pi: npty.NDArray[np.float64], indices: npty.NDArray[np.int64]):
    """
    从 pi 中读取 indices
    """
    return get_title(indices[pi[indices].argsort()])[::-1]



top_pagerank_iters = pd.concat(pagerank_top_record)
top_pagerank_iters["title"] = get_title(top_pagerank_iters["key"])


all_top_keys = top_pagerank_iters["key"].unique()
all_top_keys_order = get_title(all_top_keys[pi[all_top_keys].argsort()][::-1])

print(all_top_keys_order[:10])

# def set_font():
#     font = "Iosevka"

#     return {
#         "config" : {
#             "title": {'font': font},
#             "axis": {
#                 "labelFont": font,
#                 "titleFont": font
#             },
#             "header": {
#                 "labelFont": font,
#                 "titleFont": font
#             },
#             "legend": {
#                 "labelFont": font,
#                 "titleFont": font
#             }
#         }
#     }

# alt.themes.register('mono', set_font)
# alt.themes.enable('mono')


# alt.Chart(top_pagerank_iters).mark_line(point = True).encode(  # type: ignore
#     x = alt.X("iteration:O"), # type: ignore
#     y="rank:O",
#     color=alt.Color("title:O", scale=alt.Scale(scheme="tableau20"), sort=all_top_keys_order) # type: ignore
# ).transform_window(
#     rank="rank()",
#     sort=[alt.SortField("page_rank", order="descending")], # type: ignore
#     groupby=["iteration"]
# ).properties(
#     title="Bump Chart for Top 10 Pages in Each Iteration",
#     width=800,
#     height=150,
# )


final_keys = np.argsort(pi)[::-1]

with Console() as console:
    with console.status("Dumping results..."):
        result = pd.DataFrame({"key": final_keys, "page_rank": pi[final_keys],})
        result["title"] = result["key"].apply(get_title)
        result.loc[:,["title", "page_rank"]].to_csv("result.csv", sep="\t", header=False, index=False, float_format="%.4e")
        console.log("[green]PageRank result dumped as [u]result.csv[/].")

        with open("result.npy", "wb") as file:
            np.save(file, pi)
            console.log("[green]PageRank result dumped as [u]result.npy[/].")


# with open("result.csv", "w") as file:
#     file.writelines(result.loc[:, ["index", "page_rank"]].apply(lambda x: f"{x[0]}\t{x[1]:.4e}\n", axis=1))  # type: ignore


# get_title(np.argsort(pi)[-100:])

# import re
# PATTERN = re.compile("(?i)Earth")

# get_rank_of(pi, np.fromiter(map(get_id, filter(PATTERN.search, titles.keys())), dtype="int64"))[:20]

