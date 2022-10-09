# %%
from collections.abc import Generator
from os import environ
from pathlib import Path

import dotenv
import lxml.etree as etree
import mwparserfromhell as mw
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import regex
from rich.console import Console
from rich.progress import Progress, TaskID

# %%
# %load_ext rich

# %%
dotenv.load_dotenv()
DATA_DIR = Path(environ.get("DATA_DIR", "."))

# %%
NAMESPACE_MEDIAWIKI = r"{http://www.mediawiki.org/xml/export-0.10/}"
NS = NAMESPACE_MEDIAWIKI


def n(s: str):
    """
    附加 XML Namespace 前缀
    """
    return f"{NAMESPACE_MEDIAWIKI}{s}"


LEN_NAMESPACE_MEDIAWIKI = len(NAMESPACE_MEDIAWIKI)


def d(s: str):
    """
    去除 XML Namespace 前缀
    """
    return s[LEN_NAMESPACE_MEDIAWIKI:]


# %%
def to_title(ipt: str) -> str:
    """
    把 `<title>` 元素和 MediaWiki Wikilink 统一为第一个字母强制大写的 Snake Case，即 Wikipedia 内部存储使用的大小写模式
    """
    ret = "_".join(ipt.strip().split())
    return (ret[0].upper() + ret[1:]) if len(ret) > 1 else ret.upper()


# %%
REGEX_PATTERN_STRIP_ANCHOR = regex.compile(r"#.+$")


def strip_anchor(ipt: str) -> str:
    """
    从 Wikilink 中移除页面 Anchor `#`
    """
    return REGEX_PATTERN_STRIP_ANCHOR.sub("", ipt)


# %%
def get_relations(progress: Progress, task: TaskID, path: Path):
    """
    解析一个 Wikipedia XML 文件，并获得其中所有的页面关系
    """
    with open(path, "rb") as file:
        for idx, (event, element) in enumerate(
            etree.iterparse(file, tag=n("page"), huge_tree=True)
        ):
            # 使用 iterparse 进行 incremental parsing，不会将整个文件读入内存
            # 每次读取一个 `<page>` 元素存储于 `element`
            element: etree._Element

            # 在 `<title>` 元素的 innerText 中提取标题
            title: str = to_title(element.findtext(n("title")))  # type: ignore
            progress.update(
                task, description=f"Parsed page {idx}: {title}...", advance=1
            )

            # 提取第一个 `<revision>` 元素的 `<text>` 元素的 innerText, 并用 MediaWiki 解析器处理，获得所有的 Wikilinks
            wikitext: str = element.find(n("revision")).findtext(n("text"))  # type: ignore
            wikilinks: Generator[mw.nodes.Wikilink, None, None] = mw.parse(
                wikitext
            ).ifilter_wikilinks()

            # 对于每一个 Wikilink [[title|text]]，提取其 title 部分，并削去其 Anchor
            links = [to_title(strip_anchor(str(l.title))) for l in wikilinks]

            # 产生一个 tuple[str, list[str]] 
            yield (title, links)
            progress.refresh()
            element.clear(keep_tail=True)


# %%
def main():
    """
    主入口
    """

    console = Console()
    with Progress(console=console) as progress:
        task_file = progress.add_task("file")

        paths = list(DATA_DIR.glob("*.xml"))
        progress.update(task_file, total=len(paths))

        for path in paths:
            progress.update(
                task_file, description=f"Parsing file {path.name}...", advance=1
            )
            task_xml = progress.add_task("xml", start=False, total=None)

            df = pd.DataFrame.from_records(
                get_relations(progress, task_xml, path), columns=["title", "link"],
            ).explode(("link")) # 把 link 的嵌套列表展开

            pat = pa.Table.from_pandas(df)
            pq.write_to_dataset(pat, "data/wikipedia-links")
            #? 会将 Arrow Parquet 表写入 data/wikipedia-links 文件夹


# %% [markdown]
# - [Help:Links](https://www.mediawiki.org/wiki/Help:Links)(formats of links)
# - [Wikipedia:Naming Conventions](https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(technical_restrictions))(Wikipedia naming conventions)

# %%
if __name__ == "__main__":
    main()

# %%


# wikilinks: Generator[mw.nodes.Wikilink, None, None] = mw.parse("[[File:Hi#Hey]]").ifilter_text()

# inspect(wikilinks)

# [l for l in wikilinks]

