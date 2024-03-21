import glob
import os
import pickle
import re
import tempfile
import io
import weaviate
import weaviate.classes as wvc
from dotenv import load_dotenv
from unstructured.chunking.title import chunk_by_title
from unstructured.cleaners.core import clean, group_broken_paragraphs
from unstructured.documents.elements import (
    CompositeElement,
    Footer,
    Header,
    Table,
    TableChunk,
    Title,
)
from unstructured.partition.auto import partition
from unstructured_inference.models.base import DEFAULT_MODEL
from weaviate.config import AdditionalConfig
from pyzotero import zotero


load_dotenv()


def fix_utf8(original_list):
    cleaned_list = []
    for original_str in original_list:
        cleaned_str = original_str.replace("\ufffd", " ")
        cleaned_list.append(cleaned_str)
    return cleaned_list


def check_misc(text):
    keywords_for_misc = [
        "ACKNOWLEDGEMENTS",
        "ACKNOWLEDGMENTS",
        "ACKNOWLEDGEMENT",
        "ACKNOWLEDGMENT",
        "BIBLIOGRAPHY",
        "DATAAVAILABILITY",
        "DECLARATIONOFCOMPETINGINTEREST",
        # "ONLINE",
        "REFERENCES",
        "SUPPLEMENTARYINFORMATION",
        "SUPPLEMENTARYMATERIALS",
        "SUPPORTINGINFORMATION",
        "参考文献",
        "致谢",
        "謝",
        "謝辞",
    ]

    text = text.strip()
    text = text.replace(" ", "")
    text = text.replace("\n", "")
    text = text.replace("\t", "")
    text = text.replace(":", "")
    text = text.replace("：", "")
    text = text.upper()

    if text in keywords_for_misc or any(
        keyword in text for keyword in keywords_for_misc
    ):
        return True


def extract_text(file: any, file_name, vision=False):
    # 分割文档
    elements = partition(
        file=file,
        header_footer=False,
        pdf_extract_images=vision,
        pdf_image_output_dir_path=tempfile.gettempdir(),
        skip_infer_table_types=["jpg", "png", "xls", "xlsx"],
        strategy="hi_res",
        languages=["chi_sim"],
        model_name=DEFAULT_MODEL,
    )

    skip = False
    filtered_elements = []
    for element in elements:
        if skip:
            continue
        if isinstance(element, Title) and check_misc(element.text):
            skip = True
            continue
        if not (isinstance(element, Header) or isinstance(element, Footer)):
            filtered_elements.append(element)

    # 对文本和图像元素进行处理
    for element in filtered_elements:
        if element.text != "":
            element.text = group_broken_paragraphs(element.text)
            element.text = clean(
                element.text,
                bullets=False,
                extra_whitespace=True,
                dashes=False,
                trailing_punctuation=False,
            )
    chunks = chunk_by_title(
        elements=elements,
        multipage_sections=True,
        combine_text_under_n_chars=100,
        new_after_n_chars=None,
        max_characters=4096,
    )

    text_list = []

    for chunk in chunks:
        if isinstance(chunk, CompositeElement):
            text = chunk.text
            text_list.append(text)
        elif isinstance(chunk, (Table, TableChunk)):
            text_as_html = getattr(chunk.metadata, "text_as_html", None)
            text_to_append = text_as_html if text_as_html is not None else chunk.text

            if text_list:
                text_list[-1] = text_list[-1] + "\n" + text_to_append
            else:
                text_list.append(text_to_append)
    if len(text_list) >= 2 and len(text_list[-1]) < 10:
        text_list[-2] = text_list[-2] + " " + text_list[-1]
        text_list = text_list[:-1]

    result_list = []
    for text in text_list:
        split_text = text.split("\n\n", 1)
        if len(split_text) == 2:
            title, _ = split_text
        result_list.append({title: text})
    return result_list


def split_chunks(text_list: list, source: str):
    chunks = []
    for text in text_list:
        for key, value in text.items():
            chunks.append({"question": key, "answer": value, "source": source})
    return chunks


def chunks_add_source(text_list: list, source: str):
    chunks = []
    for text in text_list:
        chunks.append({"content": text, "source": source})
    return chunks


# w_client = weaviate.connect_to_local(
#     host="localhost", additional_config=AdditionalConfig(timeout=(600, 800))
# )

# try:
# collection = w_client.collections.create(
#     name="audit",
#     vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_transformers(),
# )
zot = zotero.Zotero(library_id=None, library_type=None, api_key=None)
collections = zot.all_collections()
for collection in collections:
    collection_key = collection["data"]["key"]
    if collection_key in ["2GQGZZMJ", "BG678IY7"]:  # 其他废止的
        continue
    items = zot.everything(zot.collection_items(collection_key))
    for item in items:
        if "links" in item and "attachment" in item["links"]:
            href = item["links"]["attachment"].get("href")
            if href:
                file_key = href.split("/")[-1]
                file_key = item["links"]["attachment"].get("href").split("/")[-1]
                file_name = item["data"]["nameOfAct"]
                # file=zot.dump(file_key)
                # with open(file_name, "wb") as file:
                #     # 将二进制数据写入文件
                #     file.write(binary_data)
                file_in_memory = io.BytesIO(zot.file(file_key))
                contents = extract_text(file_in_memory, file_name)
                # 写入文件
                data = fix_utf8(contents)
                output_dir = "docs_output"
                os.makedirs(output_dir, exist_ok=True)
                with open(os.path.join(output_dir, f"{file_name}.pkl"), "wb") as f:
                    pickle.dump(data, f)
                # 传入向量数据库
                # file_name_without_ext = re.split(r"\.pdf$", file_name)[0]
                # w_chunks = split_chunks(text_list=contents, source=file_name_without_ext)

                # questions = w_client.collections.get(name="audit")
                # questions.data.insert_many(w_chunks)

    # directory = "docs_output/audit"

    # data_list = []

    # for file_path in glob.glob(os.path.join(directory, "*.pkl")):
    #     file_name = os.path.basename(file_path)
    #     file_name_without_ext = re.split(r"\.pkl$", file_name)[0]
    #     with open(file_path, "rb") as f:
    #         data = pickle.load(f)
    #         data_list.extend(data)

    #     w_chunks = chunks_add_source(text_list=data_list, source=file_name_without_ext)

    #     collection = w_client.collections.get(name="audit")
    #     collection.data.insert_many(w_chunks)

    # w_client.collections.delete(name="water")

# finally:
#     print('ok')
#     w_client.close()

# # 定义 Zotero 集合和对应的 CSV 文件名
# collections = {
#     'F7G7FCN7': '法律法规',
#     '5VFGICV6': '工程建设法律法规',
#     'IFMKMKHG': '地方规章制度',
#     'NLS7CUUS': '国家规章制度',
#     '2GQGZZMJ': '废止规章制度',
#     'BG678IY7': '企业规章制度',
#     'YJRUXNIV': '国家审计准则',
#     'ZD29M3YR': '内部审计准则',
#     'YHJ33LQ3': '企业会计准则',
#     'IXUS4YA5': '上市公司规范',
#     '7HAWBGXX': '企业审计规范',
#     '34A8XYJK': '上市公司信息披露管理办法',
#     'LGPSNCCQ': '审计指南',
#     'PMUG3TTE': '行业审计规范',
#     'RNCU6K4Q': '注册会计师审计准则'
# }
