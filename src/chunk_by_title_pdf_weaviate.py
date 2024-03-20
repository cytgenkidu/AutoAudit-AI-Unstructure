import glob
import os
import pickle
import re
import tempfile

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

# from tools.vision import vision_completion

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


def extract_text(file_name: str, vision=False):
    # 图像的最小尺寸要求
    min_image_width = 250
    min_image_height = 270
    # 分割文档
    elements = partition(
        filename=file_name,
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
        # if vision:
        #     if isinstance(element, Image):
        #         point1 = element.metadata.coordinates.points[0]
        #         point2 = element.metadata.coordinates.points[2]
        #         width = abs(point2[0] - point1[0])
        #         height = abs(point2[1] - point1[1])
        #         if width >= min_image_width and height >= min_image_height:
        #             element.text = vision_completion(element.metadata.image_path)

    chunks = chunk_by_title(
        elements=elements,
        multipage_sections=True,
        combine_text_under_n_chars=0,
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

    data = fix_utf8(text_list)

    dir_name, file_name = os.path.split(file_name)
    output_dir = os.path.join("docs_output", dir_name)
    os.makedirs(output_dir, exist_ok=True)

    with open(os.path.join(output_dir, f"{file_name}.pkl"), "wb") as f:
        pickle.dump(data, f)

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

try:
    # collection = w_client.collections.create(
    #     name="audit",
    #     vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_transformers(),
    # )

    # directory = "audit"

    # for file_path in glob.glob(os.path.join(directory, "*.pdf")):
    #     file_name = os.path.basename(file_path)
    #     file_name_without_ext = re.split(r"\.pdf$", file_name)[0]

    #     contents = extract_text(file_path)
    #     w_chunks = split_chunks(text_list=contents, source=file_name_without_ext)

    #     questions = w_client.collections.get(name="audit")
    #     questions.data.insert_many(w_chunks)

    directory = "docs_output/audit"
    
    data_list = []

    for file_path in glob.glob(os.path.join(directory, "*.pkl")):
        file_name = os.path.basename(file_path)
        file_name_without_ext = re.split(r"\.pkl$", file_name)[0]
        with open(file_path, "rb") as f:
            data = pickle.load(f)
            data_list.extend(data)

    #     w_chunks = chunks_add_source(text_list=data_list, source=file_name_without_ext)

    #     collection = w_client.collections.get(name="audit")
    #     collection.data.insert_many(w_chunks)

    # w_client.collections.delete(name="water")

finally:
    w_client.close()
