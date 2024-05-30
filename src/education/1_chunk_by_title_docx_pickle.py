import glob
import os
import pickle
import concurrent.futures
from unstructured.partition.docx import partition_docx
from unstructured.chunking.title import chunk_by_title
from unstructured.documents.elements import CompositeElement, Table

def extract_text(file_name: str):
    elements = partition_docx(
        filename=file_name,
        multipage_sections=True,
        infer_table_structure=True,
        include_page_breaks=False,
    )

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
        elif isinstance(chunk, Table):
            if text_list:
                text_list[-1] = text_list[-1] + "\n" + chunk.metadata.text_as_html
            else:
                text_list.append(chunk.hunk.metadata.text_as_html)
    return text_list

def process_docx(file_path):
    record_id = os.path.splitext(os.path.basename(file_path))[0]

    text_list = extract_text(file_path)

    with open("education_pickle/" + record_id + ".pkl", "wb") as f:
        pickle.dump(text_list, f)

    text_str = "\n----------\n".join(map(str, text_list))

    with open("education_txt/" + record_id + ".txt", "w") as f:
        f.write(text_str)

directory = "docs/education"
docx_files = glob.glob(os.path.join(directory, "*.docx"))

with concurrent.futures.ProcessPoolExecutor(max_workers=6) as executor:
    executor.map(process_docx, docx_files)

print("Data inserted successfully")