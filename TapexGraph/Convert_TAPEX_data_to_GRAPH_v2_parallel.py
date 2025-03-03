#!/usr/bin/env python
# coding: utf-8

# In[9]:


from datasets import concatenate_datasets,load_from_disk,Dataset

from add_utils import translate_query_to_graph_form
#from datasets.utils.logging import disable_progress_bar
#disable_progress_bar()
from concurrent.futures import ProcessPoolExecutor
import itertools
from tqdm import tqdm
# Чтение данных из файло
def read_questions(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            yield line.strip()
#questions = list(read_questions('tapex_pretrain/train.src'))
#answers = list(read_questions('tapex_pretrain/train.tgt'))
# Проверка, что количество вопросов и ответов совпадает
#assert len(questions) == len(answers), "Количество вопросов и ответов должно совпадать."
# Создание словаря для Dataset

# Создание Dataset
omega_include = ["P","C","S","GB","H","OB","A","OP","L"]#


def map_function_for_question_change(example):
    try:
        #print(example['question'])
        example['question'],example['answer'] = translate_query_to_graph_form(example['question'],
                                                                              answer = example['answer'],
                                                                              Omega_include=omega_include)
    except Exception as e:
            example['question'] = "None"
            example['answer'] = "None"
    finally:
        return example
start_chank_id = (1500+92)*10+5
chank_id = 0
bach_size = 100
questions_gen = read_questions('tapex_pretrain/train.src')
answ_gen = read_questions('tapex_pretrain/train.tgt')
for i in range(start_chank_id):
    list(itertools.islice(questions_gen, bach_size))
    list(itertools.islice(answ_gen, bach_size))
chank_id = start_chank_id

while True:
    question = list(itertools.islice(questions_gen, bach_size))
    answer = list(itertools.islice(answ_gen, bach_size))
    print(f'----------------{chank_id}---------------------')
    if not question:
        break
    dataset = Dataset.from_dict({
        'question': question,
        'answer': answer
    })
    print("Загрузил данные")
    print("Start processing")
    dataset = dataset.map(map_function_for_question_change,num_proc=10)
    print(dataset[0])

    print("DATA TRANSFORMED. START SAVING")
    dataset.save_to_disk(f'./converved_to_{"".join(omega_include).lower()}_graph_tapex_data_{chank_id}')
    chank_id+=1
# In[10]:
dataset = Dataset.from_dict({
        'question': [],
        'answer': []
    })
for chank in range(chank_id):
    dataset_path = f'./converved_to_{"".join(omega_include).lower()}_graph_tapex_data_{chank}'
    data = load_from_disk(dataset_path)
    dataset = concatenate_datasets([dataset,data])
    del data
print("DATA TRANSFORMED. START SAVING")
dataset.save_to_disk(f'./converved_to_{"".join(omega_include).lower()}_graph_tapex_data_full')



#test = 'select competition where abs ( opponent - competition ) > 1891 col : goal | date | venue | opponent | score | result | competition row 1 : 1 | 12 august 1998 | asim ferhatovic hase stadium, sarajevo | faroe islands | 1–0 | 1–0 | uefa euro 2000 qualifying row 2 : 2 | 14 october 1998 | zalgiris stadium, vilnius | lithuania | 2–2 | 2–4 | uefa euro 2000 qualifying row 3 : 3 | 9 october 1999 | kadriorg stadium, tallinn | estonia | 1–1 | 4–1 | uefa euro 2000 qualifying row 4 : 4 | 9 october 1999 | kadriorg stadium, tallinn | estonia | 2–1 | 4–1 | uefa euro 2000 qualifying row 5 : 5 | 9 october 1999 | kadriorg stadium, tallinn | estonia | 3–1 | 4–1 | uefa euro 2000 qualifying row 6 : 6 | 9 october 1999 | kadriorg stadium, tallinn | estonia | 4–1 | 4–1 | uefa euro 2000 qualifying row 7 : 7 | 2 september 2000 | asim ferhatovic hase stadium, sarajevo | spain | 1-1 | 1–2 | 2002 fifa world cup qualification row 8 : 8 | 15 august 2001 | asim ferhatovic hase stadium, sarajevo | malta | 1–0 | 2–0 | friendly match row 9 : 9 | 15 august 2001 | asim ferhatovic hase stadium, sarajevo | malta | 2–0 | 2–0 | friendly match row 10 : 10 | 7 october 2001 | asim ferhatovic hase stadium, sarajevo | liechtenstein | 2–0 | 5–0 | 2002 fifa world cup qualification row 11 : 11 | 7 october 2001 | asim ferhatovic hase stadium, sarajevo | liechtenstein | 4–0 | 5–0 | 2002 fifa world cup qualification row 12 : 12 | 11 october 2002 | asim ferhatovic hase stadium, sarajevo | germany | 1–0 | 1–1 | friendly match row 13 : 13 | 13 february 2003 | millennium stadium, cardiff | wales | 1–0 | 2–2 | friendly match row 14 : 14 | 2 april 2003 | parken stadium, copenhagen | denmark | 2–0 | 2–0 | uefa euro 2004 qualifying'
