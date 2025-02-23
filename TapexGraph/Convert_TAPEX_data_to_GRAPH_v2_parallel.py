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
start_chank_id = 8
chank_id = 0
bach_size = 200000
questions_gen = read_questions('tapex_pretrain/train.src')
answ_gen = read_questions('tapex_pretrain/train.tgt')
for i in range(start_chank_id):
    list(itertools.islice(questions_gen, bach_size))
    list(itertools.islice(answ_gen, bach_size))
chank_id = start_chank_id
bach_size = 100000
list(itertools.islice(questions_gen, bach_size))
list(itertools.islice(answ_gen, bach_size))
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




