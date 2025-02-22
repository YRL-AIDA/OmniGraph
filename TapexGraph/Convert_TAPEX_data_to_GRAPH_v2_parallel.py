#!/usr/bin/env python
# coding: utf-8

# In[9]:


from datasets import load_dataset,load_from_disk,Dataset
from add_utils import translate_query_to_graph_form
#from datasets.utils.logging import disable_progress_bar
#disable_progress_bar()
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
# Чтение данных из файло
with open('tapex_pretrain/train.src', 'r', encoding='utf-8') as src_file:
    questions = src_file.read().splitlines()
with open('tapex_pretrain/train.tgt', 'r', encoding='utf-8') as tgt_file:
    answers = tgt_file.read().splitlines()
# Проверка, что количество вопросов и ответов совпадает
assert len(questions) == len(answers), "Количество вопросов и ответов должно совпадать."
# Создание словаря для Dataset
data = {
    'question': questions,
    'answer': answers
}
# Создание Dataset
dataset = Dataset.from_dict(data)


# In[10]:

#i = 0
#j = 0
omega_include = ["P","C","S","GB","H","OB","A","OP","L"]#


# In[27]:


def map_function_for_question_change(example):
    try:
        #print(example['question'])
        processed_question,answer = translate_query_to_graph_form(example['question'],
                                                                  answer = example['answer'],
                                                                  Omega_include=omega_include)

    except Exception as e:

        processed_question = "None"
        answer = "None"
    finally:
        return {'question': processed_question, 'answer': answer}

def process_dataset_in_parallel(dataset, max_workers=4):

    with ProcessPoolExecutor(max_workers=max_workers) as executor:

        # Применяем функцию обработки к каждому элементу в датасете

        processed_data = list(tqdm(executor.map(map_function_for_question_change, dataset)))

    

    # Возвращаем обработанный датасет

    return Dataset.from_list(processed_data)
# In[ ]:



#dataset = dataset.map(map_function_for_question_change)
processed_dataset = process_dataset_in_parallel(dataset,max_workers=10)
print("DATA TRANSFORMED. START SAVING")
dataset.save_to_disk(f'./converved_to_{"".join(omega_include).lower()}_graph_tapex_data')

