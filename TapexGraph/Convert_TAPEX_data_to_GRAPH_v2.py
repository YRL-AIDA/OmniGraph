#!/usr/bin/env python
# coding: utf-8

# In[9]:

import tracemalloc
from datasets import load_dataset,load_from_disk,Dataset
from add_utils import translate_query_to_graph_form
from datasets.utils.logging import disable_progress_bar
disable_progress_bar()
tracemalloc.start()
# Чтение данных из файло
print("ЧТЕНИЕ ДАННЫХ")
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

current, peak = tracemalloc.get_traced_memory()
print(f'Текущая память: {current / 10**6} Мб; Пик: {peak / 10**6} Мб')
# In[10]:

i = 0
j = 0
omega_include = ["P","C","S","GB","H","OB","A","OP","L"]#


# In[27]:


def map_function_for_question_change(example):
    try:
        current, peak = tracemalloc.get_traced_memory()

        print(f'Текущая память: {current / 10**6} Мб; Пик: {peak / 10**6} Мб')
        #print(example['question'])
        example['question'],example['answer'] = translate_query_to_graph_form(example['question'],
                                                                              answer = example['answer'],
                                                                              Omega_include=omega_include)
        snapshot = tracemalloc.take_snapshot()

        top_stats = snapshot.statistics('lineno')
        print("[Топ 3 строк по выделению памяти]")
        for stat in top_stats[:3]:
            print(stat)
        i+=1
        if i-5000 == 0 :
            with open('log3.txt','a') as logf2:
                j+=i
                logf2.write(str(j)+'\n')
                i = 0
    except Exception as e:
            example['question'] = "None"
            example['answer'] = "None"
    finally:
        return example


# In[ ]:

print("ОБРАБОТКА ДАННЫХ")

dataset = dataset.map(map_function_for_question_change)

print("DATA TRANSFORMED. START SAVING")
dataset.save_to_disk(f'./converved_to_{"".join(omega_include).lower()}_graph_tapex_data')
tracemalloc.stop()