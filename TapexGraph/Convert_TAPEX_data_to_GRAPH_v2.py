#!/usr/bin/env python
# coding: utf-8

# In[9]:


from datasets import load_dataset,load_from_disk,Dataset
from add_utils import translate_query_to_graph_form

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


omega_include = ["P","C","S","GB","H","OB","A","OP","L"]#


# In[27]:


def map_function_for_question_change(example):
    try:
        #print(example['question'])
        example['question'],example['answer'] = translate_query_to_graph_form(example['question'],
                                                                              answer = example['answer'],
                                                                              Omega_include=omega_include)
    except Exception as e:
        with open('log.txt','a') as logf:
            logf.write(example['question']+'\n')
            logf.write(str(e))
            example['question'] = "None"
            example['answer'] = "None"
    finally:
        return example


# In[ ]:



dataset = dataset.map(map_function_for_question_change)

print("DATA TRANSFORMED. START SAVING")
dataset.save_to_disk(f'./converved_to_{"".join(omega_include).lower()}_graph_tapex_data')

