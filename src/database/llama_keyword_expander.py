# TODO: When running our system, we can create a cache of keyword expansion from common queries  as we go, so popular inputs 
# already have alternatives
import torch
import os
from transformers import AutoModelForCausalLM, AutoTokenizer
import asyncio 

"""
This file includes a class for prompting an Llama model to expand user ingredients into a larger list of appropriate alternative ingredients names."
"""

class KeywordExpander():
    def __init__(self):
        self.model, self.tokenizer = self.get_llama_3b_instance()

    def get_llama_3b_instance(self):
        llama_version = 'meta-llama/Llama-3.2-3B-Instruct'
        hf_token = os.environ.get("HF_ACCESS_TOKEN", default=None)
        if hf_token is None:
            raise ValueError("HF_ACCESS_TOKEN environment variable not set.  Please create a HuggingFace access token and"
                            "set HF_ACCESS_TOKEN to it")
    
        model = AutoModelForCausalLM.from_pretrained(llama_version, device_map='auto', token=hf_token, torch_dtype=torch.bfloat16)
        tokenizer = AutoTokenizer.from_pretrained(llama_version, token=hf_token)
        return model, tokenizer


    async def expand_ingredients(self, ingredients: list[str]):
        # system_prompt = "You are a master at keyword expansion for a scalable, ingredient quantity-based recipe search system."
        system_prompt = "You are a helpful chatbot."

        # prompt = "Please take the following ingredients Input Array and expand it into an Output Array of synonyms or similar ingredient names that would help the user find appropriate recipes given the available ingredients. These alternative ingredient keywords should meet the same flavor palette as the original ingredient given, so that it makes sense to retrieve recipes that include that alternative ingredient name, but the original ingredient name provided by the user to the search engine would be a suitable replacement.\nReply only in JSON, with at most 3 synonyms per ingredient in the input array.\nExample input:\n###['bread', 'chili paste']###\nExample output:\n###{{'bread': ['loaf', 'bun', 'roll'], 'chili paste': ['chili sauce', 'sambal', 'gochujang']}}###\nInput Array:\n###{}###\nOutput Array:".format(ingredients)
        prompt = "Please take the following Input Array of ingredients and expand it into an Output Array of common alternatives chosen based on their flavor profiles and culinary uses, allowing for suitable replacements in recipes retrieved from the search engine.\nReply only in JSON, with at most 3 synonyms per ingredient in the input array.\nExample input:\n###['bread', 'chili paste']###\nExample output:\n###{{'bread': ['loaf', 'bun', 'roll'], 'chili paste': ['chili sauce', 'sambal', 'gochujang']}}###\nInput Array:\n###{}###\nOutput Array:".format(ingredients)

        chat = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]
    
        context = self.tokenizer.apply_chat_template(chat, tokenize=False, add_generation_prompt=True)

        # tokenize the context
        input_tokens = self.tokenizer(context, return_tensors='pt').to(self.model.device)
        n_input_tokens = input_tokens['input_ids'].size(-1)

        # generate output tokens
        output_tokens = self.model.generate(**input_tokens, max_new_tokens=512, pad_token_id=self.tokenizer.eos_token_id)

        model_answer = self.tokenizer.decode(output_tokens[0][n_input_tokens:], skip_special_tokens=True)
        return model_answer

async def main():
    expander = KeywordExpander()

    # expanded = await expander.expand_ingredients(["bell pepper"])
    expanded = await expander.expand_ingredients(["bell pepper", "yukon gold potato"])

    print(expanded)

if __name__ == "__main__":
    asyncio.run(main())