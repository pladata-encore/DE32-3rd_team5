import torch
from transformers import pipeline

model_id = "meta-llama/Llama-3.2-3B"

pipe = pipeline(
    "text-generation", 
    model=model_id, 
    torch_dtype=torch.bfloat16, 
    device_map="auto"
)

result = pipe("how can i fix car engine failure", max_new_tokens = 200)
