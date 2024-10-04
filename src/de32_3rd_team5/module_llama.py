import torch
from transformers import pipeline

def makeModel():
    model_id = "meta-llama/Llama-3.2-3B"

    pipe = pipeline(
        "text-generation",
        model = model_id,
        torch_dtype = torch.bfloat16,
        device_map = "auto"
    )
    return pipe

def receiveMsg(req_message: str):

    pipe = makeModel()
    result_df = pipe(req_message)

    ##TODO
    # result_df는 {"generated text", "{req_message} {result1}\{result2}\{result3}} 꼴로 이루어져있음. 이를 분리해야함
    result_msg = ""
    return result_msg
