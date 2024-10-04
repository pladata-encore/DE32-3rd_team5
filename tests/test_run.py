from de32_3rd_team5.module_llama import receiveMsg, makeModel

def testReceive():
    
    msg = "What is gimbab"
    df = receiveMsg(msg)

    assert isinstance(df, list)
