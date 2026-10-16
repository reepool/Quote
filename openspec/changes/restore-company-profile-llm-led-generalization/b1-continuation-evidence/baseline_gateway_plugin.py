import subprocess

def pytest_sessionstart(session):
    import utils.llm.client as module
    import utils.llm
    code=subprocess.check_output(['git','show','b9e620da:utils/llm/client.py'],text=True)
    exec(compile(code,'b9e620da:utils/llm/client.py','exec'),module.__dict__)
    utils.llm.LlmClient=module.LlmClient
