import traceback

from mhooge_flask.logging import logger

def test_stderr():
    try:
        raise Exception("Error happened")
    except Exception:
        traceback.print_exc()
        

if __name__ == "__main__":
    test_stderr()
