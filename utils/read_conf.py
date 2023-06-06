from os import getenv, path
from pathlib import Path
from dotenv import load_dotenv
import sys
sys.path.insert(0, '..')
if path.isfile("./conf/.env_example"):
    dir_ = Path(__file__).resolve().parent
    param_path = dir_.parent.joinpath("./conf/.env_example")
    a = load_dotenv(param_path)

PostgressHOST = getenv("PostgressHOST", default=None)
PostgressPORT = getenv("PostgressPORT", default=None)
PostgressDBNAME = getenv("PostgressDBNAME", default=None)
PostgressUSERNAME = getenv("PostgressUSERNAME", default=None)
PostgressPASSWORD = getenv("PostgressPASSWORD", default=None)
PostgressSCHEMA = getenv("PostgressSCHEMA", default=None)
PostgressTABLE = getenv("PostgressTABLE", default=None) 
RAM_LIMIT = getenv("RAM_LIMIT", default=None)
JARDRV = getenv("JARDRV", default=None)
PROCESS_TYPE = getenv("PROCESS_TYPE", default=None) 